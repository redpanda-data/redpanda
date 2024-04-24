/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cluster/security_manager.h"

#include "cluster/cluster_utils.h"
#include "cluster/commands.h"
#include "cluster/controller_snapshot.h"
#include "model/metadata.h"
#include "raft/fundamental.h"
#include "security/authorizer.h"
#include "security/credential_store.h"
#include "security/role_store.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <iterator>
#include <system_error>
#include <vector>

namespace cluster {

security_manager::security_manager(
  ss::sharded<security::credential_store>& credentials,
  ss::sharded<security::authorizer>& authorizer,
  ss::sharded<security::role_store>& roles)
  : _credentials(credentials)
  , _authorizer(authorizer)
  , _roles(roles) {}

ss::future<std::error_code>
security_manager::apply_update(model::record_batch batch) {
    return deserialize(std::move(batch), commands).then([this](auto cmd) {
        return ss::visit(
          std::move(cmd),
          [this](create_user_cmd cmd) {
              return dispatch_updates_to_cores(std::move(cmd), _credentials);
          },
          [this](delete_user_cmd cmd) {
              return dispatch_updates_to_cores(std::move(cmd), _credentials);
          },
          [this](update_user_cmd cmd) {
              return dispatch_updates_to_cores(std::move(cmd), _credentials);
          },
          [this](create_acls_cmd cmd) {
              return dispatch_updates_to_cores(std::move(cmd), _authorizer);
          },
          [this](delete_acls_cmd cmd) {
              return dispatch_updates_to_cores(std::move(cmd), _authorizer);
          },
          [this](create_role_cmd cmd) {
              return dispatch_updates_to_cores(std::move(cmd), _roles);
          },
          [this](delete_role_cmd cmd) {
              return dispatch_updates_to_cores(std::move(cmd), _roles);
          },
          [this](update_role_cmd cmd) {
              return dispatch_updates_to_cores(std::move(cmd), _roles);
          });
    });
}

namespace {

/*
 * handle: delete acls command
 */
std::error_code
do_apply(delete_acls_cmd cmd, security::authorizer& authorizer) {
    authorizer.remove_bindings(cmd.key.filters);
    return errc::success;
}

/*
 * handle: create acls command
 */
std::error_code
do_apply(create_acls_cmd cmd, security::authorizer& authorizer) {
    authorizer.add_bindings(cmd.key.bindings);
    return errc::success;
}

/*
 * handle: update user command
 */
std::error_code
do_apply(update_user_cmd cmd, security::credential_store& store) {
    auto removed = store.remove(cmd.key);
    if (!removed) {
        return errc::user_does_not_exist;
    }
    store.put(cmd.key, std::move(cmd.value));
    return errc::success;
}

/*
 * handle: delete user command
 */
std::error_code
do_apply(delete_user_cmd cmd, security::credential_store& store) {
    auto removed = store.remove(cmd.key);
    return removed ? errc::success : errc::user_does_not_exist;
}

/*
 * handle: create user command
 */
std::error_code
do_apply(create_user_cmd cmd, security::credential_store& store) {
    if (store.contains(cmd.key)) {
        return errc::user_exists;
    }
    store.put(cmd.key, std::move(cmd.value));
    return errc::success;
}

/*
 * handle: update role command
 */
std::error_code do_apply(update_role_cmd cmd, security::role_store& store) {
    auto data = std::move(cmd.value);
    auto removed = store.remove(data.name);
    if (!removed) {
        return errc::role_does_not_exist;
    }
    store.put(std::move(data.name), std::move(data.role));
    return errc::success;
}

/*
 * handle: delete role command
 */
std::error_code do_apply(delete_role_cmd cmd, security::role_store& store) {
    auto data = std::move(cmd.value);
    auto removed = store.remove(data.name);
    return removed ? errc::success : errc::role_does_not_exist;
}

/*
 * handle: create role command
 */
std::error_code do_apply(create_role_cmd cmd, security::role_store& store) {
    auto data = std::move(cmd.value);
    if (store.contains(data.name)) {
        return errc::role_exists;
    }
    store.put(std::move(data.name), std::move(data.role));
    return errc::success;
}

template<typename Cmd, typename Service>
ss::future<std::error_code>
do_apply(ss::shard_id shard, Cmd cmd, ss::sharded<Service>& service) {
    return service.invoke_on(
      shard, [cmd = std::move(cmd)](auto& local_service) mutable {
          return do_apply(std::move(cmd), local_service);
      });
}

} // namespace

template<typename Cmd, typename Service>
ss::future<std::error_code> security_manager::dispatch_updates_to_cores(
  Cmd cmd, ss::sharded<Service>& service) {
    using ret_t = std::vector<std::error_code>;
    return ss::do_with(
      ret_t{}, [cmd = std::move(cmd), &service](ret_t& ret) mutable {
          ret.reserve(ss::smp::count);
          return ss::parallel_for_each(
                   boost::irange(0, (int)ss::smp::count),
                   [&ret, cmd = std::move(cmd), &service](int shard) mutable {
                       return do_apply(shard, cmd, service)
                         .then([&ret](std::error_code r) { ret.push_back(r); });
                   })
            .then([&ret] { return std::move(ret); })
            .then([](std::vector<std::error_code> results) mutable {
                auto ret = results.front();
                for (auto& r : results) {
                    vassert(
                      ret == r,
                      "State inconsistency across shards detected, "
                      "expected "
                      "result: {}, have: {}",
                      ret,
                      r);
                }
                return ret;
            });
      });
}

ss::future<>
security_manager::fill_snapshot(controller_snapshot& controller_snap) const {
    auto& snapshot = controller_snap.security;

    // Ephemeral credentials must not be stored in the snapshot.
    auto creds = _credentials.local().range(
      security::credential_store::is_not_ephemeral);
    for (const auto& cred : creds) {
        ss::visit(cred.second, [&](security::scram_credential scram) {
            snapshot.user_credentials.push_back(user_and_credential{
              security::credential_user{cred.first}, std::move(scram)});
        });
        co_await ss::coroutine::maybe_yield();
    }

    snapshot.acls = co_await _authorizer.local().all_bindings();

    auto roles = _roles.local().range([](const auto&) { return true; });
    snapshot.roles.reserve(roles.size());
    for (const auto& role : roles) {
        security::role_name name{role};
        snapshot.roles.emplace_back(name, *_roles.local().get(name));
    }

    co_return;
}

ss::future<> security_manager::apply_snapshot(
  model::offset, const controller_snapshot& controller_snap) {
    const auto& snapshot = controller_snap.security;

    co_await _credentials.invoke_on_all(
      [&snapshot](security::credential_store& credentials) {
          credentials.clear();
          return ss::do_for_each(
            snapshot.user_credentials, [&credentials](const auto& user) {
                credentials.put(user.username, user.credential);
            });
      });

    co_await _authorizer.invoke_on_all(
      [&snapshot](security::authorizer& authorizer) {
          return authorizer.reset_bindings(snapshot.acls);
      });

    co_await _roles.invoke_on_all(
      [&snapshot](security::role_store& role_store) {
          return ss::do_for_each(snapshot.roles, [&role_store](const auto& r) {
              role_store.put(r.name, security::role{r.role});
          });
      });
}

} // namespace cluster
