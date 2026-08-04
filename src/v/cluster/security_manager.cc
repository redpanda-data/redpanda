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

#include "cluster/commands.h"
#include "cluster/controller_snapshot.h"
#include "security/acl_store.h"
#include "security/authorizer.h"
#include "security/credential_store.h"
#include "security/role_store.h"

#include <seastar/core/loop.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/maybe_yield.hh>

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
    store.put(std::move(data.name), data.role);
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
    store.put(std::move(data.name), data.role);
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
          ret.reserve(ss::this_smp_shard_count());
          return ss::parallel_for_each(
                   boost::irange(0, (int)ss::this_smp_shard_count()),
                   [&ret, &cmd, &service](int shard) {
                       return do_apply(shard, copy_cmd(cmd), service)
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
            snapshot.user_credentials.push_back(
              user_and_credential{
                security::credential_user{cred.first}, std::move(scram)});
        });
        co_await ss::coroutine::maybe_yield();
    }

    snapshot.acls = co_await _authorizer.local().all_bindings();

    auto roles = co_await _roles.local().all_roles_with_members();
    snapshot.roles.reserve(roles.size());
    for (auto& rwm : roles) {
        snapshot.roles.emplace_back(std::move(rwm.name), std::move(rwm.role));
    }

    co_return;
}

ss::future<> apply_security_snapshot_to_shard(
  security::credential_store& credentials,
  security::authorizer& authorizer,
  security::role_store& roles,
  const controller_snapshot_parts::security_t& snapshot) {
    security::credential_store staged_credentials;
    co_await ss::do_for_each(
      snapshot.user_credentials, [&staged_credentials](const auto& user) {
          staged_credentials.put(user.username, user.credential);
      });

    auto staged_acls = co_await authorizer.store().stage_bindings(
      snapshot.acls);

    security::role_store staged_roles;
    co_await ss::do_for_each(snapshot.roles, [&staged_roles](const auto& r) {
        staged_roles.put(r.name, security::role{r.role});
    });

    credentials = std::move(staged_credentials);
    authorizer.store().commit_bindings(std::move(staged_acls));
    roles = std::move(staged_roles);
}

ss::future<> security_manager::apply_snapshot(
  model::offset, const controller_snapshot& controller_snap) {
    const auto& snapshot = controller_snap.security;

    return ss::smp::invoke_on_all([this, &snapshot] {
        return apply_security_snapshot_to_shard(
          _credentials.local(), _authorizer.local(), _roles.local(), snapshot);
    });
}

} // namespace cluster
