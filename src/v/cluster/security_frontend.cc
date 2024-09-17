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

#include "cluster/security_frontend.h"

#include "cluster/cluster_utils.h"
#include "cluster/commands.h"
#include "cluster/controller.h"
#include "cluster/controller_service.h"
#include "cluster/controller_stm.h"
#include "cluster/errc.h"
#include "cluster/fwd.h"
#include "cluster/logger.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/types.h"
#include "model/errc.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/validation.h"
#include "raft/errc.h"
#include "raft/fundamental.h"
#include "random/generators.h"
#include "rpc/errc.h"
#include "rpc/types.h"
#include "security/authorizer.h"
#include "security/role.h"
#include "security/scram_algorithm.h"
#include "security/scram_authenticator.h"

#include <seastar/core/coroutine.hh>

#include <boost/algorithm/string/split.hpp>

#include <regex>

namespace cluster {

// TODO: possibly generalize with other error mapping routines in cluster::*
// on the other hand, these mappings may be partially specialized for each case
// (e.g. topic-creation, or error-creation).
static inline cluster::errc map_errc(std::error_code ec) {
    if (ec == errc::success) {
        return errc::success;
    }

    if (ec.category() == raft::error_category()) {
        switch (static_cast<raft::errc>(ec.value())) {
        case raft::errc::timeout:
            return errc::timeout;
        case raft::errc::not_leader:
            return errc::not_leader_controller;
        default:
            return errc::replication_error;
        }
    }

    if (ec.category() == rpc::error_category()) {
        switch (static_cast<rpc::errc>(ec.value())) {
        case rpc::errc::client_request_timeout:
            return errc::timeout;
        default:
            return errc::replication_error;
        }
    }

    if (ec.category() == cluster::error_category()) {
        return static_cast<errc>(ec.value());
    }

    return errc::replication_error;
}

security_frontend::security_frontend(
  model::node_id self,
  controller* controller,
  ss::sharded<controller_stm>& s,
  ss::sharded<rpc::connection_cache>& connections,
  ss::sharded<partition_leaders_table>& leaders,
  ss::sharded<features::feature_table>& features,
  ss::sharded<ss::abort_source>& as,
  ss::sharded<security::authorizer>& authorizer)
  : _self(self)
  , _controller(controller)
  , _stm(s)
  , _connections(connections)
  , _leaders(leaders)
  , _features(features)
  , _as(as)
  , _authorizer(authorizer) {}

ss::future<std::error_code> security_frontend::create_user(
  security::credential_user username,
  security::scram_credential credential,
  model::timeout_clock::time_point tout) {
    create_user_cmd cmd(std::move(username), std::move(credential));
    return replicate_and_wait(_stm, _as, std::move(cmd), tout);
}

ss::future<std::error_code> security_frontend::delete_user(
  security::credential_user username, model::timeout_clock::time_point tout) {
    delete_user_cmd cmd(std::move(username), 0 /* unused */);
    return replicate_and_wait(_stm, _as, std::move(cmd), tout);
}

ss::future<std::error_code> security_frontend::update_user(
  security::credential_user username,
  security::scram_credential credential,
  model::timeout_clock::time_point tout) {
    update_user_cmd cmd(std::move(username), std::move(credential));
    return replicate_and_wait(_stm, _as, std::move(cmd), tout);
}

ss::future<std::error_code> security_frontend::create_role(
  security::role_name name,
  security::role role,
  model::timeout_clock::time_point tout) {
    auto feature_enabled = _features.local().is_preparing(
                             features::feature::role_based_access_control)
                           || _features.local().is_active(
                             features::feature::role_based_access_control);
    if (!feature_enabled) {
        vlog(clusterlog.warn, "RBAC feature is not yet active");
        co_return cluster::errc::feature_disabled;
    }
    upsert_role_cmd_data data{.name = std::move(name), .role = std::move(role)};
    create_role_cmd cmd(0 /*unused*/, std::move(data));
    co_return co_await replicate_and_wait(_stm, _as, std::move(cmd), tout);
}

ss::future<std::error_code> security_frontend::delete_role(
  security::role_name name, model::timeout_clock::time_point tout) {
    if (!_features.local().is_active(
          features::feature::role_based_access_control)) {
        vlog(clusterlog.warn, "RBAC feature is not yet active");
        co_return cluster::errc::feature_disabled;
    }
    delete_role_cmd_data data{.name = std::move(name)};
    delete_role_cmd cmd(0 /* unused */, std::move(data));
    co_return co_await replicate_and_wait(_stm, _as, std::move(cmd), tout);
}

ss::future<std::error_code> security_frontend::update_role(
  security::role_name name,
  security::role role,
  model::timeout_clock::time_point tout) {
    if (!_features.local().is_active(
          features::feature::role_based_access_control)) {
        vlog(clusterlog.warn, "RBAC feature is not yet active");
        co_return cluster::errc::feature_disabled;
    }
    upsert_role_cmd_data data{.name = std::move(name), .role = std::move(role)};
    update_role_cmd cmd(0 /*unused*/, std::move(data));
    co_return co_await replicate_and_wait(_stm, _as, std::move(cmd), tout);
}

ss::future<std::vector<errc>> security_frontend::create_acls(
  std::vector<security::acl_binding> bindings,
  model::timeout_clock::duration timeout) {
    if (unlikely(bindings.empty())) {
        co_return std::vector<errc>{};
    }

    auto leader = _leaders.local().get_leader(model::controller_ntp);

    if (!leader) {
        std::vector<errc> res;
        res.assign(bindings.size(), errc::no_leader_controller);
        co_return res;
    }

    if (leader == _self) {
        co_return co_await do_create_acls(std::move(bindings), timeout);
    }

    co_return co_await dispatch_create_acls_to_leader(
      leader.value(), std::move(bindings), timeout);
}

ss::future<std::vector<errc>> security_frontend::dispatch_create_acls_to_leader(
  model::node_id leader,
  std::vector<security::acl_binding> bindings,
  model::timeout_clock::duration timeout) {
    auto num_bindings = bindings.size();
    return _connections.local()
      .with_node_client<cluster::controller_client_protocol>(
        _self,
        ss::this_shard_id(),
        leader,
        timeout,
        [bindings = std::move(bindings),
         timeout](controller_client_protocol cp) mutable {
            create_acls_cmd_data data{.bindings = std::move(bindings)};
            return cp.create_acls(
              create_acls_request{std::move(data), timeout},
              rpc::client_opts(model::timeout_clock::now() + timeout));
        })
      .then(&rpc::get_ctx_data<create_acls_reply>)
      .then([num_bindings](result<create_acls_reply> r) {
          if (r.has_error()) {
              std::vector<errc> res;
              res.assign(num_bindings, map_errc(r.error()));
              return res;
          }
          return std::move(r.value().results);
      });
}

ss::future<std::vector<errc>> security_frontend::do_create_acls(
  std::vector<security::acl_binding> bindings,
  model::timeout_clock::duration timeout) {
    const auto num_bindings = bindings.size();
    create_acls_cmd_data data;
    data.bindings = std::move(bindings);
    create_acls_cmd cmd(std::move(data), 0 /* unused */);

    errc err;
    try {
        auto ec = co_await replicate_and_wait(
          _stm, _as, std::move(cmd), model::timeout_clock::now() + timeout);
        err = map_errc(ec);
    } catch (const std::exception& e) {
        vlog(clusterlog.warn, "Unable to create ACLs: {}", e);
        err = errc::replication_error;
    }

    std::vector<errc> result;
    result.assign(num_bindings, err);
    co_return result;
}

ss::future<std::vector<delete_acls_result>> security_frontend::do_delete_acls(
  std::vector<security::acl_binding_filter> filters,
  model::timeout_clock::duration timeout) {
    /*
     * The removal is performed as a dry run to create the set of binding that
     * are removed. This set will be returned to the caller if the removal is
     * successful. This is done because there is a current limitation on the
     * interface between the controller frontend and backend which is based on
     * a single error code for the result of each command. This is _by design_,
     * and addressing the limitation is future work.
     *
     * For the purpose of this call the limitation results in some overhead (the
     * dry run) and the removed bindings being calculated in a non-atomic
     * context with the command execution. This later point should be ok: there
     * does not seem to be any particular guarantee on the kafka side that this
     * be atomic.
     */
    auto removed_bindings = _authorizer.local().remove_bindings(filters, true);

    delete_acls_cmd_data data;
    data.filters = std::move(filters);
    delete_acls_cmd cmd(std::move(data), 0 /* unused */);

    errc err;
    try {
        auto ec = co_await replicate_and_wait(
          _stm, _as, std::move(cmd), model::timeout_clock::now() + timeout);
        err = map_errc(ec);
    } catch (const std::exception& e) {
        vlog(clusterlog.warn, "Unable to delete ACLs: {}", e);
        err = errc::replication_error;
    }

    std::vector<delete_acls_result> res;

    if (err == errc::success) {
        res.reserve(removed_bindings.size());
        for (auto& bindings : removed_bindings) {
            res.push_back(delete_acls_result{
              .error = errc::success,
              .bindings = std::move(bindings),
            });
        }
        co_return res;
    }

    res.assign(
      removed_bindings.size(),
      delete_acls_result{
        .error = err,
      });

    co_return res;
}

ss::future<std::vector<delete_acls_result>> security_frontend::delete_acls(
  std::vector<security::acl_binding_filter> filters,
  model::timeout_clock::duration timeout) {
    if (unlikely(filters.empty())) {
        co_return std::vector<delete_acls_result>{};
    }

    auto leader = _leaders.local().get_leader(model::controller_ntp);

    if (!leader) {
        std::vector<delete_acls_result> res;
        res.assign(
          filters.size(),
          delete_acls_result{
            .error = errc::no_leader_controller,
          });
        co_return res;
    }

    if (leader == _self) {
        co_return co_await do_delete_acls(std::move(filters), timeout);
    }

    co_return co_await dispatch_delete_acls_to_leader(
      leader.value(), std::move(filters), timeout);
}

ss::future<std::vector<delete_acls_result>>
security_frontend::dispatch_delete_acls_to_leader(
  model::node_id leader,
  std::vector<security::acl_binding_filter> filters,
  model::timeout_clock::duration timeout) {
    const auto num_filters = filters.size();
    return _connections.local()
      .with_node_client<cluster::controller_client_protocol>(
        _self,
        ss::this_shard_id(),
        leader,
        timeout,
        [filters = std::move(filters),
         timeout](controller_client_protocol cp) mutable {
            delete_acls_cmd_data data{.filters = std::move(filters)};
            return cp.delete_acls(
              delete_acls_request{std::move(data), timeout},
              rpc::client_opts(model::timeout_clock::now() + timeout));
        })
      .then(&rpc::get_ctx_data<delete_acls_reply>)
      .then([num_filters](result<delete_acls_reply> r) {
          if (r.has_error()) {
              std::vector<delete_acls_result> res;
              res.assign(
                num_filters,
                delete_acls_result{
                  .error = map_errc(r.error()),
                });
          }
          return std::move(r.value().results);
      });
}

static const ss::sstring bootstrap_user_env_key{"RP_BOOTSTRAP_USER"};

/*static*/ std::optional<user_and_credential>
security_frontend::get_bootstrap_user_creds_from_env() {
    auto creds_str_ptr = std::getenv(bootstrap_user_env_key.c_str());
    if (creds_str_ptr == nullptr) {
        // Environment variable is not set
        return {};
    }

    std::string_view creds = creds_str_ptr;
    std::vector<std::string_view> parts;
    parts.reserve(3);
    boost::algorithm::split(parts, creds, [](char c) { return c == ':'; });
    if (
      !(parts.size() == 2 || parts.size() == 3) || parts[0].empty()
      || parts[1].empty()) {
        // Malformed value.  Do not log the value, it may be malformed
        // but it is still a secret.
        vlog(
          clusterlog.warn,
          "Invalid value of {} (expected \"username:password[:mechanism]\")",
          bootstrap_user_env_key);
        return {};
    }
    std::variant<security::scram_sha256, security::scram_sha512> scram;
    if (parts.size() == 3) {
        if (parts[2] == security::scram_sha512_authenticator::name) {
            scram = security::scram_sha512{};
        } else if (parts[2] != security::scram_sha256_authenticator::name) {
            throw std::invalid_argument(
              fmt::format("Invalid SCRAM mechanism: {}", parts[2]));
        }
    }
    return std::optional<user_and_credential>(
      std::in_place,
      security::credential_user{parts[0]},
      ss::visit(scram, [&](const auto& scram) {
          using scram_t = std::decay_t<decltype(scram)>;
          return scram_t::make_credentials(
            ss::sstring{parts[1]}, scram_t::min_iterations);
      }));
}

ss::future<result<model::offset>> security_frontend::get_leader_committed(
  model::timeout_clock::duration timeout) {
    auto leader = _leaders.local().get_leader(model::controller_ntp);
    if (!leader) {
        return ss::make_ready_future<result<model::offset>>(
          make_error_code(errc::no_leader_controller));
    }

    if (leader == _self) {
        return ss::smp::submit_to(controller_stm_shard, [this, timeout]() {
            return ss::with_timeout(
                     model::timeout_clock::now() + timeout,
                     _controller->linearizable_barrier())
              .handle_exception_type([](const ss::timed_out_error&) {
                  return result<model::offset>(errc::timeout);
              });
        });
    }

    return _connections.local()
      .with_node_client<cluster::controller_client_protocol>(
        _self,
        ss::this_shard_id(),
        *leader,
        timeout,
        [timeout](controller_client_protocol cp) mutable {
            return cp.get_controller_committed_offset(
              controller_committed_offset_request{},
              rpc::client_opts(model::timeout_clock::now() + timeout));
        })
      .then(&rpc::get_ctx_data<controller_committed_offset_reply>)
      .then([](result<controller_committed_offset_reply> r) {
          if (r.has_error()) {
              return result<model::offset>(r.error());
          } else if (r.value().result != errc::success) {
              return result<model::offset>(r.value().result);
          }
          return result<model::offset>(r.value().last_committed);
      });
}

ss::future<std::error_code> security_frontend::wait_until_caughtup_with_leader(
  model::timeout_clock::duration timeout) {
    /// Grab the leader committed offset - the leader may be this node or
    /// another
    const auto start = model::timeout_clock::now();
    const auto leader_committed = co_await get_leader_committed(timeout);
    if (leader_committed.has_error()) {
        co_return leader_committed.error();
    }

    /// Subtract the difference of the time already spent waiting
    const auto elapsed = model::timeout_clock::now() - start;
    if (elapsed > timeout) {
        co_return make_error_code(errc::timeout);
    }
    timeout -= elapsed;

    /// Waiting up until the leader committed offset means its possible that
    /// waiting on an offset higher then neccessary is performed but the
    /// alternative of waiting on the last_applied isn't a complete solution
    /// as its possible that this offset is behind the actual last_applied
    /// of a previously elected leader, resulting in a stale response being
    /// returned.
    co_return co_await _stm.invoke_on(
      controller_stm_shard,
      [timeout, leader_committed = leader_committed.value()](auto& stm) {
          return stm
            .wait(leader_committed, model::timeout_clock::now() + timeout)
            .then([] { return make_error_code(errc::success); })
            .handle_exception_type([](ss::abort_requested_exception) {
                return make_error_code(errc::shutting_down);
            })
            .handle_exception_type([](ss::timed_out_error) {
                return make_error_code(errc::timeout);
            });
      });
}

} // namespace cluster
