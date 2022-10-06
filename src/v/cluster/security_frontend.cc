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
#include "raft/types.h"
#include "random/generators.h"
#include "rpc/errc.h"
#include "rpc/types.h"
#include "security/authorizer.h"
#include "security/scram_algorithm.h"

#include <seastar/core/coroutine.hh>

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
  ss::sharded<controller_stm>& s,
  ss::sharded<rpc::connection_cache>& connections,
  ss::sharded<partition_leaders_table>& leaders,
  ss::sharded<features::feature_table>& features,
  ss::sharded<ss::abort_source>& as,
  ss::sharded<security::authorizer>& authorizer)
  : _self(self)
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
    return replicate_and_wait(_stm, _features, _as, std::move(cmd), tout);
}

ss::future<std::error_code> security_frontend::delete_user(
  security::credential_user username, model::timeout_clock::time_point tout) {
    delete_user_cmd cmd(std::move(username), 0 /* unused */);
    return replicate_and_wait(_stm, _features, _as, std::move(cmd), tout);
}

ss::future<std::error_code> security_frontend::update_user(
  security::credential_user username,
  security::scram_credential credential,
  model::timeout_clock::time_point tout) {
    update_user_cmd cmd(std::move(username), std::move(credential));
    return replicate_and_wait(_stm, _features, _as, std::move(cmd), tout);
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
          _stm,
          _features,
          _as,
          std::move(cmd),
          model::timeout_clock::now() + timeout);
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
          _stm,
          _features,
          _as,
          std::move(cmd),
          model::timeout_clock::now() + timeout);
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

    ss::sstring creds_str = creds_str_ptr;
    auto colon = creds_str.find(":");
    if (colon == ss::sstring::npos || colon == creds_str.size() - 1) {
        // Malformed value.  Do not log the value, it may be malformed
        // but it is still a secret.
        vlog(
          clusterlog.warn,
          "Invalid value of {} (expected \"username:password\")",
          bootstrap_user_env_key);
        return {};
    }

    auto username = security::credential_user{creds_str.substr(0, colon)};
    auto password = creds_str.substr(colon + 1);
    auto credentials = security::scram_sha256::make_credentials(
      password, security::scram_sha256::min_iterations);
    return std::optional<user_and_credential>(
      std::in_place, std::move(username), std::move(credentials));
}

} // namespace cluster
