/*
 * Copyright 2020 Vectorized, Inc.
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
#include "cluster/controller_stm.h"
#include "cluster/errc.h"
#include "cluster/logger.h"
#include "cluster/partition_allocator.h"
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
  ss::sharded<ss::abort_source>& as)
  : _self(self)
  , _stm(s)
  , _connections(connections)
  , _leaders(leaders)
  , _as(as) {}

ss::future<std::error_code> security_frontend::create_user(
  security::credential_user username,
  security::scram_credential credential,
  model::timeout_clock::time_point tout) {
    create_user_cmd cmd(std::move(username), std::move(credential));
    return replicate_and_wait(std::move(cmd), tout);
}

ss::future<std::error_code> security_frontend::delete_user(
  security::credential_user username, model::timeout_clock::time_point tout) {
    delete_user_cmd cmd(std::move(username), 0 /* unused */);
    return replicate_and_wait(std::move(cmd), tout);
}

ss::future<std::error_code> security_frontend::update_user(
  security::credential_user username,
  security::scram_credential credential,
  model::timeout_clock::time_point tout) {
    update_user_cmd cmd(std::move(username), std::move(credential));
    return replicate_and_wait(std::move(cmd), tout);
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
          std::move(cmd), model::timeout_clock::now() + timeout);
        err = map_errc(ec);
    } catch (const std::exception& e) {
        vlog(clusterlog.warn, "Unable to create ACLs: {}", e);
        err = errc::replication_error;
    }

    std::vector<errc> result;
    result.assign(num_bindings, err);
    co_return result;
}

template<typename Cmd>
ss::future<std::error_code> security_frontend::replicate_and_wait(
  Cmd&& cmd, model::timeout_clock::time_point timeout) {
    return _stm.invoke_on(
      controller_stm_shard,
      [cmd = std::forward<Cmd>(cmd), &as = _as, timeout](
        controller_stm& stm) mutable {
          return serialize_cmd(std::forward<Cmd>(cmd))
            .then([&stm, timeout, &as](model::record_batch b) {
                return stm.replicate_and_wait(
                  std::move(b), timeout, as.local());
            });
      });
}

} // namespace cluster
