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
  ss::sharded<controller_stm>& s, ss::sharded<ss::abort_source>& as)
  : _stm(s)
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
  model::timeout_clock::time_point timeout) {
    if (unlikely(bindings.empty())) {
        co_return std::vector<errc>{};
    }

    const auto num_bindings = bindings.size();
    create_acls_cmd_data data;
    data.bindings = std::move(bindings);
    create_acls_cmd cmd(std::move(data), 0 /* unused */);

    errc err;
    try {
        auto ec = co_await replicate_and_wait(std::move(cmd), timeout);
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
