/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "config_frontend.h"

#include "cluster/cluster_utils.h"

namespace cluster {

config_frontend::config_frontend(
  ss::sharded<controller_stm>& stm, ss::sharded<ss::abort_source>& as)
  : _stm(stm)
  , _as(as) {}

ss::future<> config_frontend::start() { return ss::now(); }
ss::future<> config_frontend::stop() { return ss::now(); }

/**
 * Issue a write of new configuration to the raft0 log.  Any pre-write
 * validation of the content should have been done already (e.g. in admin API
 * handler).  This function will naively write the config values through,
 * so any invalid settings will show up in the config_manager (and as 'invalid'
 * entries in the config_status to nodes they propagate to).
 *
 * Must be called on `version_shard` to enable serialized generation of
 * version numbers.
 */
ss::future<std::error_code> config_frontend::patch(
  config_update_request const& update,
  model::timeout_clock::time_point timeout) {
    vassert(
      ss::this_shard_id() == version_shard, "Must be called on version_shard");

    if (_next_version == config_version_unset) {
        // Race with config_manager initialization which is responsible for
        // setting _next_version once loaded.
        co_return errc::waiting_for_recovery;
    }

    auto data = cluster_config_delta_cmd_data();

    data.upsert = std::move(update.upsert);
    data.remove = std::move(update.remove);

    // Take _write_lock to serialize updates to _next_version
    co_return co_await _write_lock.with([this, data, timeout] {
        auto cmd = cluster_config_delta_cmd(_next_version, data);
        vlog(
          clusterlog.trace,
          "patch: writing delta with version {}",
          _next_version);
        return replicate_and_wait(_stm, _as, std::move(cmd), timeout);
    });
}

/**
 * Called with the content of an incoming config_status_request: a
 * remote node is informing us of its status.  Write it into the log,
 * from whence config_manager will read it back into their in-memory
 * map of node statuses.
 */
ss::future<std::error_code> config_frontend::set_status(
  config_status& status, model::timeout_clock::time_point timeout) {
    vlog(clusterlog.trace, "set_status from node {}: {}", status.node, status);
    if (status.node < 0) {
        co_return errc::node_does_not_exists;
    }

    auto data = cluster_config_status_cmd_data();
    data.status = status;
    auto cmd = cluster_config_status_cmd(status.node, data);
    co_return co_await replicate_and_wait(_stm, _as, std::move(cmd), timeout);
}

/**
 * For config_manager to notify the frontend of what the next version
 * number should be.
 *
 * Must be called on version_shard (the shard where writes are
 * serialized to generate version numbers).
 */
void config_frontend::set_next_version(config_version v) {
    vassert(
      ss::this_shard_id() == version_shard, "Must be called on version_shard");
    vlog(clusterlog.trace, "set_next_version: {}", v);
    _next_version = v;
}

} // namespace cluster