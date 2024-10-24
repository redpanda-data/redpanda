/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cluster/cluster_recovery_manager.h"

#include "base/seastarx.h"
#include "cloud_storage/cache_service.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_file.h"
#include "cluster/cloud_metadata/cluster_manifest.h"
#include "cluster/cloud_metadata/manifest_downloads.h"
#include "cluster/cluster_recovery_table.h"
#include "cluster/cluster_utils.h"
#include "cluster/commands.h"
#include "cluster/config_frontend.h"
#include "cluster/errc.h"
#include "cluster/feature_manager.h"
#include "cluster/logger.h"
#include "cluster/security_frontend.h"
#include "config/configuration.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sharded.hh>

namespace cluster {

cluster_recovery_manager::cluster_recovery_manager(
  ss::sharded<ss::abort_source>& sharded_as,
  ss::sharded<controller_stm>& controller_stm,
  ss::sharded<features::feature_table>& feature_table,
  ss::sharded<cloud_storage::remote>& remote,
  ss::sharded<cluster_recovery_table>& recovery_table,
  ss::sharded<storage::api>& storage,
  consensus_ptr raft0)
  : _sharded_as(sharded_as)
  , _controller_stm(controller_stm)
  , _feature_table(feature_table)
  , _remote(remote)
  , _recovery_table(recovery_table)
  , _storage(storage)
  , _raft0(std::move(raft0)) {}

ss::future<std::optional<model::term_id>>
cluster_recovery_manager::sync_leader(ss::abort_source& as) {
    if (!_raft0->is_leader()) {
        co_return std::nullopt;
    }
    // If we couldn't heartbeat to a majority, or the term has changed, exit
    // out of the loop.
    auto synced_term = _raft0->term();
    auto res = co_await _raft0->linearizable_barrier();
    if (
      !res.has_value() || !_raft0->is_leader()
      || synced_term != _raft0->term()) {
        co_return std::nullopt;
    }
    // Make sure we catch up to the committed offset.
    const auto committed_offset = _raft0->committed_offset();
    try {
        co_await _controller_stm.local().wait(
          committed_offset, model::timeout_clock::time_point::max(), as);
    } catch (const ss::abort_requested_exception&) {
        co_return std::nullopt;
    }
    if (!_raft0->is_leader() || synced_term != _raft0->term()) {
        co_return std::nullopt;
    }
    vlog(
      clusterlog.debug,
      "Recovery manager synced up to offset {} in term {}",
      committed_offset,
      synced_term);
    co_return synced_term;
}

ss::future<result<cluster::errc, cloud_metadata::error_outcome>>
cluster_recovery_manager::initialize_recovery(
  cloud_storage_clients::bucket_name bucket) {
    if (!_remote.local_is_initialized()) {
        co_return cluster::errc::invalid_request;
    }
    if (config::node().recovery_mode_enabled()) {
        co_return cluster::errc::feature_disabled;
    }
    auto synced_term = co_await sync_leader(_sharded_as.local());
    if (!synced_term.has_value()) {
        co_return cluster::errc::not_leader_controller;
    }
    if (_recovery_table.local().is_recovery_active()) {
        co_return cluster::errc::cluster_already_exists;
    }
    if (
      !_raft0->is_leader()
      || !_storage.local().get_cluster_uuid().has_value()) {
        co_return cluster::errc::not_leader_controller;
    }
    const auto& ignored_uuid = _storage.local().get_cluster_uuid().value();
    // TODO: protect with semaphore with a term check.
    auto fib = retry_chain_node{_sharded_as.local(), 30s, 1s};
    auto cluster_manifest_res
      = co_await cluster::cloud_metadata::download_highest_manifest_in_bucket(
        _remote.local(), bucket, fib, ignored_uuid);
    if (cluster_manifest_res.has_error()) {
        vlog(
          clusterlog.warn,
          "Error finding cluster manifests in bucket {}: {}",
          bucket,
          cluster_manifest_res.error());
        co_return cluster_manifest_res.error();
    }
    auto& manifest = cluster_manifest_res.value();
    vlog(clusterlog.info, "Found cluster manifest for recovery: {}", manifest);

    // Replicate an update to start recovery. Once applied, this will update
    // the recovery table.
    cluster_recovery_init_cmd_data data;
    data.state.manifest = std::move(manifest);
    data.state.bucket = std::move(bucket);
    auto errc = co_await replicate_and_wait(
      _controller_stm,
      _sharded_as,
      cluster_recovery_init_cmd(0, std::move(data)),
      model::timeout_clock::now() + 30s,
      synced_term);
    if (errc != errc::success) {
        vlog(
          clusterlog.warn,
          "Error replicating recovery initialization command: {}",
          errc.message());
        co_return cluster::errc::replication_error;
    }
    co_return cluster::errc::success;
}

ss::future<cluster::errc> cluster_recovery_manager::replicate_update(
  model::term_id term,
  recovery_stage next_stage,
  std::optional<ss::sstring> error_msg) {
    if (!_raft0->is_leader() || term != _raft0->term()) {
        co_return cluster::errc::not_leader_controller;
    }
    cluster_recovery_update_cmd_data data;
    data.stage = next_stage;
    data.error_msg = std::move(error_msg);
    vlog(
      clusterlog.debug,
      "Replicating recovery update command in term {}: {}",
      term,
      next_stage);
    auto errc = co_await replicate_and_wait(
      _controller_stm,
      _sharded_as,
      cluster_recovery_update_cmd(0, std::move(data)),
      model::timeout_clock::now() + 30s,
      std::make_optional(term));
    if (errc != errc::success) {
        vlog(
          clusterlog.warn,
          "Error replicating recovery update command: {}",
          errc.message());
        co_return cluster::errc::replication_error;
    }
    co_return cluster::errc::success;
}

ss::future<std::error_code>
cluster_recovery_manager::apply_update(model::record_batch b) {
    auto offset = b.base_offset();
    auto cmd = co_await cluster::deserialize(std::move(b), commands);
    co_return co_await ss::visit(cmd, [this, offset](auto cmd) {
        return apply_to_table(offset, std::move(cmd));
    });
}
bool cluster_recovery_manager::is_batch_applicable(
  const model::record_batch& b) const {
    return b.header().type == model::record_batch_type::cluster_recovery_cmd;
}

ss::future<std::error_code> cluster_recovery_manager::apply_to_table(
  model::offset offset, cluster_recovery_init_cmd cmd) {
    return dispatch_updates_to_cores(offset, std::move(cmd));
}
ss::future<std::error_code> cluster_recovery_manager::apply_to_table(
  model::offset offset, cluster_recovery_update_cmd cmd) {
    return dispatch_updates_to_cores(offset, std::move(cmd));
}

template<typename Cmd>
ss::future<std::error_code> cluster_recovery_manager::dispatch_updates_to_cores(
  model::offset offset, Cmd cmd) {
    return _recovery_table
      .map([cmd, offset](auto& local_table) {
          return local_table.apply(offset, cmd);
      })
      .then([](std::vector<std::error_code> results) {
          auto first_res = results.front();
          auto state_consistent = std::all_of(
            results.begin(), results.end(), [first_res](std::error_code res) {
                return first_res == res;
            });

          vassert(
            state_consistent,
            "State inconsistency across shards detected, "
            "expected result: {}, have: {}",
            first_res,
            results);

          return first_res;
      });
}

} // namespace cluster
