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
#include "cloud_storage/remote.h"
#include "cluster/cloud_metadata/cluster_manifest.h"
#include "cluster/cloud_metadata/error_outcome.h"
#include "cluster/cloud_metadata/manifest_downloads.h"
#include "cluster/cluster_recovery_table.h"
#include "cluster/cluster_utils.h"
#include "cluster/commands.h"
#include "cluster/errc.h"
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
  ss::sharded<cloud_storage::remote>& remote,
  ss::sharded<cluster_recovery_table>& recovery_table,
  ss::sharded<storage::api>& storage,
  consensus_ptr raft0)
  : _sharded_as(sharded_as)
  , _controller_stm(controller_stm)
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

namespace {

ss::future<checked<void, cluster_recovery_manager::errc>> check_can_use_uuid(
  cloud_storage::remote& _remote,
  cloud_storage_clients::bucket_name bucket,
  std::optional<ss::sstring> cluster_name,
  model::cluster_uuid cluster_uuid_override,
  retry_chain_node& fib) {
    // Validate that the provided cluster UUID is owned by the configured
    // cluster name or that the bucket does not contain any cluster
    // name references.
    if (cluster_name.has_value()) {
        auto uses_cluster_id_res
          = co_await cluster::cloud_metadata::check_cluster_name_owns_uuid(
            _remote, bucket, cluster_name.value(), cluster_uuid_override, fib);
        if (!uses_cluster_id_res.has_value()) {
            vlog(
              clusterlog.warn,
              "Error checking cluster name ownership: {}",
              uses_cluster_id_res.error());
            co_return cluster_recovery_manager::errc::unknown;
        }
        if (!uses_cluster_id_res.value()) {
            vlog(
              clusterlog.warn,
              "Cluster name {} does not own cluster UUID {}. Cannot proceed.",
              *cluster_name,
              cluster_uuid_override);
            co_return cluster_recovery_manager::errc::misconfigured;
        }
    } else {
        // If cluster name is not configured we need to error out if the
        // bucket contains any cluster name references to avoid customer
        // shooting themselves in the foot.
        auto uses_cluster_id_res = co_await cluster::cloud_metadata::
          check_bucket_contains_cluster_names(_remote, bucket, fib);
        if (!uses_cluster_id_res.has_value()) {
            vlog(
              clusterlog.warn,
              "Error checking if bucket contains cluster names: {}",
              uses_cluster_id_res.error());
            co_return cluster_recovery_manager::errc::unknown;
        }
        if (uses_cluster_id_res.value()) {
            vlog(
              clusterlog.warn,
              "Cluster name references exist in bucket {}, but no cluster "
              "name is configured. Cannot proceed. Please check "
              "`cloud_storage_cluster_name` config property.",
              bucket());
            co_return cluster_recovery_manager::errc::misconfigured;
        }
    }

    co_return outcome::success();
}

ss::future<checked<
  cloud_metadata::cluster_metadata_manifest,
  cluster_recovery_manager::errc>>
download_manifest_for_recovery(
  cloud_storage::remote& _remote,
  cloud_storage_clients::bucket_name bucket,
  std::optional<model::cluster_uuid> cluster_uuid_override,
  std::optional<ss::sstring> cluster_name,
  model::cluster_uuid ignored_uuid,
  retry_chain_node& fib) {
    if (!cluster_uuid_override.has_value()) {
        vlog(
          clusterlog.info,
          "No cluster UUID override provided. Will search for latest manifest "
          "(cluster name: {})",
          cluster_name);
        auto cluster_manifest_res = co_await cluster::cloud_metadata::
          download_highest_manifest_in_bucket(
            _remote, bucket, fib, cluster_name, ignored_uuid);
        if (cluster_manifest_res.has_error()) {
            vlog(
              clusterlog.warn,
              "Error finding cluster manifests: {}",
              cluster_manifest_res.error());
            if (
              cluster_manifest_res.error()
              == cloud_metadata::error_outcome::no_matching_metadata) {
                co_return cluster_recovery_manager::errc::no_matching_metadata;
            }
            co_return cluster_recovery_manager::errc::unknown;
        }
        co_return std::move(cluster_manifest_res.value());
    }

    vlog(
      clusterlog.info,
      "Using user-provided cluster UUID override for recovery (cluster uuid "
      "override: {}, cluster name: {})",
      cluster_uuid_override,
      cluster_name);

    auto can_use_res = co_await check_can_use_uuid(
      _remote, bucket, cluster_name, cluster_uuid_override.value(), fib);
    if (can_use_res.has_error()) {
        co_return can_use_res.error();
    }

    auto cluster_manifest_res
      = co_await cluster::cloud_metadata::download_highest_manifest_for_cluster(
        _remote, cluster_uuid_override.value(), bucket, fib);
    if (cluster_manifest_res.has_error()) {
        vlog(
          clusterlog.warn,
          "Error finding cluster manifests for cluster UUID "
          "{}: {}",
          cluster_uuid_override.value(),
          cluster_manifest_res.error());
        if (
          cluster_manifest_res.error()
          == cloud_metadata::error_outcome::no_matching_metadata) {
            co_return cluster_recovery_manager::errc::no_matching_metadata;
        }
        co_return cluster_recovery_manager::errc::unknown;
    }
    co_return std::move(cluster_manifest_res.value());
}

} // namespace

ss::future<checked<void, cluster_recovery_manager::errc>>
cluster_recovery_manager::initialize_recovery(
  cloud_storage_clients::bucket_name bucket,
  std::optional<model::cluster_uuid> cluster_uuid_override) {
    if (!_remote.local_is_initialized()) {
        vlog(clusterlog.warn, "Cloud storage is not configured/initialized");
        co_return errc::misconfigured;
    }
    if (config::node().recovery_mode_enabled()) {
        vlog(
          clusterlog.warn, "Node is in recovery mode, cannot start recovery");
        co_return errc::misconfigured;
    }
    auto synced_term = co_await sync_leader(_sharded_as.local());
    if (!synced_term.has_value()) {
        vlog(clusterlog.warn, "Not leader, cannot start recovery");
        co_return errc::not_a_leader;
    }
    if (_recovery_table.local().is_recovery_active()) {
        co_return errc::already_in_progress;
    }
    if (
      !_raft0->is_leader()
      || !_storage.local().get_cluster_uuid().has_value()) {
        vlog(
          clusterlog.warn,
          "Lost leadership or storage uuid disappeared, cannot start recovery");
        co_return errc::not_a_leader;
    }
    auto cluster_name = config::shard_local_cfg().cloud_storage_cluster_name();
    const auto& this_cluster_uuid = _storage.local().get_cluster_uuid().value();
    // TODO: protect with semaphore with a term check.
    auto fib = retry_chain_node{_sharded_as.local(), 30s, 1s};
    auto manifest_res = co_await download_manifest_for_recovery(
      _remote.local(),
      bucket,
      cluster_uuid_override,
      cluster_name,
      this_cluster_uuid,
      fib);
    if (manifest_res.has_error()) {
        co_return manifest_res.error();
    }
    vlog(
      clusterlog.info,
      "Found cluster manifest for recovery: {}",
      manifest_res.value());

    // Replicate an update to start recovery. Once applied, this will update
    // the recovery table.
    cluster_recovery_init_cmd_data data;
    data.state.manifest = std::move(manifest_res.value());
    data.state.bucket = std::move(bucket);
    auto errc = co_await replicate_and_wait(
      _controller_stm,
      _sharded_as,
      cluster_recovery_init_cmd(0, std::move(data)),
      model::timeout_clock::now() + 30s,
      synced_term);
    if (errc != cluster::errc::success) {
        vlog(
          clusterlog.warn,
          "Error replicating recovery initialization command: {}",
          errc.message());
        co_return errc::unknown;
    }
    co_return outcome::success();
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
    if (errc != cluster::errc::success) {
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
