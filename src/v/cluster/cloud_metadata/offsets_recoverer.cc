/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/cloud_metadata/offsets_recoverer.h"

#include "cloud_storage/cache_service.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_file.h"
#include "cluster/cloud_metadata/offsets_lookup_batcher.h"
#include "cluster/cloud_metadata/offsets_recovery_rpc_types.h"
#include "cluster/logger.h"
#include "kafka/protocol/errors.h"
#include "kafka/server/group_manager.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "rpc/connection_cache.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/timed_out_error.hh>

namespace cluster::cloud_metadata {

offsets_recoverer::offsets_recoverer(
  model::node_id node_id,
  ss::sharded<cloud_storage::remote>& remote,
  ss::sharded<cloud_storage::cache>& cache,
  ss::sharded<offsets_lookup>& local_lookup,
  ss::sharded<cluster::partition_leaders_table>& leaders_table,
  ss::sharded<rpc::connection_cache>& connection_cache,
  ss::sharded<kafka::group_manager>& gm)
  : _node_id(node_id)
  , _remote(remote)
  , _cache(cache)
  , _offsets_lookup(local_lookup)
  , _leaders_table(leaders_table)
  , _connection_cache(connection_cache)
  , _group_manager(gm) {}

ss::future<> offsets_recoverer::stop() {
    _as.request_abort();
    return _gate.close();
}

ss::future<cluster::errc> offsets_recoverer::recover_groups(
  model::partition_id pid,
  group_offsets_snapshot groups,
  retry_chain_node& retry_parent) {
    retry_chain_node batch_node(&retry_parent);
    auto permit = batch_node.retry();
    groups.offsets_topic_pid = pid;
    if (!permit.is_allowed) {
        co_return cluster::errc::timeout;
    }
    auto err = co_await _group_manager.local().recover_offsets(
      std::move(groups));
    if (err == kafka::error_code::none) {
        co_return cluster::errc::success;
    }
    if (
      err == kafka::error_code::not_leader_for_partition
      || err == kafka::error_code::coordinator_load_in_progress
      || err == kafka::error_code::not_coordinator) {
        // NOTE: if returned as an error in a reply, this signals to the
        // leader_router to retry.
        co_return cluster::errc::replication_error;
    }
    co_return cluster::errc::waiting_for_recovery;
}

ss::future<offsets_recovery_reply> offsets_recoverer::recover(
  offsets_recovery_request req, size_t groups_per_batch) {
    offsets_recovery_reply reply;
    if (_gate.is_closed() || _as.abort_requested()) {
        reply.ec = cluster::errc::shutting_down;
        co_return reply;
    }
    auto holder = _gate.hold();
    retry_chain_node retry_node(_as, 300s, 1s);
    vlog(
      clusterlog.info,
      "Requested recovery of {} offsets snapshots on NTP {}",
      req.offsets_snapshot_paths.size(),
      req.offsets_ntp);

    // Offsets recovery comprises of the following steps:
    // - Download the remote snapshot file, deserializing the groups.
    // - Perform lookups on each of the NTPs that are a part of any group.
    // - If any offset falls below the looked up HWM, trim the committed offset
    //   down to the HWM.
    // - Persist the groups, calling into the kafka::group_manager to persist
    //   the group offsets in the underlying offsets topic partition.
    //
    // To help ensure a single snapshot doesn't hog resources, groups are fed
    // into the kafka::group_manager in batches.
    const auto& snapshot_paths = req.offsets_snapshot_paths;
    offsets_lookup_batcher offsets_lookup(
      _node_id,
      _offsets_lookup.local(),
      _leaders_table.local(),
      _connection_cache.local());
    for (const auto& snapshot_path : snapshot_paths) {
        auto offsets_snapshot = cloud_storage::remote_file(
          _remote.local(),
          _cache.local(),
          req.bucket,
          cloud_storage::remote_segment_path{snapshot_path},
          retry_node,
          "offsets_snapshot");
        group_offsets_snapshot snapshot;
        try {
            auto f = co_await offsets_snapshot.hydrate_readable_file();
            auto f_size = co_await f.size();
            ss::file_input_stream_options options;
            auto input = ss::make_file_input_stream(f, options);
            auto snap_buf_parser = iobuf_parser{
              co_await read_iobuf_exactly(input, f_size)};
            snapshot = serde::read<group_offsets_snapshot>(snap_buf_parser);
        } catch (...) {
            reply.ec = cluster::errc::allocation_error;
            co_return reply;
        };
        // Collect the NTPs corresponding to the snapshot file, in preparation
        // to perform some offset lookups.
        absl::btree_set<model::ntp> ntps;
        for (const auto& g : snapshot.groups) {
            for (const auto& tp : g.offsets) {
                for (const auto& po : tp.partitions) {
                    ntps.emplace(
                      model::kafka_namespace, tp.topic, po.partition);
                }
            }
        }
        try {
            co_await offsets_lookup.run_lookups(std::move(ntps), retry_node);
        } catch (const ss::timed_out_error&) {
            reply.ec = cluster::errc::timeout;
            co_return reply;
        }
        const auto& offsets_by_ntp = offsets_lookup.offsets_by_ntp();
        group_offsets_snapshot batched_groups;
        for (auto& g : snapshot.groups) {
            // Collect into a batched request so we can periodically kick off a
            // batch of commits.
            batched_groups.groups.emplace_back(group_offsets{});
            auto& req_group = batched_groups.groups.back();
            req_group.group_id = g.group_id;

            bool discard_group = false;
            absl::flat_hash_map<model::topic, group_offsets::topic_partitions>
              req_topics;
            for (auto& tp : g.offsets) {
                for (auto& po : tp.partitions) {
                    auto ntp = model::ntp{
                      model::kafka_namespace, tp.topic, po.partition};
                    if (auto offset_it = offsets_by_ntp.find(ntp);
                        offset_it != offsets_by_ntp.end()) {
                        auto hwm = offset_it->second;
                        if (po.offset > hwm) {
                            // The recovered partition doesn't have all the data
                            // required to restore the snapshot exactly. Trim
                            // it down to the high watermark.
                            vlog(
                              clusterlog.warn,
                              "Group {} NTP {} trimmed to restored HWM instead "
                              "of snapshotted offset {} < {}",
                              g.group_id,
                              ntp,
                              hwm,
                              po.offset);
                            po.offset = kafka::offset{hwm()};
                        }
                        // Add the NTP to our request topic.
                        const auto& t = tp.topic;
                        auto& req_topic = req_topics[t];
                        req_topic.partitions.emplace_back(
                          po.partition, po.offset);
                    } else {
                        // If there is a group that contains a partition that
                        // wasn't restored for some reason, discard the group.
                        discard_group = true;
                        vlog(
                          clusterlog.warn,
                          "NTP {} doesn't exist, skipping group {} on recovery "
                          "of {}",
                          ntp,
                          g.group_id,
                          req.offsets_ntp);
                        break;
                    }
                }
            }
            if (discard_group) {
                continue;
            }
            for (auto& [t, req_topic] : req_topics) {
                req_topic.topic = t;
                req_group.offsets.emplace_back(std::move(req_topic));
            }

            // If we've amassed a reasonably large set of groups managed by the
            // same partition, send the offset commits.
            if (batched_groups.groups.size() == groups_per_batch) {
                auto errc = co_await recover_groups(
                  req.offsets_ntp.tp.partition,
                  std::move(batched_groups),
                  retry_node);
                if (errc != cluster::errc::success) {
                    reply.ec = errc;
                    co_return reply;
                }
                batched_groups = {};
            }
            co_await ss::maybe_yield();
        }
        // We've processed all groups in the snapshot. Send out any remaining
        // batches of offset commits.
        if (!batched_groups.groups.empty()) {
            auto errc = co_await recover_groups(
              req.offsets_ntp.tp.partition,
              std::move(batched_groups),
              retry_node);
            if (errc != cluster::errc::success) {
                reply.ec = errc;
                co_return reply;
            }
            batched_groups = {};
        }
    }
    co_return reply;
}

} // namespace cluster::cloud_metadata
