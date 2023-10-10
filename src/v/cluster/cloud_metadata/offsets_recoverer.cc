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
#include "cluster/cloud_metadata/offsets_recovery_rpc_types.h"
#include "kafka/server/group_manager.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record_batch_reader.h"

#include <seastar/core/lowres_clock.hh>

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

ss::future<offsets_recovery_reply>
offsets_recoverer::recover(offsets_recovery_request req) {
    retry_chain_node retry_node(_as, ss::lowres_clock::time_point::max(), 1s);
    offsets_lookup_batcher offsets_lookup(
      _node_id,
      _offsets_lookup.local(),
      _leaders_table.local(),
      _connection_cache.local());
    vlog(
      clusterlog.info,
      "Requested recovery of {} offsets snapshots on NTP {}",
      req.offsets_snapshot_paths.size(),
      req.offsets_ntp);
    const auto& snapshot_paths = req.offsets_snapshot_paths;
    offsets_recovery_reply reply;
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
        absl::btree_set<model::ntp> ntps;
        for (const auto& g : snapshot.groups) {
            for (const auto& tp : g.offsets) {
                for (const auto& po : tp.partitions) {
                    ntps.emplace(
                      model::kafka_namespace, tp.topic, po.partition);
                }
            }
        }
        co_await offsets_lookup.run_lookups(std::move(ntps), retry_node);
        const auto& offsets_by_ntp = offsets_lookup.offsets_by_ntp();
        group_offsets_snapshot batched_groups;
        for (auto& g : snapshot.groups) {
            batched_groups.groups.emplace_back(group_offsets{});
            auto& req_group = batched_groups.groups.back();
            req_group.group_id = g.group_id;

            // Trim offsets as appropriate.
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
                        if (po.offset >= hwm) {
                            // The recovered partition doesn't have all the data
                            // required to restore the snapshot exactly. Trim
                            // such offsets to the recovered partition's end
                            // offset.
                            po.offset = kafka::offset{hwm() - 1};
                        }
                        // Add the NTP to our request topic.
                        const auto& t = tp.topic;
                        auto& req_topic = req_topics[t];
                        req_topic.partitions.emplace_back(
                          po.partition, po.offset);
                    } else {
                        discard_group = true;
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
            if (batched_groups.groups.size() == 1000) {
                auto retry = retry_node.retry();
                batched_groups.offsets_topic_pid = req.offsets_ntp.tp.partition;
                while (retry.is_allowed) {
                    auto err = co_await _group_manager.local().recover_offsets(
                      std::move(batched_groups));
                    if (err == kafka::error_code::none) {
                        break;
                    }
                    retry = retry_node.retry();
                    co_await ss::sleep(retry.delay);
                }
            }
            co_await ss::maybe_yield();
        }
        if (!batched_groups.groups.empty()) {
            auto retry = retry_node.retry();
            batched_groups.offsets_topic_pid = req.offsets_ntp.tp.partition;
            while (retry.is_allowed) {
                auto err = co_await _group_manager.local().recover_offsets(
                  std::move(batched_groups));
                if (err == kafka::error_code::none) {
                    break;
                }
                retry = retry_node.retry();
                co_await ss::sleep(retry.delay);
            }
        }
    }
    co_return reply;
}

} // namespace cluster::cloud_metadata
