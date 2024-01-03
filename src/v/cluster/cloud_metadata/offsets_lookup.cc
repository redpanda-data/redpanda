/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/cloud_metadata/offsets_lookup.h"

#include "base/vlog.h"
#include "cluster/cloud_metadata/offsets_lookup_rpc_types.h"
#include "cluster/logger.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "config/configuration.h"
#include "kafka/server/partition_proxy.h"
#include "model/ktp.h"

#include <seastar/util/later.hh>

namespace cluster::cloud_metadata {

offsets_lookup::offsets_lookup(
  model::node_id node_id,
  ss::sharded<cluster::partition_manager>& pm,
  ss::sharded<cluster::shard_table>& st)
  : _node_id(node_id)
  , _partitions(pm)
  , _shards(st) {}

ss::future<offsets_lookup_reply>
offsets_lookup::lookup(offsets_lookup_request req) {
    auto reply = offsets_lookup_reply{};
    if (req.node_id != _node_id) {
        vlog(
          clusterlog.error,
          "Node {} received offsets lookup request for wrong node_id {}",
          _node_id,
          req.node_id);
        // If for some reason we were sent the wrong request, suggest retrying
        // all NTPs. Perhaps there's a transient metadata issue.
        reply.node_id = _node_id;
        co_return reply;
    }
    reply.node_id = _node_id;
    absl::btree_map<ss::shard_id, fragmented_vector<model::ntp>>
      lookups_per_shard;

    // Group the lookup requests by shard.
    for (auto& ntp : req.ntps) {
        auto shard = _shards.local().shard_for(ntp);
        if (!shard.has_value()) {
            continue;
        }
        lookups_per_shard[shard.value()].emplace_back(std::move(ntp));
    }

    // Send out the requests per shard.
    std::vector<ss::future<offsets_lookup_reply>> pending_shard_replies;
    std::vector<ss::shard_id> shards;
    pending_shard_replies.reserve(lookups_per_shard.size());
    shards.reserve(lookups_per_shard.size());
    for (auto& [shard, ntps] : lookups_per_shard) {
        shards.emplace_back(shard);
        pending_shard_replies.emplace_back(_partitions.invoke_on(
          shard, [ntps = std::move(ntps)](partition_manager& pm) mutable {
              offsets_lookup_reply shard_reply;
              for (auto& ntp : ntps) {
                  auto partition = kafka::make_partition_proxy(
                    model::ktp{ntp.tp.topic, ntp.tp.partition}, pm);
                  if (!partition.has_value()) {
                      // Partition may have moved between scheduling points.
                      continue;
                  }
                  shard_reply.ntp_and_offset.emplace_back(
                    std::move(ntp),
                    model::offset_cast(partition->high_watermark()));
              }
              return shard_reply;
          }));
    }

    // Aggregate the results. Unexpected exceptions are not retriable.
    auto shard_replies = co_await ss::when_all(
      pending_shard_replies.begin(), pending_shard_replies.end());
    vassert(
      shard_replies.size() == shards.size(),
      "{} vs {}",
      shard_replies.size(),
      shards.size());
    size_t shard_idx = 0;
    for (auto& sr : shard_replies) {
        vassert(sr.available(), "waited for future but not available");
        auto shard = shards[shard_idx++];
        try {
            auto&& rep = sr.get();
            for (auto& n : rep.ntp_and_offset) {
                reply.ntp_and_offset.emplace_back(std::move(n));
            }
        } catch (const std::exception& e) {
            vlog(
              clusterlog.error,
              "Error listing offsets on shard {}: {}",
              shard,
              e.what());
        }
        // NOTE: the scheduling point is safe since the state being operated on
        // is local to this frame.
        co_await ss::maybe_yield();
    }
    co_return reply;
}

} // namespace cluster::cloud_metadata
