/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cluster/cloud_metadata/offsets_lookup_batcher.h"

#include "base/vlog.h"
#include "cluster/cloud_metadata/offsets_lookup_rpc_types.h"
#include "cluster/logger.h"
#include "cluster/offsets_recovery_rpc_service.h"
#include "rpc/connection_cache.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/net/packet.hh>

namespace cluster::cloud_metadata {

ss::future<offsets_lookup_reply>
offsets_lookup_batcher::send_request(offsets_lookup_request req) {
    if (req.node_id == _node_id) {
        vlog(
          clusterlog.debug,
          "Processing local offset lookup request for {} NTPs on node {}",
          req.ntps.size(),
          req.node_id);
        co_return co_await _local_lookup.lookup(std::move(req));
    }
    vlog(
      clusterlog.debug,
      "Sending offset lookup request for {} NTPs to node {}",
      req.ntps.size(),
      req.node_id);
    auto num_requested_ntps = req.ntps.size();
    auto empty_reply = offsets_lookup_reply{};
    empty_reply.node_id = req.node_id;

    auto opts = rpc::client_opts(30s);
    auto timeout = opts.timeout;
    auto node_id = req.node_id;
    auto reply
      = co_await _connection_cache
          .with_node_client<offsets_recovery_client_protocol>(
            _node_id,
            ss::this_shard_id(),
            node_id,
            timeout,
            [opts = std::move(opts), r = std::move(req)](auto client) mutable {
                return client.offsets_lookup(std::move(r), std::move(opts));
            })
          .then(&rpc::get_ctx_data<offsets_lookup_reply>);
    if (reply.has_error()) {
        vlog(
          clusterlog.error,
          "Error sending offsets request for {} NTPs to node {}: {}",
          num_requested_ntps,
          empty_reply.node_id,
          reply.error());
        co_return empty_reply;
    }
    co_return std::move(reply.value());
}

ss::future<> offsets_lookup_batcher::run_lookups(
  absl::btree_set<model::ntp> ntps_to_lookup, retry_chain_node& parent_node) {
    retry_chain_node retry_node(&parent_node);
    // Keep sending out lookup requests until a non-retriable error is hit, or
    // until there are no more NTPs to retry.
    while (!ntps_to_lookup.empty()) {
        auto retry_permit = retry_node.retry();
        if (!retry_permit.is_allowed) {
            vlog(
              clusterlog.error,
              "Timed out running lookups with {} NTPs to lookup",
              ntps_to_lookup.size());
            throw ss::timed_out_error();
        }
        vlog(clusterlog.debug, "Looking up {} NTPs", ntps_to_lookup.size());
        std::vector<ss::future<offsets_lookup_reply>> pending_replies;
        absl::btree_map<model::node_id, offsets_lookup_request>
          pending_requests_per_node;

        // Copy the NTPs to send. As we process duplicates, we may end up
        // removing from our lookup set.
        fragmented_vector<model::ntp> ntps(
          ntps_to_lookup.begin(), ntps_to_lookup.end());

        // Batch up all our lookup requests per node.
        std::vector<model::node_id> nodes;
        for (auto& ntp : ntps) {
            if (_offset_by_ntp.contains(ntp)) {
                ntps_to_lookup.erase(ntp);
                continue;
            }
            auto leader_id = _leaders_table.get_leader(ntp).value_or(
              model::node_id{});
            if (leader_id == model::node_id{}) {
                // Perhaps no leader? Retry the ntp in the next iteration.
                continue;
            }
            auto& node_req = pending_requests_per_node[leader_id];
            node_req.node_id = leader_id;
            node_req.ntps.emplace_back(std::move(ntp));

            // Once we hit the target per node, send out the request.
            if (node_req.ntps.size() == _batch_size) {
                nodes.emplace_back(leader_id);
                pending_replies.emplace_back(send_request(std::move(node_req)));
                pending_requests_per_node.erase(leader_id);
            }
        }
        // Send all remaining batched requests, regardless of size.
        for (auto& [node_id, node_req] : pending_requests_per_node) {
            nodes.emplace_back(node_id);
            pending_replies.emplace_back(send_request(std::move(node_req)));
        }

        // Materialize the replies from the futures.
        auto per_node_replies = co_await ss::when_all(
          pending_replies.begin(), pending_replies.end());
        vassert(
          per_node_replies.size() == nodes.size(),
          "{} vs {}",
          per_node_replies.size(),
          nodes.size());
        size_t node_idx = 0;
        for (auto& r : per_node_replies) {
            vassert(r.available(), "waited for future but not available");
            auto node_id = nodes[node_idx++];
            if (r.failed()) {
                vlog(
                  clusterlog.error,
                  "Error while looking up offsets on node {}: {}",
                  node_id,
                  r.get_exception());
                continue;
            }
            auto reply = r.get();
            vlog(
              clusterlog.debug,
              "Node {} (replied node_id {}) responded with offsets for {} NTPs",
              node_id,
              reply.node_id,
              reply.ntp_and_offset.size());
            for (auto& [ntp, offset] : reply.ntp_and_offset) {
                if (offset == kafka::offset{}) {
                    continue;
                }
                // Success! Add it to our map.
                ntps_to_lookup.erase(ntp);
                _offset_by_ntp.emplace(std::move(ntp), offset);
            }
        }
        if (ntps_to_lookup.empty()) {
            co_return;
        }
        co_await ss::sleep_abortable(
          retry_node.get_backoff(), retry_node.root_abort_source());
    }
}

} // namespace cluster::cloud_metadata
