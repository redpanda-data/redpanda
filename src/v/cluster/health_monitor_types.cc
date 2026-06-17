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
#include "cluster/health_monitor_types.h"

#include "base/format_to.h"
#include "cluster/drain_status.h"
#include "cluster/errc.h"
#include "cluster/node/types.h"
#include "features/feature_table.h"
#include "model/adl_serde.h"
#include "model/metadata.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

#include <fmt/core.h>

#include <algorithm>
#include <iterator>
#include <optional>

namespace cluster {

bool partitions_filter::matches(const model::ntp& ntp) const {
    return matches(model::topic_namespace_view(ntp), ntp.tp.partition);
}

bool partitions_filter::matches(
  model::topic_namespace_view tp_ns, model::partition_id p_id) const {
    if (namespaces.empty()) {
        return true;
    }

    if (auto it = namespaces.find(tp_ns.ns); it != namespaces.end()) {
        auto& [_, topics_map] = *it;

        if (topics_map.empty()) {
            return true;
        }

        if (
          auto topic_it = topics_map.find(tp_ns.tp);
          topic_it != topics_map.end()) {
            auto& [_, partitions] = *topic_it;
            return partitions.empty() || partitions.contains(p_id);
        }
    }

    return false;
}

node_state::node_state(
  model::node_id id, model::membership_state membership_state, alive is_alive)
  : _id(id)
  , _membership_state(membership_state)
  , _is_alive(is_alive) {}
fmt::iterator node_state::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{membership_state: {}, is_alive: {}}}",
      _membership_state,
      _is_alive);
}
fmt::iterator node_liveness_report::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{node_id_to_last_seen: {}}}", node_id_to_last_seen);
}

bool operator==(const node_liveness_report& a, const node_liveness_report& b) {
    return std::ranges::equal(a.node_id_to_last_seen, b.node_id_to_last_seen);
}

node_health_report::node_health_report(
  model::node_id id,
  node::local_state local_state,
  chunked_vector<topic_status> topics_vec,
  std::optional<cluster::drain_status> drain_status,
  struct node_liveness_report node_liveness_report)
  : id(id)
  , local_state(std::move(local_state))
  , drain_status(drain_status)
  , node_liveness_report(std::move(node_liveness_report)) {
    topics.reserve(topics_vec.size());
    for (auto& topic : topics_vec) {
        topics.emplace(
          std::move(topic.tp_ns), move_to_map(std::move(topic.partitions)));
    }
}

node_health_report node_health_report::copy() const {
    node_health_report ret{
      id, local_state, {}, drain_status, node_liveness_report};
    ret.topics.reserve(topics.bucket_count());
    for (const auto& [tp_ns, partitions] : topics) {
        ret.topics.emplace(tp_ns, copy_partition_statuses(partitions));
    }
    return ret;
}

fmt::iterator node_health_report::format_to(fmt::iterator it) const {
    return node_health_report_serde{*this}.format_to(it);
}

node_health_report_serde::node_health_report_serde(const node_health_report& hr)
  : node_health_report_serde(
      hr.id,
      hr.local_state,
      /* topics */ {},
      hr.drain_status,
      hr.node_liveness_report) {
    topics.reserve(hr.topics.size());
    for (const auto& [tp_ns, partitions] : hr.topics) {
        topics.emplace_back(tp_ns, copy_to_vector(partitions));
    }
}

partition_statuses_map_t
copy_partition_statuses(const partition_statuses_map_t& ps) {
    partition_statuses_map_t ret;
    ret.reserve(ps.size());
    for (const auto& [p_id, status] : ps) {
        ret.emplace(p_id, status);
    }
    return ret;
}

partition_statuses_t copy_to_vector(const partition_statuses_map_t& ps) {
    partition_statuses_t vec;
    vec.reserve(ps.size());
    std::ranges::copy(ps | std::views::values, std::back_inserter(vec));
    return vec;
}
partition_statuses_t move_to_vector(partition_statuses_map_t&& ps) {
    partition_statuses_t vec;
    vec.reserve(ps.size());
    std::ranges::move(ps | std::views::values, std::back_inserter(vec));
    return vec;
}
partition_statuses_map_t move_to_map(partition_statuses_t&& ps_vec) {
    partition_statuses_map_t ret;
    ret.reserve(ps_vec.size());
    for (auto& status : ps_vec) {
        ret.emplace(status.id, std::move(status));
    }
    return ret;
}

partition_statuses_map_t copy_to_map(const partition_statuses_t& ps_vec) {
    partition_statuses_map_t ret;
    ret.reserve(ps_vec.size());
    for (const auto& status : ps_vec) {
        ret.emplace(status.id, status);
    }
    return ret;
}
fmt::iterator node_health_report_serde::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{id: {}, topics: {}, local_state: {}, drain_status: {}, "
      "node_liveness_report {}}}",
      id,
      topics,
      local_state,
      drain_status,
      node_liveness_report);
}

bool operator==(
  const node_health_report_serde& a, const node_health_report_serde& b) {
    return a.id == b.id && a.local_state == b.local_state
           && a.drain_status == b.drain_status
           && a.topics.size() == b.topics.size()
           && std::equal(
             a.topics.cbegin(),
             a.topics.cend(),
             b.topics.cbegin(),
             b.topics.cend())
           && a.node_liveness_report == b.node_liveness_report;
}
fmt::iterator cluster_health_report::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{raft0_leader: {}, node_states: {}, node_reports_count: {}, "
      "bytes_in_cloud_storage: {} }}",
      raft0_leader,
      node_states,
      node_reports.size(),
      bytes_in_cloud_storage);
}
fmt::iterator format_to(follower_status e, fmt::iterator out) {
    switch (e) {
    case follower_status::in_sync:
        return fmt::format_to(out, "in_sync");
    case follower_status::out_of_sync:
        return fmt::format_to(out, "out_of_sync");
    case follower_status::down:
        return fmt::format_to(out, "down");
    }
}
fmt::iterator followers_stats::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{in_sync: {}, out_of_sync: {}, down: "
      "{}}}",
      in_sync,
      out_of_sync,
      down);
}
fmt::iterator partition_status::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{id: {}, term: {}, leader_id: {}, revision_id: {}, size_bytes: {}, "
      "reclaimable_size_bytes: {}, under_replicated: {}, shard: {}, "
      "followers_stats: {}, kafka_highwatermark: {}, ct_max_gc_epoch: {}, "
      "log_start_offset: {}}}",
      id,
      term,
      leader_id,
      revision_id,
      size_bytes,
      reclaimable_size_bytes,
      under_replicated_replicas,
      shard,
      followers_stats,
      high_watermark,
      cloud_topic_max_gc_eligible_epoch,
      log_start_offset);
}

topic_status& topic_status::operator=(const topic_status& rhs) {
    if (this == &rhs) {
        return *this;
    }

    partition_statuses_t p;
    p.reserve(rhs.partitions.size());
    std::copy(
      rhs.partitions.begin(), rhs.partitions.end(), std::back_inserter(p));

    tp_ns = rhs.tp_ns;
    partitions = std::move(p);
    return *this;
}

topic_status::topic_status(
  model::topic_namespace tp_ns, partition_statuses_t partitions)
  : tp_ns(std::move(tp_ns))
  , partitions(std::move(partitions)) {}

topic_status::topic_status(const topic_status& o)
  : tp_ns(o.tp_ns) {
    std::copy(
      o.partitions.cbegin(),
      o.partitions.cend(),
      std::back_inserter(partitions));
}
bool operator==(const topic_status& a, const topic_status& b) {
    return a.tp_ns == b.tp_ns && a.partitions.size() == b.partitions.size()
           && std::equal(
             a.partitions.cbegin(),
             a.partitions.cend(),
             b.partitions.cbegin(),
             b.partitions.cend());
}

cluster_health_report cluster_health_report::copy() const {
    cluster_health_report r;
    r.raft0_leader = raft0_leader;
    r.node_states = node_states;
    r.bytes_in_cloud_storage = bytes_in_cloud_storage;
    r.node_reports.reserve(node_reports.size());
    for (auto& nr : node_reports) {
        r.node_reports.emplace_back(ss::make_lw_shared(nr->copy()));
    }
    return r;
}

get_cluster_health_reply get_cluster_health_reply::copy() const {
    get_cluster_health_reply reply{.error = error};
    if (report.has_value()) {
        reply.report = report->copy();
    }
    return reply;
}
fmt::iterator topic_status::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{topic: {}, partitions: {}}}", tp_ns, partitions);
}
fmt::iterator node_report_filter::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{include_partitions: {}, ntp_filters: {}}}",
      include_partitions,
      ntp_filters);
}
fmt::iterator cluster_report_filter::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{per_node_filter: {}, nodes: {}}}", node_report_filter, nodes);
}
fmt::iterator partitions_filter::format_to(fmt::iterator it) const {
    it = fmt::format_to(it, "{{");
    for (auto& [ns, tp_f] : namespaces) {
        it = fmt::format_to(it, "{{namespace: {}, topics: [", ns);
        for (auto& [tp, p_f] : tp_f) {
            it = fmt::format_to(it, "{{topic: {}, partitions: [", tp);
            if (!p_f.empty()) {
                auto pit = p_f.begin();
                it = fmt::format_to(it, "{}", *pit);
                ++pit;
                for (; pit != p_f.end(); ++pit) {
                    it = fmt::format_to(it, ",{}", *pit);
                }
            }
            it = fmt::format_to(it, "] }},");
        }
        it = fmt::format_to(it, "]}},");
    }
    return fmt::format_to(it, "}}");
}

fmt::iterator get_node_health_request::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{target_node_id: {}}}", get_target_node_id());
}
fmt::iterator get_node_health_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{error: {}, report: {}}}", error, report);
}
fmt::iterator get_cluster_health_request::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{filter: {}, refresh: {}, decoded_version: {}}}",
      filter,
      refresh,
      decoded_version);
}
fmt::iterator get_cluster_health_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{error: {}, report: {}}}", error, report);
}

void restart_risk_report::push(
  partitions_t restart_risk_report::* member,
  const model::topic_namespace& nt,
  model::partition_id pid) {
    auto& list = this->*member;
    if (list.size() < limit) {
        list.emplace_back(nt.ns, nt.tp, pid);
    }
}
fmt::iterator cluster_health_overview::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{controller_id: {}, nodes: {}, unhealthy_reasons: {}, nodes_down: {}, "
      "high_disk_usage_nodes: {}, nodes_in_recovery_mode: {}, "
      "bytes_in_cloud_storage: {}, leaderless_count: {}, "
      "under_replicated_count: {}, leaderless_partitions: {}, "
      "under_replicated_partitions: {}}}",
      controller_id,
      all_nodes,
      unhealthy_reasons,
      nodes_down,
      high_disk_usage_nodes,
      nodes_in_recovery_mode,
      bytes_in_cloud_storage,
      leaderless_count,
      under_replicated_count,
      leaderless_partitions,
      under_replicated_partitions);
}

} // namespace cluster
