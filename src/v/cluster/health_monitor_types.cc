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

#include "cluster/errc.h"
#include "cluster/node/types.h"
#include "features/feature_table.h"
#include "model/adl_serde.h"
#include "utils/to_string.h"

#include <fmt/ostream.h>

#include <chrono>

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

        if (auto topic_it = topics_map.find(tp_ns.tp);
            topic_it != topics_map.end()) {
            auto& [_, partitions] = *topic_it;
            return partitions.empty() || partitions.contains(p_id);
        }
    }

    return false;
}

std::ostream& operator<<(std::ostream& o, const node_state& s) {
    fmt::print(
      o,
      "{{membership_state: {}, is_alive: {}}}",
      s.membership_state,
      s.is_alive);
    return o;
}

std::ostream& operator<<(std::ostream& o, const node_health_report& r) {
    fmt::print(
      o,
      "{{id: {}, data_disk: {}, cache_disk: {}, topics: {}, redpanda_version: "
      "{}, uptime: "
      "{}, logical_version: {}, drain_status: {}}}",
      r.id,
      r.local_state.data_disk,
      r.local_state.cache_disk,
      r.topics,
      r.local_state.redpanda_version,
      r.local_state.uptime,
      r.local_state.logical_version,
      r.drain_status);
    return o;
}

std::ostream& operator<<(std::ostream& o, const cluster_health_report& r) {
    fmt::print(
      o,
      "{{raft0_leader: {}, node_states: {}, node_reports: {}, "
      "bytes_in_cloud_storage: {} }}",
      r.raft0_leader,
      r.node_states,
      r.node_reports,
      r.bytes_in_cloud_storage);
    return o;
}

std::ostream& operator<<(std::ostream& o, const partition_status& ps) {
    fmt::print(
      o,
      "{{id: {}, term: {}, leader_id: {}, revision_id: {}, size_bytes: {}, "
      "under_replicated: {}}}",
      ps.id,
      ps.term,
      ps.leader_id,
      ps.revision_id,
      ps.size_bytes,
      ps.under_replicated_replicas);
    return o;
}

std::ostream& operator<<(std::ostream& o, const topic_status& tl) {
    fmt::print(o, "{{topic: {}, leaders: {}}}", tl.tp_ns, tl.partitions);
    return o;
}

std::ostream& operator<<(std::ostream& o, const node_report_filter& s) {
    fmt::print(
      o,
      "{{include_partitions: {}, ntp_filters: {}}}",
      s.include_partitions,
      s.ntp_filters);
    return o;
}

std::ostream& operator<<(std::ostream& o, const cluster_report_filter& s) {
    fmt::print(
      o, "{{per_node_filter: {}, nodes: {}}}", s.node_report_filter, s.nodes);
    return o;
}

std::ostream& operator<<(std::ostream& o, const partitions_filter& filter) {
    fmt::print(o, "{{");
    for (auto& [ns, tp_f] : filter.namespaces) {
        fmt::print(o, "{{namespace: {}, topics: [", ns);
        for (auto& [tp, p_f] : tp_f) {
            fmt::print(o, "{{topic: {}, paritions: [", tp);
            if (!p_f.empty()) {
                auto it = p_f.begin();
                fmt::print(o, "{}", *it);
                ++it;
                for (; it != p_f.end(); ++it) {
                    fmt::print(o, ",{}", *it);
                }
            }
            fmt::print(o, "] }},");
        }
        fmt::print(o, "]}},");
    }
    fmt::print(o, "}}");

    return o;
}

std::ostream& operator<<(std::ostream& o, const get_node_health_request& r) {
    fmt::print(
      o, "{{filter: {}, current_version: {}}}", r.filter, r.current_version);
    return o;
}

std::ostream& operator<<(std::ostream& o, const get_node_health_reply& r) {
    fmt::print(o, "{{error: {}, report: {}}}", r.error, r.report);
    return o;
}

std::ostream& operator<<(std::ostream& o, const get_cluster_health_request& r) {
    fmt::print(
      o,
      "{{filter: {}, refresh: {}, decoded_version: {}}}",
      r.filter,
      r.refresh,
      r.decoded_version);
    return o;
}

std::ostream& operator<<(std::ostream& o, const get_cluster_health_reply& r) {
    fmt::print(o, "{{error: {}, report: {}}}", r.error, r.report);
    return o;
}

} // namespace cluster
