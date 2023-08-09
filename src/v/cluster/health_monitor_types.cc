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

#include "cluster/drain_manager.h"
#include "cluster/errc.h"
#include "cluster/node/types.h"
#include "features/feature_table.h"
#include "model/adl_serde.h"
#include "model/metadata.h"
#include "utils/to_string.h"

#include <seastar/core/chunked_fifo.hh>

#include <fmt/ostream.h>

#include <algorithm>
#include <chrono>
#include <iterator>

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

node_health_report::node_health_report(
  model::node_id id,
  node::local_state local_state,
  ss::chunked_fifo<topic_status> topics,
  bool include_drain_status,
  std::optional<drain_manager::drain_status> drain_status)
  : id(id)
  , local_state(std::move(local_state))
  , topics(std::move(topics))
  , include_drain_status(include_drain_status)
  , drain_status(drain_status) {}

node_health_report::node_health_report(const node_health_report& other)
  : id(other.id)
  , local_state(other.local_state)
  , topics()
  , include_drain_status(other.include_drain_status)
  , drain_status(other.drain_status) {
    std::copy(
      other.topics.cbegin(), other.topics.cend(), std::back_inserter(topics));
}

node_health_report&
node_health_report::operator=(const node_health_report& other) {
    if (this == &other) {
        return *this;
    }
    id = other.id;
    local_state = other.local_state;
    include_drain_status = other.include_drain_status;
    drain_status = other.drain_status;
    ss::chunked_fifo<topic_status> t;
    t.reserve(other.topics.size());
    std::copy(
      other.topics.cbegin(), other.topics.cend(), std::back_inserter(t));
    topics = std::move(t);
    return *this;
}

std::ostream& operator<<(std::ostream& o, const node_health_report& r) {
    fmt::print(
      o,
      "{{id: {}, topics: {}, local_state: {}, drain_status: {}}}",
      r.id,
      r.topics,
      r.local_state,
      r.drain_status);
    return o;
}
bool operator==(const node_health_report& a, const node_health_report& b) {
    return a.id == b.id && a.local_state == b.local_state
           && a.drain_status == b.drain_status
           && a.topics.size() == b.topics.size()
           && std::equal(
             a.topics.cbegin(),
             a.topics.cend(),
             b.topics.cbegin(),
             b.topics.cend());
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
      "reclaimable_size_bytes: {}, under_replicated: {}}}",
      ps.id,
      ps.term,
      ps.leader_id,
      ps.revision_id,
      ps.size_bytes,
      ps.reclaimable_size_bytes,
      ps.under_replicated_replicas);
    return o;
}

topic_status& topic_status::operator=(const topic_status& rhs) {
    if (this == &rhs) {
        return *this;
    }

    ss::chunked_fifo<partition_status> p;
    p.reserve(rhs.partitions.size());
    std::copy(
      rhs.partitions.begin(), rhs.partitions.end(), std::back_inserter(p));

    tp_ns = rhs.tp_ns;
    partitions = std::move(p);
    return *this;
}

topic_status::topic_status(
  model::topic_namespace tp_ns, ss::chunked_fifo<partition_status> partitions)
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
