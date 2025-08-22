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
#include "health_monitor_types.h"
#include "model/adl_serde.h"
#include "model/metadata.h"
#include "utils/to_string.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

#include <fmt/core.h>
#include <fmt/ostream.h>

#include <algorithm>
#include <chrono>
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

        if (auto topic_it = topics_map.find(tp_ns.tp);
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

std::ostream& operator<<(std::ostream& o, const node_state& s) {
    fmt::print(
      o,
      "{{membership_state: {}, is_alive: {}}}",
      s._membership_state,
      s._is_alive);
    return o;
}

node_health_report::node_health_report(
  model::node_id id,
  node::local_state local_state,
  chunked_vector<topic_status> topics_vec,
  std::optional<drain_manager::drain_status> drain_status)
  : id(id)
  , local_state(std::move(local_state))
  , drain_status(drain_status) {
    topics.reserve(topics_vec.size());
    for (auto& topic : topics_vec) {
        topics.emplace(
          std::move(topic.tp_ns), move_to_map(std::move(topic.partitions)));
    }
}

node_health_report node_health_report::copy() const {
    node_health_report ret{id, local_state, {}, drain_status};
    ret.topics.reserve(topics.bucket_count());
    for (const auto& [tp_ns, partitions] : topics) {
        ret.topics.emplace(tp_ns, copy_partition_statuses(partitions));
    }
    ret.version = version;
    ret.last_removal = last_removal;
    return ret;
}

node_health_report new_report_from_deltas(node_health_report_deltas deltas) {
    vassert(deltas.full_report, "Can only create new report from full deltas");
    node_health_report ret;
    ret.id = deltas.id;
    ret.local_state = deltas.local_state;
    ret.drain_status = deltas.drain_status;
    ret.version = deltas.version;
    ret.topics = topic_statuses_from_deltas(std::move(deltas.topics));
    return ret;
}

node_health_report with_deltas_applied(
  const node_health_report& current_report, node_health_report_deltas deltas) {
    const auto& current_statuses = current_report.topics;

    node_health_report updated_report;
    updated_report.id = current_report.id;
    updated_report.local_state = deltas.local_state;
    updated_report.drain_status = deltas.drain_status;
    updated_report.version = deltas.version;
    updated_report.last_removal = current_report.last_removal;

    // include current statuses
    for (auto& [tp_ns, t_partitions] : current_statuses) {
        auto& partitions = updated_report.topics[tp_ns];
        for (auto& [id, status] : t_partitions) {
            partitions[id] = status;
        }
    }
    // apply deltas
    for (auto& topic_deltas : deltas.topics) {
        auto& current_topic
          = updated_report.topics[std::move(topic_deltas.tp_ns)];
        for (auto& partition_status_update : topic_deltas.partitions) {
            auto p_id = partition_status_update.id;
            auto p_it = current_topic.find(p_id);
            if (p_it == current_topic.end()) {
                current_topic.emplace_hint(
                  p_it, p_id, std::move(partition_status_update));
            } else {
                p_it->second = std::move(partition_status_update);
            }
        }
    }
    return updated_report;
}

std::ostream& operator<<(std::ostream& o, const node_health_report& report) {
    fmt::print(
      o,
      "{{id: {}, local_state: {}, topics: {}, drain_status: {}, version: {}, "
      "last_removal: {}}}",
      report.id,
      report.local_state,
      fmt::join(report.topics | std::views::keys, ", "),
      report.drain_status,
      report.version,
      report.last_removal);
    return o;
}

node_health_report_deltas::node_health_report_deltas(
  const node_health_report& hr)
  : node_health_report_deltas(
      hr.id, hr.local_state, {}, hr.drain_status, hr.version) {
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

std::ostream& operator<<(std::ostream& o, const node_health_report_deltas& r) {
    fmt::print(
      o,
      "{{id: {}, topics: {}, local_state: {}, drain_status: {}}}",
      r.id,
      r.topics,
      r.local_state,
      r.drain_status);
    return o;
}

bool operator==(
  const node_health_report_deltas& a, const node_health_report_deltas& b) {
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

std::ostream& operator<<(std::ostream& o, follower_status status) {
    switch (status) {
    case follower_status::in_sync:
        return o << "in_sync";
    case follower_status::out_of_sync:
        return o << "out_of_sync";
    case follower_status::down:
        return o << "down";
    }
}

std::ostream& operator<<(std::ostream& o, const followers_stats& ls) {
    fmt::print(
      o,
      "{{in_sync: {}, out_of_sync: {}, down: "
      "{}}}",
      ls.in_sync,
      ls.out_of_sync,
      ls.down);
    return o;
}

std::ostream& operator<<(std::ostream& o, const partition_status& ps) {
    fmt::print(
      o,
      "{{id: {}, term: {}, leader_id: {}, revision_id: {}, size_bytes: {}, "
      "reclaimable_size_bytes: {}, under_replicated: {}, shard: {}, "
      "followers_stats: {}, last_update: {}}}",
      ps.id,
      ps.term,
      ps.leader_id,
      ps.revision_id,
      ps.size_bytes,
      ps.reclaimable_size_bytes,
      ps.under_replicated_replicas,
      ps.shard,
      ps.followers_stats,
      ps.last_update);
    return o;
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

node_health_report_deltas
node_health_report::get_deltas(report_version last_seen_version) const {
    node_health_report_deltas deltas{
      id, local_state, {}, drain_status, version};

    for (auto& [tp_ns, partitions] : topics) {
        partition_statuses_t updated_partitions;
        for (auto& [p_id, p_status] : partitions) {
            if (p_status.last_update > last_seen_version) {
                updated_partitions.push_back(p_status);
            }
        }
        if (!updated_partitions.empty()) {
            deltas.topics.emplace_back(tp_ns, std::move(updated_partitions));
        }
    }

    return deltas;
}

node_health_report::topics_t
topic_statuses_from_deltas(chunked_vector<topic_status> deltas) {
    node_health_report::topics_t ret;
    ret.reserve(deltas.size());
    for (auto& tp : deltas) {
        ret.emplace(std::move(tp.tp_ns), move_to_map(std::move(tp.partitions)));
    }

    return ret;
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

ss::future<> cluster_health_report::serde_async_write(iobuf& out) {
    using serde::write;
    using serde::write_async;
    // the current version decodes into the decoded version and is used in
    // request handling--that is, it is used at layer above serialization so
    // without further changes we'll need to preserve that behavior.
    write(out, raft0_leader);
    write(out, node_states);
    write(out, static_cast<serde::serde_size_t>(node_reports.size()));
    for (auto& nr : node_reports) {
        co_await write_async(out, node_health_report_deltas{*nr});
    }
    write(out, bytes_in_cloud_storage);
}

ss::future<> cluster_health_report::serde_async_read(
  iobuf_parser& in, const serde::header& h) {
    using serde::read_async_nested;
    using serde::read_nested;
    raft0_leader = read_nested<std::optional<model::node_id>>(
      in, h._bytes_left_limit);
    node_states = read_nested<std::vector<node_state>>(in, h._bytes_left_limit);
    const auto sz = read_nested<serde::serde_size_t>(in, h._bytes_left_limit);
    node_reports.reserve(sz);
    for (auto i = 0U; i < sz; ++i) {
        auto r = co_await read_async_nested<node_health_report_deltas>(
          in, h._bytes_left_limit);
        node_reports.emplace_back(
          ss::make_lw_shared<node_health_report>(
            new_report_from_deltas(std::move(r))));
    }
    bytes_in_cloud_storage = read_nested<std::optional<size_t>>(
      in, h._bytes_left_limit);

    if (in.bytes_left() > h._bytes_left_limit) {
        in.skip(in.bytes_left() - h._bytes_left_limit);
    }
}

void cluster_health_report::serde_write(iobuf& out) {
    using serde::write;

    // the current version decodes into the decoded version and is used in
    // request handling--that is, it is used at layer above serialization so
    // without further changes we'll need to preserve that behavior.
    write(out, raft0_leader);
    write(out, node_states);
    write(out, static_cast<serde::serde_size_t>(node_reports.size()));
    for (auto& nr : node_reports) {
        write(out, node_health_report_deltas{*nr});
    }
    write(out, bytes_in_cloud_storage);
}

void cluster_health_report::serde_read(
  iobuf_parser& in, const serde::header& h) {
    using serde::read_nested;
    raft0_leader = read_nested<std::optional<model::node_id>>(
      in, h._bytes_left_limit);
    node_states = read_nested<std::vector<node_state>>(in, h._bytes_left_limit);
    const auto sz = read_nested<serde::serde_size_t>(in, h._bytes_left_limit);
    node_reports.reserve(sz);
    for (auto i = 0U; i < sz; ++i) {
        auto r = read_nested<node_health_report_deltas>(
          in, h._bytes_left_limit);
        node_reports.emplace_back(
          ss::make_lw_shared<node_health_report>(
            new_report_from_deltas(std::move(r))));
    }
    bytes_in_cloud_storage = read_nested<std::optional<size_t>>(
      in, h._bytes_left_limit);

    if (in.bytes_left() > h._bytes_left_limit) {
        in.skip(in.bytes_left() - h._bytes_left_limit);
    }
}

std::ostream& operator<<(std::ostream& o, const topic_status& tl) {
    fmt::print(o, "{{topic: {}, partitions: {}}}", tl.tp_ns, tl.partitions);
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
    fmt::print(o, "{{target_node_id: {}}}", r.get_target_node_id());
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

void restart_risk_report::push(
  partitions_t restart_risk_report::* member,
  const model::topic_namespace& nt,
  model::partition_id pid) {
    auto& list = this->*member;
    if (list.size() < limit) {
        list.emplace_back(nt.ns, nt.tp, pid);
    }
}

std::ostream& operator<<(std::ostream& o, const cluster_health_overview& ho) {
    fmt::print(
      o,
      "{{controller_id: {}, nodes: {}, unhealthy_reasons: {}, nodes_down: {}, "
      "nodes_in_recovery_mode: {}, bytes_in_cloud_storage: {}, "
      "leaderless_count: {}, under_replicated_count: {}, "
      "leaderless_partitions: {}, under_replicated_partitions: {}}}",
      ho.controller_id,
      ho.all_nodes,
      ho.unhealthy_reasons,
      ho.nodes_down,
      ho.nodes_in_recovery_mode,
      ho.bytes_in_cloud_storage,
      ho.leaderless_count,
      ho.under_replicated_count,
      ho.leaderless_partitions,
      ho.under_replicated_partitions);
    return o;
}

} // namespace cluster
