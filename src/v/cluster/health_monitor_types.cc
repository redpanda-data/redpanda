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
      "{{id: {}, disks: {}, topics: {}, redpanda_version: {}, uptime: "
      "{}, logical_version: {}, drain_status: {}}}",
      r.id,
      r.local_state.disks,
      r.topics,
      r.local_state.redpanda_version,
      r.local_state.uptime,
      r.local_state.logical_version,
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
      "under_replicated: {}}}",
      ps.id,
      ps.term,
      ps.leader_id,
      ps.revision_id,
      ps.size_bytes,
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
namespace reflection {

template<typename T>
int8_t read_and_assert_version(std::string_view type, iobuf_parser& parser) {
    auto version = adl<int8_t>{}.from(parser);
    vassert(
      version <= T::current_version,
      "unsupported version of {}, max_supported version: {}, read version: {}",
      type,
      T::current_version,
      version);
    return version;
}

void adl<cluster::node_state>::to(iobuf& out, cluster::node_state&& s) {
    serialize(out, s.current_version, s.id, s.membership_state, s.is_alive);
}

cluster::node_state adl<cluster::node_state>::from(iobuf_parser& p) {
    read_and_assert_version<cluster::node_state>("cluster::node_state", p);

    auto id = adl<model::node_id>{}.from(p);
    auto m_state = adl<model::membership_state>{}.from(p);
    auto is_alive = adl<cluster::alive>{}.from(p);

    return cluster::node_state{
      .id = id,
      .membership_state = m_state,
      .is_alive = is_alive,
    };
}

void adl<cluster::partition_status>::to(
  iobuf& out, cluster::partition_status&& s) {
    // if revision or size is not set fallback to old version, we do it here to
    // prevent old redpanda version from crashing, request handler will decode
    // request version and base on that handle revision_id and size_bytes fields
    // correctly.
    if (s.revision_id == model::revision_id{}) {
        serialize(
          out,
          cluster::partition_status::initial_version,
          s.id,
          s.term,
          s.leader_id);
    } else if (s.size_bytes == cluster::partition_status::invalid_size_bytes) {
        serialize(
          out,
          cluster::partition_status::revision_id_version,
          s.id,
          s.term,
          s.leader_id,
          s.revision_id);
    } else {
        serialize(
          out,
          cluster::partition_status::size_bytes_version,
          s.id,
          s.term,
          s.leader_id,
          s.revision_id,
          s.size_bytes);
    }
}

cluster::partition_status
adl<cluster::partition_status>::from(iobuf_parser& p) {
    auto version = adl<int8_t>{}.from(p);

    auto id = adl<model::partition_id>{}.from(p);
    auto term = adl<model::term_id>{}.from(p);
    auto leader = adl<std::optional<model::node_id>>{}.from(p);
    cluster::partition_status ret{
      .id = id,
      .term = term,
      .leader_id = leader,
    };
    if (version <= cluster::partition_status::revision_id_version) {
        ret.revision_id = adl<model::revision_id>{}.from(p);
    }
    if (version <= cluster::partition_status::size_bytes_version) {
        ret.size_bytes = adl<size_t>{}.from(p);
    }
    return ret;
}

void adl<cluster::topic_status>::to(iobuf& out, cluster::topic_status&& l) {
    return serialize(
      out,
      l.current_version,
      std::move(l.tp_ns.ns),
      std::move(l.tp_ns.tp),
      std::move(l.partitions));
}

cluster::topic_status adl<cluster::topic_status>::from(iobuf_parser& p) {
    read_and_assert_version<cluster::topic_status>("cluster::topic_status", p);

    auto ns = adl<model::ns>{}.from(p);
    auto topic = adl<model::topic>{}.from(p);
    auto partitions = adl<ss::chunked_fifo<cluster::partition_status>>{}.from(
      p);

    return cluster::topic_status(
      model::topic_namespace(std::move(ns), std::move(topic)),
      std::move(partitions));
}

void adl<cluster::node_health_report>::to(
  iobuf& out, cluster::node_health_report&& r) {
    if (r.include_drain_status) {
        reflection::serialize(
          out,
          cluster::node_health_report::current_version,
          r.id,
          std::move(r.local_state.redpanda_version),
          r.local_state.uptime,
          std::move(r.local_state.disks),
          std::move(r.topics),
          std::move(r.local_state.logical_version),
          std::move(r.drain_status));
    } else {
        reflection::serialize(
          out,
          static_cast<int8_t>(1), // version right before maintenance mode added
          r.id,
          std::move(r.local_state.redpanda_version),
          r.local_state.uptime,
          std::move(r.local_state.disks),
          std::move(r.topics),
          std::move(r.local_state.logical_version));
    }
}

cluster::node_health_report
adl<cluster::node_health_report>::from(iobuf_parser& p) {
    auto version = read_and_assert_version<cluster::node_health_report>(
      "cluster::node_health_report", p);

    auto id = adl<model::node_id>{}.from(p);
    auto redpanda_version = adl<cluster::node::application_version>{}.from(p);
    auto uptime = adl<std::chrono::milliseconds>{}.from(p);
    auto disks = adl<std::vector<storage::disk>>{}.from(p);
    auto topics = adl<ss::chunked_fifo<cluster::topic_status>>{}.from(p);
    cluster::cluster_version logical_version{cluster::invalid_version};
    if (version >= 1) {
        logical_version = adl<cluster::cluster_version>{}.from(p);
    }
    std::optional<cluster::drain_manager::drain_status> drain_status;
    if (version >= 2) {
        drain_status
          = adl<std::optional<cluster::drain_manager::drain_status>>{}.from(p);
    }

    return cluster::node_health_report(
      id,
      {.redpanda_version = std::move(redpanda_version),
       .logical_version = std::move(logical_version),
       .uptime = uptime,
       .disks = std::move(disks)},
      std::move(topics),
      false,
      drain_status);
}

void adl<cluster::cluster_health_report>::to(
  iobuf& out, cluster::cluster_health_report&& r) {
    reflection::serialize(
      out,
      r.current_version,
      r.raft0_leader,
      std::move(r.node_states),
      std::move(r.node_reports));
}

cluster::cluster_health_report
adl<cluster::cluster_health_report>::from(iobuf_parser& p) {
    read_and_assert_version<cluster::cluster_health_report>(
      "cluster::cluster_health_report", p);

    auto raft0_leader = adl<std::optional<model::node_id>>{}.from(p);
    auto node_states = adl<std::vector<cluster::node_state>>{}.from(p);
    auto node_reports = adl<std::vector<cluster::node_health_report>>{}.from(p);

    return cluster::cluster_health_report{
      .raft0_leader = raft0_leader,
      .node_states = std::move(node_states),
      .node_reports = std::move(node_reports),
    };
}

void adl<cluster::partitions_filter>::to(
  iobuf& out, cluster::partitions_filter&& filter) {
    std::vector<raw_ns_filter> raw_filters;
    raw_filters.reserve(filter.namespaces.size());
    for (auto& [ns, topics] : filter.namespaces) {
        raw_ns_filter nsf{.ns = ns};
        nsf.topics.reserve(topics.size());
        for (auto& [tp, partitions] : topics) {
            raw_tp_filter tpf{.topic = tp};
            tpf.partitions.reserve(partitions.size());
            std::move(
              partitions.begin(),
              partitions.end(),
              std::back_inserter(tpf.partitions));

            nsf.topics.push_back(std::move(tpf));
        }
        raw_filters.push_back(nsf);
    }

    serialize(out, filter.current_version, std::move(raw_filters));
}

cluster::partitions_filter
adl<cluster::partitions_filter>::from(iobuf_parser& p) {
    read_and_assert_version<cluster::partitions_filter>(
      "cluster::partitions_filter", p);

    cluster::partitions_filter ret;
    auto raw_filters = adl<std::vector<raw_ns_filter>>{}.from(p);
    ret.namespaces.reserve(raw_filters.size());
    for (auto& rf : raw_filters) {
        cluster::partitions_filter::topic_map_t topics;
        topics.reserve(rf.topics.size());
        for (auto& tp : rf.topics) {
            cluster::partitions_filter::partitions_set_t paritions;
            paritions.reserve(tp.partitions.size());
            for (auto& p : tp.partitions) {
                paritions.emplace(p);
            }

            topics.emplace(tp.topic, std::move(paritions));
        }
        ret.namespaces.emplace(std::move(rf.ns), std::move(topics));
    }

    return ret;
}

void adl<cluster::node_report_filter>::to(
  iobuf& out, cluster::node_report_filter&& f) {
    reflection::serialize(
      out, f.current_version, f.include_partitions, std::move(f.ntp_filters));
}

cluster::node_report_filter
adl<cluster::node_report_filter>::from(iobuf_parser& p) {
    read_and_assert_version<cluster::node_report_filter>(
      "cluster::node_report_filter", p);

    auto include_partitions = adl<cluster::include_partitions_info>{}.from(p);
    auto ntp_filters = adl<cluster::partitions_filter>{}.from(p);

    return cluster::node_report_filter{
      .include_partitions = include_partitions,
      .ntp_filters = std::move(ntp_filters),
    };
}

void adl<cluster::cluster_report_filter>::to(
  iobuf& out, cluster::cluster_report_filter&& f) {
    reflection::serialize(
      out,
      f.current_version,
      std::move(f.node_report_filter),
      std::move(f.nodes));
}

cluster::cluster_report_filter
adl<cluster::cluster_report_filter>::from(iobuf_parser& p) {
    read_and_assert_version<cluster::cluster_report_filter>(
      "cluster::cluster_report_filter", p);

    auto node_filter = adl<cluster::node_report_filter>{}.from(p);
    auto nodes = adl<std::vector<model::node_id>>{}.from(p);

    return cluster::cluster_report_filter{
      .node_report_filter = std::move(node_filter),
      .nodes = std::move(nodes),
    };
}

void adl<cluster::get_node_health_request>::to(
  iobuf& out, cluster::get_node_health_request&& req) {
    reflection::serialize(out, req.current_version, std::move(req.filter));
}

cluster::get_node_health_request
adl<cluster::get_node_health_request>::from(iobuf_parser& p) {
    auto version = adl<int8_t>{}.from(p);

    auto filter = adl<cluster::node_report_filter>{}.from(p);

    return cluster::get_node_health_request{
      .filter = std::move(filter),
      .decoded_version = version,
    };
}

void adl<cluster::get_node_health_reply>::to(
  iobuf& out, cluster::get_node_health_reply&& reply) {
    reflection::serialize(out, reply.current_version, std::move(reply.report));
}

cluster::get_node_health_reply
adl<cluster::get_node_health_reply>::from(iobuf_parser& p) {
    read_and_assert_version<cluster::get_node_health_reply>(
      "cluster::get_node_health_reply", p);

    auto report = adl<std::optional<cluster::node_health_report>>{}.from(p);

    return cluster::get_node_health_reply{
      .report = std::move(report),
    };
}

void adl<cluster::get_cluster_health_request>::to(
  iobuf& out, cluster::get_cluster_health_request&& req) {
    reflection::serialize(
      out, req.current_version, std::move(req.filter), req.refresh);
}

cluster::get_cluster_health_request
adl<cluster::get_cluster_health_request>::from(iobuf_parser& p) {
    auto version = adl<int8_t>{}.from(p);

    auto filter = adl<cluster::cluster_report_filter>{}.from(p);
    auto refresh = adl<cluster::force_refresh>{}.from(p);

    return cluster::get_cluster_health_request{
      .filter = std::move(filter),
      .refresh = refresh,
      .decoded_version = version,
    };
}

void adl<cluster::get_cluster_health_reply>::to(
  iobuf& out, cluster::get_cluster_health_reply&& reply) {
    reflection::serialize(
      out, reply.current_version, reply.error, reply.report);
}

cluster::get_cluster_health_reply
adl<cluster::get_cluster_health_reply>::from(iobuf_parser& p) {
    read_and_assert_version<cluster::get_cluster_health_reply>(
      "cluster::get_cluster_health_reply", p);
    auto err = adl<cluster::errc>{}.from(p);
    auto report = adl<std::optional<cluster::cluster_health_report>>{}.from(p);

    return cluster::get_cluster_health_reply{
      .error = err,
      .report = std::move(report),
    };
}

} // namespace reflection
