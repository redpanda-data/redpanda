/*
 * Copyright 2020 Vectorized, Inc.
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
#include "model/adl_serde.h"
#include "utils/human.h"
#include "utils/to_string.h"

#include <fmt/ostream.h>

namespace cluster {

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
      "{{id: {}, disk_space: {}, topics: {}, redpanda_version: {}}}",
      r.id,
      r.disk_space,
      r.topics,
      r.redpanda_version);
    return o;
}

std::ostream& operator<<(std::ostream& o, const cluster_health_report& r) {
    fmt::print(
      o,
      "{{raft0_leader: {}, node_states: {}, node_reports: {} }}",
      r.raft0_leader,
      r.node_states,
      r.node_reports);
    return o;
}

std::ostream& operator<<(std::ostream& o, const node_disk_space& s) {
    fmt::print(
      o,
      "{{path: {}, free: {}, total: {}}}",
      s.path,
      human::bytes(s.free),
      human::bytes(s.total));
    return o;
}

std::ostream& operator<<(std::ostream& o, const partition_status& pl) {
    fmt::print(
      o, "{{id: {}, term: {}, leader_id: {}}}", pl.id, pl.term, pl.leader_id);
    return o;
}

std::ostream& operator<<(std::ostream& o, const topic_status& tl) {
    fmt::print(o, "{{topic: {}, leaders: {}}}", tl.tp_ns, tl.partitions);
    return o;
}
} // namespace cluster
namespace reflection {

template<typename T>
void read_and_assert_version(std::string_view type, iobuf_parser& parser) {
    auto version = adl<int8_t>{}.from(parser);
    vassert(
      version <= T::current_version,
      "unsupported version of {}, max_supported version: {}, read version: {}",
      type,
      version,
      T::current_version);
}

void adl<cluster::node_disk_space>::to(
  iobuf& out, cluster::node_disk_space&& s) {
    serialize(out, s.current_version, s.path, s.free, s.total);
}

cluster::node_disk_space adl<cluster::node_disk_space>::from(iobuf_parser& p) {
    read_and_assert_version<cluster::node_disk_space>(
      "cluster::node_disk_space", p);

    auto path = adl<ss::sstring>{}.from(p);
    auto free = adl<uint64_t>{}.from(p);
    auto total = adl<uint64_t>{}.from(p);

    return cluster::node_disk_space{
      .path = path,
      .free = free,
      .total = total,
    };
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
    serialize(out, s.current_version, s.id, s.term, s.leader_id);
}

cluster::partition_status
adl<cluster::partition_status>::from(iobuf_parser& p) {
    read_and_assert_version<cluster::partition_status>(
      "cluster::partition_status", p);

    auto id = adl<model::partition_id>{}.from(p);
    auto term = adl<model::term_id>{}.from(p);
    auto leader = adl<std::optional<model::node_id>>{}.from(p);

    return cluster::partition_status{
      .id = id,
      .term = term,
      .leader_id = leader,
    };
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
    auto partitions = adl<std::vector<cluster::partition_status>>{}.from(p);

    return cluster::topic_status{
      .tp_ns = model::topic_namespace(std::move(ns), std::move(topic)),
      .partitions = std::move(partitions),
    };
}

void adl<cluster::node_health_report>::to(
  iobuf& out, cluster::node_health_report&& r) {
    reflection::serialize(
      out,
      r.current_version,
      r.id,
      std::move(r.redpanda_version),
      std::move(r.disk_space),
      std::move(r.topics));
}

cluster::node_health_report
adl<cluster::node_health_report>::from(iobuf_parser& p) {
    read_and_assert_version<cluster::node_health_report>(
      "cluster::node_health_report", p);

    auto id = adl<model::node_id>{}.from(p);
    auto redpanda_version = adl<cluster::application_version>{}.from(p);
    auto disk_space = adl<std::vector<cluster::node_disk_space>>{}.from(p);
    auto topics = adl<std::vector<cluster::topic_status>>{}.from(p);

    return cluster::node_health_report{
      .id = id,
      .redpanda_version = std::move(redpanda_version),
      .disk_space = std::move(disk_space),
      .topics = std::move(topics),
    };
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
} // namespace reflection
