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
#include "cluster/node/types.h"
#include "health_monitor_types.h"
#include "model/metadata.h"

#include <seastar/core/chunked_fifo.hh>

#include <fmt/format.h>
#include <fmt/ostream.h>

#include <algorithm>
#include <iterator>

namespace cluster {

static_assert(std::forward_iterator<unsorted_col_t<long>::lw_const_iterator>);

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
  chunked_vector<topic_status> topics,
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
    chunked_vector<topic_status> t;
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
      "reclaimable_size_bytes: {}, under_replicated: {}, shard: {}}}",
      ps.id,
      ps.term,
      ps.leader_id,
      ps.revision_id,
      ps.size_bytes,
      ps.reclaimable_size_bytes,
      ps.under_replicated_replicas,
      ps.shard);
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

partition_status_materializing_iterator::
  partition_status_materializing_iterator(
    partition_cstore_iterators_t iterators)
  : _iterators(std::move(iterators)) {}

namespace details {
topic_range::topic_range(model::topic tp, size_t ns_idx, size_t partition_count)
  : tp(std::move(tp))
  , namespace_index(ns_idx)
  , partition_count(partition_count) {}
} // namespace details

topic_status_view::topic_status_view(
  const model::ns& ns,
  const model::topic& tp,
  size_t partition_count,
  partition_status_materializing_iterator begin,
  partition_status_materializing_iterator end)
  : tp_ns(ns, tp)
  , partitions(partition_count, std::move(begin), std::move(end)) {}

topics_store_iterator::topics_store_iterator(
  topic_ranges_t::const_iterator topic_it,
  partition_status_materializing_iterator partition_it,
  const topics_store* parent)
  : _topic_it(topic_it)
  , _partition_it(std::move(partition_it))
  , _parent(parent) {}
namespace {
partition_status_materializing_iterator
advance(const partition_status_materializing_iterator& it, size_t n) {
    partition_status_materializing_iterator next(it);
    for (size_t i = 0; i < n; ++i) {
        ++next;
    }
    return next;
}
} // namespace
const topic_status_view& topics_store_iterator::dereference() const {
    if (!_current_value) [[likely]] {
        _partition_it_next = advance(_partition_it, _topic_it->partition_count);

        _current_value.emplace(
          _parent->ns(_topic_it->namespace_index),
          _topic_it->tp,
          _topic_it->partition_count,
          _partition_it,
          *_partition_it_next);
    }

    return *_current_value;
}

size_t topics_store_iterator::index() const {
    return std::distance(_parent->_topics.begin(), _topic_it);
}

bool topics_store_iterator::is_end() const {
    return _topic_it == _parent->_topics.end();
}

void topics_store_iterator::increment() {
    _current_value.reset();
    ++_topic_it;
    if (_topic_it == _parent->_topics.end()) {
        _partition_it = _parent->_partitions.end();
    } else {
        if (!_partition_it_next) {
            _partition_it_next = advance(
              _partition_it, _topic_it->partition_count);
        }
        _partition_it = std::move(*_partition_it_next);
        _partition_it_next.reset();
    }
}

bool topics_store_iterator::equal(const topics_store_iterator& other) const {
    return std::tie(_topic_it, _parent, _partition_it)
           == std::tie(other._topic_it, other._parent, other._partition_it);
}
namespace {
template<size_t idx, typename T>
std::optional<T> get_optional(auto iterators) {
    T value = T{*std::get<idx>(iterators)};
    if (value < 0) {
        return std::nullopt;
    }
    return value;
}
}; // namespace
const partition_status&
partition_status_materializing_iterator::dereference() const {
    if (!_current.has_value()) {
        using idx_t = partition_statuses_cstore::column_idx;
        _current = partition_status{
          .id = get_as<idx_t::id, model::partition_id>(),
          .term = get_as<idx_t::term, model::term_id>(),
          .leader_id = get_optional<idx_t::leader, model::node_id>(_iterators),
          .revision_id = get_as<idx_t::revision_id, model::revision_id>(),
          .size_bytes = get_as<idx_t::size_bytes, size_t>(),
          .under_replicated_replicas
          = get_as<idx_t::under_replicated_replicas, uint8_t>(),
          .reclaimable_size_bytes
          = get_as<idx_t::reclaimable_size_bytes, size_t>(),
          .shard = get_as<idx_t::shard, uint32_t>(),
        };
    }
    return _current.value();
}

void partition_status_materializing_iterator::increment() {
    _current = std::nullopt;
    std::apply([](auto&... it) { (++it, ...); }, _iterators);
}

bool partition_status_materializing_iterator::equal(
  const partition_status_materializing_iterator& other) const {
    return _iterators == other._iterators;
}

void partition_statuses_cstore::push_back(const partition_status& status) {
    _id.append(status.id);
    _term.append(status.term);
    _leader.append(status.leader_id.value_or(model::node_id(-1)));
    _revision_id.append(status.revision_id);
    _size_bytes.append(status.size_bytes);
    _under_replicated_replicas.append(
      status.under_replicated_replicas.value_or(0));
    _reclaimable_size_bytes.append(status.reclaimable_size_bytes.value_or(0));
    _shard.append(status.shard);
    _size++;
}

void topics_store::append(
  model::topic_namespace tp_ns,
  const chunked_vector<partition_status>& statuses) {
    _topics.emplace_back(
      std::move(tp_ns.tp), namespace_idx(tp_ns.ns), statuses.size());

    for (const auto& ps : statuses) {
        _partitions.push_back(ps);
    }
}

partition_status_materializing_iterator
partition_statuses_cstore::begin() const {
    return partition_status_materializing_iterator(internal_begin());
}
partition_status_materializing_iterator partition_statuses_cstore::end() const {
    return partition_status_materializing_iterator(internal_end());
}

node_health_report
columnar_node_health_report::materialize_legacy_report() const {
    node_health_report ret;
    ret.id = id;
    ret.drain_status = drain_status;
    ret.include_drain_status = true;
    ret.local_state = local_state;
    ret.topics.reserve(topics.size());
    for (auto& t : topics) {
        partition_statuses_t partitions;
        partitions.reserve(t.partitions.size());
        for (auto& p : t.partitions) {
            partitions.push_back(p);
        }

        ret.topics.emplace_back(
          model::topic_namespace(t.tp_ns), std::move(partitions));
    }
    return ret;
}

columnar_node_health_report columnar_node_health_report::copy() const {
    columnar_node_health_report ret;
    ret.id = id;
    ret.drain_status = drain_status;
    ret.local_state = local_state;
    ret.topics = topics.copy();
    return ret;
}

topics_store topics_store::copy() const {
    topics_store ret;
    ret._topics = _topics.copy();
    ret._namespaces = _namespaces;
    ret._partitions = _partitions.copy();
    return ret;
}

partition_statuses_cstore partition_statuses_cstore::copy() const {
    partition_statuses_cstore ret;
    ret._size = _size;
    std::apply(
      [&](auto&&... dest) mutable {
          std::apply(
            [&](auto&&... src) mutable { ((dest = src.copy()), ...); },
            columns());
      },
      ret.columns());

    return ret;
}

get_node_health_reply get_node_health_reply::copy() const {
    return get_node_health_reply{
      .error = error,
      .report = report,
      .columnar_report = columnar_report
                           ? std::make_optional(columnar_report->copy())
                           : std::nullopt,
    };
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
    fmt::print(
      o,
      "{{filter: {}, use_columnar_format: {}}}",
      r.filter,
      r.use_columnar_format);
    return o;
}

std::ostream& operator<<(std::ostream& o, const get_node_health_reply& r) {
    fmt::print(
      o,
      "{{error: {}, report: {}, columnar_report: {}}}",
      r.error,
      r.report,
      r.columnar_report);
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

namespace {
template<typename Range>
void print_range(std::ostream& o, const Range& range) {
    if (range.size() > 0) {
        auto it = range.begin();
        auto end = range.end();
        fmt::print(o, "{}", *it);
        ++it;
        for (; it != end; ++it) {
            fmt::print(o, ", {}", *it);
        }
    }
}
} // namespace

std::ostream&
operator<<(std::ostream& o, const columnar_node_health_report& report) {
    fmt::print(
      o,
      "{{id: {}, local_state: {}, drain_status: {}, topics: [",
      report.id,
      report.local_state,
      report.drain_status);
    print_range(o, report.topics);
    fmt::print(o, "]}}");

    return o;
}
std::ostream& operator<<(std::ostream& o, const topic_status_view& store) {
    fmt::print(o, "{{tp_ns: {}, partitions: [", store.tp_ns);
    print_range(o, store.partitions);
    fmt::print(o, "]}}");
    return o;
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
