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
#include "base/vassert.h"
#include "cluster/drain_status.h"
#include "cluster/errc.h"
#include "cluster/node/types.h"
#include "container/chunked_hash_map.h"
#include "features/feature_table.h"
#include "model/adl_serde.h"
#include "model/metadata.h"
#include "serde/rw/map.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

#include <fmt/core.h>

#include <algorithm>
#include <iterator>
#include <optional>
#include <ranges>

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
  topics_t topics,
  std::optional<cluster::drain_status> drain_status,
  struct node_liveness_report node_liveness_report)
  : id(id)
  , local_state(std::move(local_state))
  , topics(std::move(topics))
  , drain_status(drain_status)
  , node_liveness_report(std::move(node_liveness_report)) {}

node_health_report::node_health_report(
  model::node_id id,
  node::local_state local_state,
  chunked_vector<topic_status> topics_vec,
  std::optional<cluster::drain_status> drain_status,
  struct node_liveness_report node_liveness_report)
  : node_health_report(
      id,
      std::move(local_state),
      ss::chunked_table_from_range<node_health_report::topics_t>(
        std::move(topics_vec) | std::views::transform([](topic_status& ts) {
            return std::make_pair(
              std::move(ts.tp_ns), move_to_map(std::move(ts.partitions)));
        })),
      drain_status,
      std::move(node_liveness_report)) {}

node_health_report node_health_report::copy() const {
    return {
      id,
      local_state,
      ss::chunked_table_from_range<topics_t>(
        topics | std::views::transform([](const auto& kv) {
            return std::make_pair(
              kv.first, ss::chunked_hash_map_from_range(kv.second));
        })),
      drain_status,
      node_liveness_report};
}

fmt::iterator node_health_report::format_to(fmt::iterator it) const {
    return node_health_report_serde{*this}.format_to(it);
}

node_health_report_serde::node_health_report_serde(const node_health_report& hr)
  : node_health_report_serde(
      hr.id,
      hr.local_state,
      {std::from_range,
       hr.topics | std::views::transform([](const auto& kv) -> topic_status {
           return {kv.first, copy_to_vector(kv.second)};
       })},
      hr.drain_status,
      hr.node_liveness_report) {}

partition_statuses_map_t
copy_partition_statuses(const partition_statuses_map_t& ps) {
    return ss::chunked_hash_map_from_range(ps);
}

partition_statuses_t copy_to_vector(const partition_statuses_map_t& ps) {
    return {std::from_range, ps | std::views::values};
}
partition_statuses_t move_to_vector(partition_statuses_map_t&& ps) {
    return {
      std::from_range, std::ranges::as_rvalue_view(ps) | std::views::values};
}
partition_statuses_map_t move_to_map(partition_statuses_t&& ps_vec) {
    return ss::chunked_hash_map_from_range(
      std::move(ps_vec) | std::views::transform([](partition_status& ps) {
          return std::make_pair(ps.id, std::move(ps));
      }));
}

partition_statuses_map_t copy_to_map(const partition_statuses_t& ps_vec) {
    return ss::chunked_hash_map_from_range(
      ps_vec | std::views::transform([](const partition_status& ps) {
          return std::make_pair(ps.id, ps);
      }));
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
      "under_replicated_partitions: {}, refresh_failed: {}, "
      "all_members_reported: {}}}",
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
      under_replicated_partitions,
      refresh_failed,
      all_members_reported);
}

} // namespace cluster

namespace cluster::health {

fmt::iterator node_health_version::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{boot:{} counter:{}}}", boot, counter);
}

void approx_timestamp::serde_write(iobuf& out) const {
    auto age = clock_t::now() - value;
    serde::write(
      out, std::chrono::duration_cast<std::chrono::milliseconds>(age));
}

void approx_timestamp::serde_read(iobuf_parser& in, const serde::header& h) {
    auto age = serde::read_nested<std::chrono::milliseconds>(
      in, h._bytes_left_limit);
    value = clock_t::now() - age;
}

// src_timestamp excluded: it uses approx_timestamp which loses precision
// during serde round-trip (serialized as millisecond age).
bool operator==(const health_snapshot& a, const health_snapshot& b) {
    return a.local_state == b.local_state && a.drain_status == b.drain_status
           && a.liveness == b.liveness && a.data == b.data;
}

fmt::iterator health_snapshot::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{local_state: {}, drain_status: {}, liveness: {}, "
      "data_topics: {}}}",
      local_state,
      drain_status,
      liveness,
      data.size());
}

fmt::iterator node_health::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{snapshot: {}, metadata_topics: {}}}", *snapshot, metadata->size());
}

fmt::iterator diff_entry::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{start: {}, end: {}, metadata_diff_topics: {}}}",
      start,
      end,
      metadata_diff ? metadata_diff->size() : 0);
}

topic_partition_metadata_diff_map
as_map(const topic_partition_metadata_diff& vec) {
    return ss::chunked_table_from_range<topic_partition_metadata_diff_map>(
      vec | std::views::transform([](const auto& kv) {
          return std::make_pair(
            kv.first, ss::chunked_hash_map_from_range(kv.second));
      }));
}

namespace {
template<typename Container>
requires std::same_as<Container, topic_partition_metadata_diff_map>
         || std::same_as<Container, topic_partition_metadata_diff_semimap>
metadata_diff_ptr as_vec(const Container& map) {
    return ss::make_lw_shared<topic_partition_metadata_diff>(
      std::from_range,
      map | std::views::transform([](const auto& kv) {
          return std::make_pair(
            kv.first, partition_metadata_diff_list{std::from_range, kv.second});
      }));
}
} // namespace

void diff_entry::compose(const diff_entry& later) {
    vassert(
      end == later.start,
      "Cannot compose non-consecutive diffs: {}..{} and {}..{}",
      start,
      end,
      later.start,
      later.end);

    // build temp map for fast lookups while merging
    auto merged = as_map(*metadata_diff);

    // merge
    for (const auto& [tp_ns, later_parts] : *later.metadata_diff) {
        auto& existing_parts = merged[tp_ns];
        for (const auto& [p_id, later_opt_meta] : later_parts) {
            existing_parts.insert_or_assign(p_id, later_opt_meta);
        }
    }

    // flush merged map back to vec-of-vecs
    metadata_diff = as_vec(merged);

    end = later.end;
    snapshot = later.snapshot;
}

void diff_entry::apply_to(node_health& target) const {
    target.snapshot = snapshot;

    for (const auto& [tp_ns, diff_parts] : *metadata_diff) {
        auto topic_it = target.metadata->try_emplace(tp_ns).first;
        for (const auto& [p_id, maybe_meta] : diff_parts) {
            if (maybe_meta.has_value()) {
                topic_it->second.insert_or_assign(p_id, *maybe_meta);
            } else {
                topic_it->second.erase(p_id);
            }
        }
        if (topic_it->second.empty()) {
            target.metadata->erase(topic_it);
        }
    }
}

diff_entry::diff_entry(
  const node_health& old_report, const node_health& new_report)
  : snapshot(new_report.snapshot)
  , metadata_diff(ss::make_lw_shared<topic_partition_metadata_diff>()) {
    topic_partition_metadata_diff_semimap m;

    // Find changed and added partitions.
    for (const auto& [tp_ns, new_parts] : *new_report.metadata) {
        partition_metadata_diff_list p;
        auto old_topic_it = old_report.metadata->find(tp_ns);
        for (const auto& [p_id, new_meta] : new_parts) {
            if (old_topic_it != old_report.metadata->end()) {
                auto old_it = old_topic_it->second.find(p_id);
                if (
                  old_it != old_topic_it->second.end()
                  && old_it->second == new_meta) {
                    continue;
                }
            }
            p.push_back({p_id, new_meta});
        }
        if (!p.empty()) {
            m[tp_ns] = std::move(p);
        }
    }

    // Find removed partitions (in old but not in new).
    for (const auto& [tp_ns, old_parts] : *old_report.metadata) {
        auto new_topic_it = new_report.metadata->find(tp_ns);
        if (new_topic_it == new_report.metadata->end()) {
            // Entire topic removed — tombstone all partitions.
            m[tp_ns] = {
              std::from_range,
              old_parts | std::views::transform([](const auto& kv) {
                  return std::pair{
                    kv.first, std::optional<partition_metadata>{}};
              })};
        } else {
            for (const auto& [p_id, _] : old_parts) {
                if (!new_topic_it->second.contains(p_id)) {
                    m[tp_ns].push_back({p_id, std::nullopt});
                }
            }
        }
    }

    // Flush map to vec-of-vecs.
    metadata_diff = as_vec(m);
}

diff_entry::diff_entry(diff_entry_serde&& s)
  : start(s.start)
  , end(s.end)
  , snapshot(ss::make_lw_shared<const health_snapshot>(*std::move(s.snapshot)))
  , metadata_diff(
      ss::make_lw_shared<topic_partition_metadata_diff>(
        *std::move(s.metadata_diff))) {}

versioned_report::versioned_report(versioned_report_serde&& s)
  : health{
      .snapshot = ss::make_lw_shared<const health_snapshot>(
        *std::move(s.snapshot)),
      .metadata = ss::make_lw_shared<topic_partition_metadata_map>(
        *std::move(s.metadata)),
    }
  , version(s.version) {}

diff_entry_serde::diff_entry_serde(const diff_entry& d)
  : start(d.start)
  , end(d.end)
  , snapshot(d.snapshot)
  , metadata_diff(d.metadata_diff) {}

ss::future<> diff_entry_serde::serde_async_write(iobuf& out) {
    serde::write(out, start);
    serde::write(out, end);
    co_await serde::write_async(out, *snapshot);
    co_await serde::write_async(out, *metadata_diff);
}

ss::future<>
diff_entry_serde::serde_async_read(iobuf_parser& in, const serde::header h) {
    start = serde::read_nested<node_health_version>(in, h._bytes_left_limit);
    end = serde::read_nested<node_health_version>(in, h._bytes_left_limit);
    snapshot = value_or_foreign<health_snapshot>(
      co_await serde::read_async_nested<health_snapshot>(
        in, h._bytes_left_limit));
    metadata_diff = value_or_foreign<topic_partition_metadata_diff>(
      co_await serde::read_async_nested<topic_partition_metadata_diff>(
        in, h._bytes_left_limit));

    if (in.bytes_left() > h._bytes_left_limit) {
        in.skip(in.bytes_left() - h._bytes_left_limit);
    }
}

versioned_report_serde::versioned_report_serde(const versioned_report& r)
  : snapshot(r.health.snapshot)
  , metadata(r.health.metadata)
  , version(r.version) {}

ss::future<> versioned_report_serde::serde_async_write(iobuf& out) {
    serde::write(out, version);
    co_await serde::write_async(out, *snapshot);
    co_await serde::write_async(out, *metadata);
}

ss::future<> versioned_report_serde::serde_async_read(
  iobuf_parser& in, const serde::header h) {
    version = serde::read_nested<node_health_version>(in, h._bytes_left_limit);
    snapshot = value_or_foreign<health_snapshot>(
      co_await serde::read_async_nested<health_snapshot>(
        in, h._bytes_left_limit));
    metadata = value_or_foreign<topic_partition_metadata_map>(
      co_await serde::read_async_nested<topic_partition_metadata_map>(
        in, h._bytes_left_limit));

    if (in.bytes_left() > h._bytes_left_limit) {
        in.skip(in.bytes_left() - h._bytes_left_limit);
    }
}

node_health_report
to_node_health_report(model::node_id id, const node_health& nh) {
    node_health_report::topics_t topics;
    for (const auto& [tp_ns, data_parts] : nh.snapshot->data) {
        auto& statuses = topics[tp_ns];
        statuses.reserve(data_parts.size());
        auto meta_topic_it = nh.metadata->find(tp_ns);
        for (const auto& [p_id, data] : data_parts) {
            partition_status ps;
            ps.id = p_id;
            ps.size_bytes = data.size_bytes;
            ps.high_watermark = data.high_watermark;
            ps.log_start_offset = data.log_start_offset;
            ps.reclaimable_size_bytes = data.reclaimable_size_bytes;
            ps.cloud_topic_max_gc_eligible_epoch
              = data.cloud_topic_max_gc_eligible_epoch;
            if (meta_topic_it != nh.metadata->end()) {
                auto meta_it = meta_topic_it->second.find(p_id);
                if (meta_it != meta_topic_it->second.end()) {
                    const auto& meta = meta_it->second;
                    ps.term = meta.term;
                    ps.leader_id = meta.leader_id;
                    ps.revision_id = meta.revision_id;
                    ps.under_replicated_replicas
                      = meta.under_replicated_replicas;
                    ps.followers_stats = meta.followers_stats;
                    ps.shard = meta.shard;
                }
            }
            statuses.emplace(p_id, std::move(ps));
        }
    }

    return node_health_report{
      id,
      nh.snapshot->local_state,
      std::move(topics),
      nh.snapshot->drain_status,
      nh.snapshot->liveness};
}

} // namespace cluster::health

namespace cluster {

ss::future<> health_pull_reply::serde_async_write(iobuf& out) {
    serde::write(out, error);
    co_await serde::write_async_vector(out, items);
}

ss::future<>
health_pull_reply::serde_async_read(iobuf_parser& in, const serde::header h) {
    error = serde::read_nested<errc>(in, h._bytes_left_limit);
    items = co_await serde::read_async_vector<decltype(items)>(
      in, h._bytes_left_limit);

    if (in.bytes_left() > h._bytes_left_limit) {
        in.skip(in.bytes_left() - h._bytes_left_limit);
    }
}

} // namespace cluster
