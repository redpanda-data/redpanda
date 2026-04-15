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
#pragma once
#include "absl/container/node_hash_map.h"
#include "absl/container/node_hash_set.h"
#include "base/format_to.h"
#include "bytes/iobuf_parser.h"
#include "cluster/drain_status.h"
#include "cluster/errc.h"
#include "cluster/node/types.h"
#include "cluster/types.h"
#include "container/chunked_hash_map.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "rpc/types.h"
#include "serde/async.h"
#include "serde/rw/bool_class.h"
#include "serde/rw/chrono.h"
#include "serde/rw/envelope.h"
#include "serde/rw/optional.h"
#include "serde/rw/pair.h"
#include "serde/rw/rw.h"
#include "serde/rw/scalar.h"
#include "serde/rw/vector.h"
#include "utils/named_type.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/util/bool_class.hh>

namespace cluster {

inline constexpr ss::shard_id health_monitor_backend_shard = 0;
/**
 * Health reports
 */

using alive = ss::bool_class<struct node_alive_tag>;

// An application version is a software release, like v1.2.3_gfa0d09f8a
using application_version = named_type<ss::sstring, struct version_number_tag>;

/**
 * node state is determined from controller, and it doesn't require contacting
 * with the node directly
 */
struct node_state
  : serde::envelope<node_state, serde::version<0>, serde::compat_version<0>> {
    static constexpr int8_t current_version = 0;
    node_state(
      model::node_id id,
      model::membership_state membership_state,
      alive is_alive);

    node_state() = default;
    node_state(const node_state&) = default;
    node_state(node_state&&) noexcept = default;
    node_state& operator=(const node_state&) = default;
    node_state& operator=(node_state&&) noexcept = default;
    ~node_state() noexcept = default;

    model::node_id id() const { return _id; }

    model::membership_state membership_state() const {
        return _membership_state;
    }
    // clang-format off
    [[deprecated("please use health_monitor_frontend::is_alive() to query for "
                 "liveness")]] 
    alive is_alive() const {
        return _is_alive;
    }
    // clang-format on
    fmt::iterator format_to(fmt::iterator it) const;

    friend bool operator==(const node_state&, const node_state&) = default;

    auto serde_fields() { return std::tie(_id, _membership_state, _is_alive); }

private:
    model::node_id _id;
    model::membership_state _membership_state;
    alive _is_alive;
};

enum class follower_status { in_sync, out_of_sync, down };
fmt::iterator format_to(follower_status s, fmt::iterator);

struct followers_stats
  : serde::
      envelope<followers_stats, serde::version<0>, serde::compat_version<0>> {
    // all distinct
    int16_t in_sync = 1; // leader is always in sync
    std::vector<model::node_id> out_of_sync;
    std::vector<model::node_id> down;

    auto serde_fields() { return std::tie(in_sync, out_of_sync, down); }

    fmt::iterator format_to(fmt::iterator it) const;
    friend bool
    operator==(const followers_stats&, const followers_stats&) = default;
};
struct partition_status
  : serde::
      envelope<partition_status, serde::version<7>, serde::compat_version<0>> {
    static constexpr size_t invalid_size_bytes = size_t(-1);
    static constexpr uint32_t invalid_shard_id = uint32_t(-1);

    model::partition_id id;
    model::term_id term;
    std::optional<model::node_id> leader_id;
    model::revision_id revision_id;
    size_t size_bytes;
    // on leaders only, counts under-replicated replicas, whether live or not
    std::optional<uint8_t> under_replicated_replicas;

    /*
     * estimated amount of data subject to reclaim under disk pressure without
     * violating safety guarantees. this is useful for the partition balancer
     * which is interested in free space on a node. a node may have very little
     * physical free space, but have effective free space represented by
     * reclaimable size bytes.
     *
     * an intuitive relationship between size_bytes and reclaimable_size_bytes
     * would have the former being >= than the later. however due to the way
     * that data is collected it is conceivable that this inequality doesn't
     * hold. callers should check for this condition and normalize the values or
     * ignore the update.
     */
    std::optional<size_t> reclaimable_size_bytes;

    uint32_t shard = invalid_shard_id;

    // present on leaders only
    std::optional<followers_stats> followers_stats;

    /**
     * Kafka high watermark of partition replica, this offset is only populated
     * for Kafka partitions. For other partitions the offset stays not
     * initialized.
     */
    kafka::offset high_watermark;

    /*
     * If the partition is a cloud topics partition, then this field records the
     * maximum epoch (inclusive) that is eligible for garbage collection. When
     * reducing (applying `min`) this value across node reports, ignore
     * std::nullopt. If the reduced value is std::nullopt then the partition
     * should be treated as if it contains no GC eligible data.
     */
    std::optional<int64_t> cloud_topic_max_gc_eligible_epoch;

    /**
     * Kafka log start offset (first readable offset) for this partition
     * replica. Only populated for Kafka namespace partitions. std::nullopt
     * when reported by nodes running an older version.
     */
    std::optional<kafka::offset> log_start_offset;

    auto serde_fields() {
        return std::tie(
          id,
          term,
          leader_id,
          revision_id,
          size_bytes,
          under_replicated_replicas,
          reclaimable_size_bytes,
          shard,
          followers_stats,
          high_watermark,
          cloud_topic_max_gc_eligible_epoch,
          log_start_offset);
    }

    fmt::iterator format_to(fmt::iterator it) const;
    friend bool
    operator==(const partition_status&, const partition_status&) = default;
};

using partition_statuses_t = chunked_vector<partition_status>;
using partition_statuses_map_t
  = chunked_hash_map<model::partition_id, partition_status>;

struct topic_status
  : serde::envelope<topic_status, serde::version<0>, serde::compat_version<0>> {
    static constexpr int8_t current_version = 0;

    topic_status() = default;
    topic_status(model::topic_namespace, partition_statuses_t);
    topic_status& operator=(const topic_status&);
    topic_status(const topic_status&);
    topic_status& operator=(topic_status&&) = default;
    topic_status(topic_status&&) = default;
    ~topic_status() = default;

    model::topic_namespace tp_ns;
    partition_statuses_t partitions;
    fmt::iterator format_to(fmt::iterator it) const;
    friend bool operator==(const topic_status&, const topic_status&);

    auto serde_fields() { return std::tie(tp_ns, partitions); }
};

/**
 * Status for the automatic decommissioning of dead nodes
 */

struct node_liveness_report_data {
    // map between a given node id and how long ago that node was last seen
    // no entry will be present for a given node if it has not been seen
    absl::flat_hash_map<model::node_id, rpc::clock_type::duration>
      node_id_to_last_seen;
};

struct node_liveness_report
  : serde::envelope<
      node_liveness_report,
      serde::version<0>,
      serde::compat_version<0>>
  , node_liveness_report_data {
    static constexpr int8_t current_version = 0;

    using node_liveness_report_data::node_liveness_report_data;

    // NOLINTNEXTLINE hicpp-explicit-conversions
    node_liveness_report(node_liveness_report_data data) noexcept
      : node_liveness_report_data{std::move(data)} {
        static_assert(
          sizeof(node_liveness_report_data) == sizeof(node_liveness_report));
    }

    fmt::iterator format_to(fmt::iterator it) const;

    auto serde_fields() { return std::tie(node_id_to_last_seen); }

    friend bool
    operator==(const node_liveness_report& a, const node_liveness_report& b);
};

/**
 * Node health report is collected built based on node local state at given
 * instance of time
 */
struct node_health_report {
    using topics_t = chunked_hash_map<
      model::topic_namespace,
      partition_statuses_map_t,
      model::topic_namespace_hash,
      model::topic_namespace_eq>;

    model::node_id id;
    node::local_state local_state;
    topics_t topics;
    std::optional<cluster::drain_status> drain_status;
    node_liveness_report node_liveness_report;

    node_health_report(
      model::node_id,
      node::local_state,
      chunked_vector<topic_status>,
      std::optional<cluster::drain_status>,
      struct node_liveness_report);

    node_health_report(
      model::node_id,
      node::local_state,
      topics_t,
      std::optional<cluster::drain_status>,
      struct node_liveness_report);

    node_health_report copy() const;

    fmt::iterator format_to(fmt::iterator it) const;
};

using node_health_report_ptr
  = ss::foreign_ptr<ss::lw_shared_ptr<const node_health_report>>;

// This struct is different from node_health_report because for the latter we
// want additional flexibility in how we store partition replica statuses (so
// that searching for a replica status doesn't require a full scan). The _serde
// variant is used for RPC serde and is more constrained for the reasons of
// backwards compat.
struct node_health_report_serde
  : serde::envelope<
      node_health_report_serde,
      serde::version<1>,
      serde::compat_version<0>> {
    model::node_id id;
    node::local_state local_state;
    chunked_vector<topic_status> topics;
    std::optional<cluster::drain_status> drain_status;
    node_liveness_report node_liveness_report;

    auto serde_fields() {
        return std::tie(
          id, local_state, topics, drain_status, node_liveness_report);
    }

    node_health_report_serde() = default;

    node_health_report_serde(
      model::node_id id,
      node::local_state local_state,
      chunked_vector<topic_status> topics,
      std::optional<cluster::drain_status> drain_status,
      struct node_liveness_report node_liveness_report)
      : id(id)
      , local_state(std::move(local_state))
      , topics(std::move(topics))
      , drain_status(drain_status)
      , node_liveness_report(std::move(node_liveness_report)) {}

    node_health_report_serde copy() const {
        return {
          id, local_state, topics.copy(), drain_status, node_liveness_report};
    }

    explicit node_health_report_serde(const node_health_report& hr);

    node_health_report to_in_memory() && {
        return node_health_report{
          id,
          std::move(local_state),
          std::move(topics),
          drain_status,
          std::move(node_liveness_report)};
    }

    fmt::iterator format_to(fmt::iterator it) const;

    friend bool operator==(
      const node_health_report_serde& a, const node_health_report_serde& b);
};

struct cluster_health_report
  : serde::envelope<
      cluster_health_report,
      serde::version<1>,
      serde::compat_version<0>> {
    std::optional<model::node_id> raft0_leader;
    // we split node status from node health reports since node status is a
    // cluster wide property (currently based on raft0 follower state)
    std::vector<node_state> node_states;

    // node reports are node specific information collected directly on a
    // node
    std::vector<node_health_report_ptr> node_reports;

    // cluster-wide cached information about total cloud storage usage
    std::optional<size_t> bytes_in_cloud_storage;
    fmt::iterator format_to(fmt::iterator it) const;

    friend bool operator==(
      const cluster_health_report&, const cluster_health_report&) = default;

    cluster_health_report copy() const;

    ss::future<> serde_async_write(iobuf& out) {
        using serde::write;
        using serde::write_async;
        // the current version decodes into the decoded version and is used in
        // request handling--that is, it is used at layer above serialization so
        // without further changes we'll need to preserve that behavior.
        write(out, raft0_leader);
        write(out, node_states);
        write(out, static_cast<serde::serde_size_t>(node_reports.size()));
        for (auto& nr : node_reports) {
            co_await write_async(out, node_health_report_serde{*nr});
        }
        write(out, bytes_in_cloud_storage);
    }

    ss::future<> serde_async_read(iobuf_parser& in, const serde::header& h) {
        using serde::read_async_nested;
        using serde::read_nested;
        raft0_leader = read_nested<std::optional<model::node_id>>(
          in, h._bytes_left_limit);
        node_states = read_nested<std::vector<node_state>>(
          in, h._bytes_left_limit);
        const auto sz = read_nested<serde::serde_size_t>(
          in, h._bytes_left_limit);
        node_reports.reserve(sz);
        for (auto i = 0U; i < sz; ++i) {
            auto r = co_await read_async_nested<node_health_report_serde>(
              in, h._bytes_left_limit);
            node_reports.emplace_back(
              ss::make_lw_shared<node_health_report>(
                std::move(r).to_in_memory()));
        }
        bytes_in_cloud_storage = read_nested<std::optional<size_t>>(
          in, h._bytes_left_limit);

        if (in.bytes_left() > h._bytes_left_limit) {
            in.skip(in.bytes_left() - h._bytes_left_limit);
        }
    }
    void serde_write(iobuf& out) {
        using serde::write;

        // the current version decodes into the decoded version and is used in
        // request handling--that is, it is used at layer above serialization so
        // without further changes we'll need to preserve that behavior.
        write(out, raft0_leader);
        write(out, node_states);
        write(out, static_cast<serde::serde_size_t>(node_reports.size()));
        for (auto& nr : node_reports) {
            write(out, node_health_report_serde{*nr});
        }
        write(out, bytes_in_cloud_storage);
    }

    void serde_read(iobuf_parser& in, const serde::header& h) {
        using serde::read_nested;
        raft0_leader = read_nested<std::optional<model::node_id>>(
          in, h._bytes_left_limit);
        node_states = read_nested<std::vector<node_state>>(
          in, h._bytes_left_limit);
        const auto sz = read_nested<serde::serde_size_t>(
          in, h._bytes_left_limit);
        node_reports.reserve(sz);
        for (auto i = 0U; i < sz; ++i) {
            auto r = read_nested<node_health_report_serde>(
              in, h._bytes_left_limit);
            node_reports.emplace_back(
              ss::make_lw_shared<node_health_report>(
                std::move(r).to_in_memory()));
        }
        bytes_in_cloud_storage = read_nested<std::optional<size_t>>(
          in, h._bytes_left_limit);

        if (in.bytes_left() > h._bytes_left_limit) {
            in.skip(in.bytes_left() - h._bytes_left_limit);
        }
    }
};

struct restart_risk_report {
    using partitions_t = chunked_vector<model::ntp>;
    partitions_t rf1_offline;
    partitions_t full_acks_produce_unavailable;
    partitions_t unavailable;
    partitions_t acks1_data_loss;

    size_t limit;

    void push(
      partitions_t restart_risk_report::* member,
      const model::topic_namespace&,
      model::partition_id);
};

struct cluster_health_overview {
    // is healthy is a main cluster indicator, it is intended as an simple flag
    // that will allow all external cluster orchestrating processes to decide if
    // they can proceed with next steps
    bool is_healthy() { return unhealthy_reasons.empty(); }

    // additional human readable information that will make debugging cluster
    // errors easier

    // Zero or more "unhealthy" reasons, which are terse human-readable strings
    // indicating one reason the cluster is unhealthy (there may be several).
    // is_healthy is true iff this list is empty (effectively, is_healthy is
    // redundnat but it's there for backwards compat and convenience).
    std::vector<ss::sstring> unhealthy_reasons;

    // The ID of the controller node, or nullopt if no controller is currently
    // elected.
    std::optional<model::node_id> controller_id;
    // All known nodes in the cluster, including nodes that have joined in the
    // past but are not curently up.
    std::vector<model::node_id> all_nodes;
    // A list of known nodes which are down from the point of view of the health
    // subsystem.
    std::vector<model::node_id> nodes_down;
    // A list of nodes that exceed disk usage alerts defined by
    // storage_space_alert_free_threshold_percent and
    // storage_space_alert_free_threshold_bytes
    std::vector<model::node_id> high_disk_usage_nodes;
    // A list of nodes that have been booted up in recovery mode.
    std::vector<model::node_id> nodes_in_recovery_mode;
    std::vector<model::ntp> leaderless_partitions;
    size_t leaderless_count{};
    std::vector<model::ntp> under_replicated_partitions;
    size_t under_replicated_count{};
    std::optional<size_t> bytes_in_cloud_storage;
    // True if the refresh attempted at assembly time errored. False on
    // success, or if no refresh was needed (cache fresh enough to skip).
    // Also surfaced as "no_health_report" in unhealthy_reasons.
    bool refresh_failed{false};
    // True if, at overview-assembly time, the local _reports cache holds a
    // health report for every known cluster member (and refresh_failed is
    // false). Signals that this overview reflects a fully-collected view
    // rather than a partial one. The underlying refresh may have happened
    // earlier - this is a property of the cache state, not of a single
    // refresh attempt.
    bool all_members_reported{false};

    fmt::iterator format_to(fmt::iterator it) const;
};

using include_partitions_info = ss::bool_class<struct include_partitions_tag>;

/**
 * Filters are used to limit amout of data returned in health reports
 */
struct partitions_filter
  : serde::
      envelope<partitions_filter, serde::version<0>, serde::compat_version<0>> {
    static constexpr int8_t current_version = 0;

    using partitions_set_t = absl::node_hash_set<model::partition_id>;
    using topic_map_t = absl::node_hash_map<model::topic, partitions_set_t>;
    using ns_map_t = absl::node_hash_map<model::ns, topic_map_t>;

    bool matches(const model::ntp& ntp) const;
    bool matches(model::topic_namespace_view, model::partition_id) const;

    ns_map_t namespaces;

    friend bool
    operator==(const partitions_filter&, const partitions_filter&) = default;

    fmt::iterator format_to(fmt::iterator it) const;

    auto serde_fields() { return std::tie(namespaces); }
};

struct node_report_filter
  : serde::envelope<
      node_report_filter,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr int8_t current_version = 0;

    include_partitions_info include_partitions = include_partitions_info::yes;

    partitions_filter ntp_filters;

    friend bool
    operator==(const node_report_filter&, const node_report_filter&) = default;

    fmt::iterator format_to(fmt::iterator it) const;

    auto serde_fields() { return std::tie(include_partitions, ntp_filters); }
};

struct cluster_report_filter
  : serde::envelope<
      cluster_report_filter,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr int8_t current_version = 0;
    // filtering that will be applied to node reports
    node_report_filter node_report_filter;
    // list of requested nodes, if empty report will contain all nodes
    std::vector<model::node_id> nodes;

    fmt::iterator format_to(fmt::iterator it) const;

    friend bool operator==(
      const cluster_report_filter&, const cluster_report_filter&) = default;

    auto serde_fields() { return std::tie(node_report_filter, nodes); }
};

using force_refresh = ss::bool_class<struct hm_force_refresh_tag>;

/**
 * RPC requests
 */

class get_node_health_request
  : public serde::envelope<
      get_node_health_request,
      serde::version<1>,
      serde::compat_version<0>> {
public:
    get_node_health_request() = default;
    explicit get_node_health_request(model::node_id target_node_id)
      : _target_node_id(target_node_id) {}

    friend bool operator==(
      const get_node_health_request&, const get_node_health_request&) = default;

    fmt::iterator format_to(fmt::iterator it) const;

    auto serde_fields() { return std::tie(_filter, _target_node_id); }
    static constexpr model::node_id node_id_not_set{-1};

    model::node_id get_target_node_id() const { return _target_node_id; }

private:
    // default value for backward compatibility
    model::node_id _target_node_id = node_id_not_set;
    /**
     * This field is no longer used, as it never was. It was made private on
     * purpose
     */
    node_report_filter _filter;
};

struct get_node_health_reply
  : serde::envelope<
      get_node_health_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    errc error = cluster::errc::success;
    std::optional<node_health_report_serde> report;

    friend bool operator==(
      const get_node_health_reply&, const get_node_health_reply&) = default;

    get_node_health_reply copy() const {
        return {
          .error = error,
          .report = report ? std::optional{report->copy()} : std::nullopt,
        };
    }

    fmt::iterator format_to(fmt::iterator it) const;

    auto serde_fields() { return std::tie(error, report); }
};

struct get_cluster_health_request
  : serde::envelope<
      get_cluster_health_request,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr int8_t initial_version = 0;
    // version -1: included revision id in partition status
    static constexpr int8_t revision_id_version = -1;
    // version -2: included size_bytes in partition status
    static constexpr int8_t size_bytes_version = -2;

    static constexpr int8_t current_version = size_bytes_version;

    cluster_report_filter filter;
    // if set to true will force node health metadata refresh
    force_refresh refresh = force_refresh::no;
    // this field is not serialized
    int8_t decoded_version = current_version;

    friend bool operator==(
      const get_cluster_health_request&,
      const get_cluster_health_request&) = default;

    fmt::iterator format_to(fmt::iterator it) const;

    void serde_write(iobuf& out) {
        using serde::write;
        // the current version decodes into the decoded version and is used in
        // request handling--that is, it is used at layer above serialization so
        // without further changes we'll need to preserve that behavior.
        write(out, current_version);
        write(out, filter);
        write(out, refresh);
    }

    void serde_read(iobuf_parser& in, const serde::header& h) {
        using serde::read_nested;
        decoded_version = read_nested<int8_t>(in, h._bytes_left_limit);
        filter = read_nested<cluster_report_filter>(in, h._bytes_left_limit);
        refresh = read_nested<force_refresh>(in, h._bytes_left_limit);
    }
};

struct get_cluster_health_reply
  : serde::envelope<
      get_cluster_health_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr int8_t current_version = 0;

    errc error = cluster::errc::success;
    std::optional<cluster_health_report> report;

    friend bool
    operator==(const get_cluster_health_reply&, const get_cluster_health_reply&)
      = default;

    fmt::iterator format_to(fmt::iterator it) const;

    get_cluster_health_reply copy() const;

    auto serde_fields() { return std::tie(error, report); }
};

partition_statuses_map_t
copy_partition_statuses(const partition_statuses_map_t& ps);
partition_statuses_t copy_to_vector(const partition_statuses_map_t&);
partition_statuses_t move_to_vector(partition_statuses_map_t&&);
partition_statuses_map_t move_to_map(partition_statuses_t&&);
partition_statuses_map_t copy_to_map(const partition_statuses_t&);

} // namespace cluster

/*
 * Dissemination types
 *
 * These types support the demand-driven pull protocol with delta encoding.
 * Partition data is split into two tiers:
 *   - metadata (term, leader, followers, etc.): diffed, changes rarely
 *   - data (sizes, watermarks): always sent in full, changes continuously
 */

namespace cluster::health {

/// Health version tag carried by every report
struct node_health_version
  : serde::envelope<
      node_health_version,
      serde::version<0>,
      serde::compat_version<0>> {
    model::node_boot_id boot{}; // changes across restarts
    int64_t counter{0};         // advances within a single boot

    node_health_version() = default;
    node_health_version(model::node_boot_id b, int64_t c)
      : boot(b)
      , counter(c) {}
    explicit node_health_version(int64_t c)
      : counter(c) {}

    auto operator<=>(const node_health_version&) const = default;
    bool operator==(const node_health_version&) const = default;

    auto serde_fields() { return std::tie(boot, counter); }

    fmt::iterator format_to(fmt::iterator it) const;
};

/// Per-partition metadata fields: included in diffs only when changed.
struct partition_metadata
  : serde::envelope<
      partition_metadata,
      serde::version<0>,
      serde::compat_version<0>> {
    model::term_id term;
    std::optional<model::node_id> leader_id;
    model::revision_id revision_id;
    std::optional<uint8_t> under_replicated_replicas;
    std::optional<followers_stats> followers_stats;
    uint32_t shard = partition_status::invalid_shard_id;

    auto serde_fields() {
        return std::tie(
          term,
          leader_id,
          revision_id,
          under_replicated_replicas,
          followers_stats,
          shard);
    }

    friend bool
    operator==(const partition_metadata&, const partition_metadata&) = default;
};

/// Per-partition data fields: always included for all partitions.
struct partition_data
  : serde::
      envelope<partition_data, serde::version<0>, serde::compat_version<0>> {
    size_t size_bytes = partition_status::invalid_size_bytes;
    kafka::offset high_watermark;
    std::optional<kafka::offset> log_start_offset;
    std::optional<size_t> reclaimable_size_bytes;
    std::optional<int64_t> cloud_topic_max_gc_eligible_epoch;

    auto serde_fields() {
        return std::tie(
          size_bytes,
          high_watermark,
          log_start_offset,
          reclaimable_size_bytes,
          cloud_topic_max_gc_eligible_epoch);
    }

    friend bool
    operator==(const partition_data&, const partition_data&) = default;
};

/// Two-level map: topic_namespace → partition_id → T.
template<typename T>
using topic_partition_map = chunked_hash_map<
  model::topic_namespace,
  chunked_hash_map<model::partition_id, T>,
  model::topic_namespace_hash,
  model::topic_namespace_eq>;

using topic_partition_metadata_map = topic_partition_map<partition_metadata>;
using topic_partition_data_map = topic_partition_map<partition_data>;

/// Metadata diff: nullopt = partition removed, value = changed/added,
/// absent key = no change.

/// Stored as vec-of-pairs of vec-of-pairs for fast iteration & serialization:
using partition_metadata_diff_list = chunked_vector<
  std::pair<model::partition_id, std::optional<partition_metadata>>>;
using topic_partition_metadata_diff = chunked_vector<
  std::pair<model::topic_namespace, partition_metadata_diff_list>>;
// Same as a map
using topic_partition_metadata_diff_map
  = topic_partition_map<std::optional<partition_metadata>>;
// Same as a map on topic level, vector on partition level
using topic_partition_metadata_diff_semimap = chunked_hash_map<
  model::topic_namespace,
  partition_metadata_diff_list,
  model::topic_namespace_hash,
  model::topic_namespace_eq>;

topic_partition_metadata_diff_map as_map(const topic_partition_metadata_diff&);

/// A timestamp that serializes as age (duration since now) and
/// deserializes back to a time_point on the receiver using its local clock.
/// Poor man's clock sync: the receiver reconstructs an approximate
/// absolute time from the relative age on the wire. Can drift by the latency of
/// the RPC round trip, but that's acceptable for our use case since these are
/// used for staleness checks and not precise ordering.
struct approx_timestamp
  : serde::
      envelope<approx_timestamp, serde::version<0>, serde::compat_version<0>> {
    using clock_t = model::timeout_clock;
    using underlying_t = clock_t::time_point;
    underlying_t value;

    constexpr approx_timestamp() = default;
    // implicit
    constexpr approx_timestamp(underlying_t tp)
      : value(tp) {}
    // implicit
    constexpr operator underlying_t() const { return value; }

    void serde_write(iobuf& out) const;
    void serde_read(iobuf_parser& in, const serde::header& h);

    static constexpr approx_timestamp min() { return underlying_t::min(); }

    friend auto
    operator<=>(const approx_timestamp&, const approx_timestamp&) = default;
    friend bool
    operator==(const approx_timestamp&, const approx_timestamp&) = default;
};

/// Always-full data, replaced wholesale during diff composition/application.
/// Combines node-level state with per-partition data fields.
struct health_snapshot
  : serde::
      envelope<health_snapshot, serde::version<0>, serde::compat_version<0>> {
    approx_timestamp src_timestamp;
    node::local_state local_state;
    std::optional<cluster::drain_status> drain_status;
    node_liveness_report liveness;
    topic_partition_data_map data;

    auto serde_fields() {
        return std::tie(
          src_timestamp, local_state, drain_status, liveness, data);
    }

    // ignores src_timestamp as it's not reliably comparable across nodes
    friend bool operator==(const health_snapshot& a, const health_snapshot& b);
    fmt::iterator format_to(fmt::iterator it) const;
};

using health_snapshot_ptr = ss::lw_shared_ptr<const health_snapshot>;
using metadata_diff_ptr = ss::lw_shared_ptr<topic_partition_metadata_diff>;
using metadata_map_ptr = ss::lw_shared_ptr<topic_partition_metadata_map>;

/// Consumer-facing health data for a single node. No version awareness.
/// Replaces node_health_report for consumers.
struct node_health {
    health_snapshot_ptr snapshot;
    metadata_map_ptr metadata;

    fmt::iterator format_to(fmt::iterator it) const;
};

/// A diff entry. Contains always-full snapshot (replaced wholesale) and
/// metadata changes (only changed partitions).
/// In metadata_diff: nullopt value = partition removed, present value =
/// changed/added, absent key = no change.
struct diff_entry_serde;
struct versioned_report_serde;
struct diff_entry {
    diff_entry()
      : metadata_diff(ss::make_lw_shared<topic_partition_metadata_diff>()) {}

    /// Compute a diff from old_report to new_report.
    diff_entry(const node_health& old_report, const node_health& new_report);

    /// Construct from serde type (receiver side).
    explicit diff_entry(diff_entry_serde&& s);

    /// Compose `later` into `this`: A..B + B..C → A..C (in-place).
    /// `later` is not consumed.
    void compose(const diff_entry& later);

    /// Apply this diff to `target` in-place.
    void apply_to(node_health& target) const;

    node_health_version start;
    node_health_version end;
    health_snapshot_ptr snapshot;
    metadata_diff_ptr metadata_diff;

    fmt::iterator format_to(fmt::iterator it) const;
};

/// A health report bundled with its version, for sending to peers.
struct versioned_report {
    node_health health;
    node_health_version version;

    versioned_report() = default;
    versioned_report(node_health h, node_health_version v)
      : health(std::move(h))
      , version(v) {}
    explicit versioned_report(versioned_report_serde&& s);
};

// holds either T by value (receiver side) or
// foreign_ptr<lw_shared_ptr<const T>> (sender side, zero-copy ref).
// On the wire they're identical.
template<typename T>
class value_or_foreign {
    using lw_shared_t = ss::lw_shared_ptr<const T>;
    using foreign_t = ss::foreign_ptr<lw_shared_t>;
    std::variant<T, foreign_t> _data;

public:
    value_or_foreign()
    requires std::is_default_constructible_v<T>
      : _data(T{}) {}
    explicit value_or_foreign(T val)
      : _data(std::move(val)) {}
    explicit value_or_foreign(lw_shared_t ptr)
      : _data(ss::make_foreign(std::move(ptr))) {}

    const T& operator*() const& {
        return ss::visit(
          _data,
          [](const T& v) -> const T& { return v; },
          [](const foreign_t& fp) -> const T& { return *fp; });
    }

    T&& operator*() && {
        vassert(
          std::holds_alternative<T>(_data),
          "rvalue operator* called on foreign pointer branch");
        return std::move(std::get<T>(_data));
    }
};

template<typename T>
bool operator==(const value_or_foreign<T>& a, const value_or_foreign<T>& b) {
    return *a == *b;
}

// Serde types for RPC wire format
struct diff_entry_serde
  : serde::
      envelope<diff_entry_serde, serde::version<0>, serde::compat_version<0>> {
    node_health_version start;
    node_health_version end;
    value_or_foreign<health_snapshot> snapshot;
    value_or_foreign<topic_partition_metadata_diff> metadata_diff;

    diff_entry_serde() = default;
    explicit diff_entry_serde(const diff_entry& d);

    ss::future<> serde_async_write(iobuf& out);
    ss::future<> serde_async_read(iobuf_parser& in, const serde::header h);

    friend bool
    operator==(const diff_entry_serde&, const diff_entry_serde&) = default;
};

struct versioned_report_serde
  : serde::envelope<
      versioned_report_serde,
      serde::version<0>,
      serde::compat_version<0>> {
    value_or_foreign<health_snapshot> snapshot;
    value_or_foreign<topic_partition_metadata_map> metadata;
    node_health_version version;

    versioned_report_serde() = default;
    explicit versioned_report_serde(const versioned_report& r);

    ss::future<> serde_async_write(iobuf& out);
    ss::future<> serde_async_read(iobuf_parser& in, const serde::header h);

    friend bool operator==(
      const versioned_report_serde&, const versioned_report_serde&) = default;
};

using health_update_serde
  = serde::variant<diff_entry_serde, versioned_report_serde>;

} // namespace cluster::health

namespace cluster {

/// RPC request: pull health data from a peer.
/// Carries a version vector (what the requester already has for each node)
/// and a freshness threshold.
struct health_pull_request
  : serde::envelope<
      health_pull_request,
      serde::version<0>,
      serde::compat_version<0>> {
    /// Double check the requested node is the one we intended to reach out to.
    model::node_id target_node_id;
    /// For each node the requester knows about: the version it already has.
    chunked_hash_map<model::node_id, health::node_health_version>
      existing_versions;
    /// Minimum acceptable source timestamp for health data.
    health::approx_timestamp min_src_timestamp;

    auto serde_fields() {
        return std::tie(target_node_id, existing_versions, min_src_timestamp);
    }
};

/// RPC reply: health data updates for requested nodes.
/// Custom async serde: write dereferences foreign_ptrs, read creates them.
struct health_pull_reply
  : serde::
      envelope<health_pull_reply, serde::version<0>, serde::compat_version<0>> {
    errc error = errc::success;
    chunked_vector<std::pair<model::node_id, health::health_update_serde>>
      items;

    friend bool
    operator==(const health_pull_reply& a, const health_pull_reply& b) {
        return a.error == b.error && std::ranges::equal(a.items, b.items);
    }

    ss::future<> serde_async_write(iobuf& out);
    ss::future<> serde_async_read(iobuf_parser& in, const serde::header h);
};

} // namespace cluster
