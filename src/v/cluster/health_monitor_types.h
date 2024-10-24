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
#include "bytes/iobuf_parser.h"
#include "cluster/drain_manager.h"
#include "cluster/errc.h"
#include "cluster/node/types.h"
#include "container/chunked_hash_map.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "serde/async.h"
#include "serde/rw/bool_class.h"
#include "serde/rw/envelope.h"
#include "serde/rw/optional.h"
#include "serde/rw/rw.h"
#include "serde/rw/scalar.h"
#include "serde/rw/vector.h"
#include "utils/named_type.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/util/bool_class.hh>

#include <absl/container/node_hash_map.h>
#include <absl/container/node_hash_set.h>

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
    friend std::ostream& operator<<(std::ostream&, const node_state&);

    friend bool operator==(const node_state&, const node_state&) = default;

    auto serde_fields() { return std::tie(_id, _membership_state, _is_alive); }

private:
    model::node_id _id;
    model::membership_state _membership_state;
    alive _is_alive;
};

struct partition_status
  : serde::
      envelope<partition_status, serde::version<3>, serde::compat_version<0>> {
    static constexpr size_t invalid_size_bytes = size_t(-1);
    static constexpr uint32_t invalid_shard_id = uint32_t(-1);

    model::partition_id id;
    model::term_id term;
    std::optional<model::node_id> leader_id;
    model::revision_id revision_id;
    size_t size_bytes;
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

    auto serde_fields() {
        return std::tie(
          id,
          term,
          leader_id,
          revision_id,
          size_bytes,
          under_replicated_replicas,
          reclaimable_size_bytes,
          shard);
    }

    friend std::ostream& operator<<(std::ostream&, const partition_status&);
    friend bool operator==(const partition_status&, const partition_status&)
      = default;
};

using partition_statuses_t = chunked_vector<partition_status>;

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
    friend std::ostream& operator<<(std::ostream&, const topic_status&);
    friend bool operator==(const topic_status&, const topic_status&);

    auto serde_fields() { return std::tie(tp_ns, partitions); }
};

/**
 * Node health report is collected built based on node local state at given
 * instance of time
 */
struct node_health_report {
    using topics_t = chunked_hash_map<
      model::topic_namespace,
      partition_statuses_t,
      model::topic_namespace_hash,
      model::topic_namespace_eq>;

    model::node_id id;
    node::local_state local_state;
    topics_t topics;
    std::optional<drain_manager::drain_status> drain_status;

    node_health_report(
      model::node_id,
      node::local_state,
      chunked_vector<topic_status>,
      std::optional<drain_manager::drain_status>);

    node_health_report copy() const;

    friend std::ostream& operator<<(std::ostream&, const node_health_report&);
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
      serde::version<0>,
      serde::compat_version<0>> {
    model::node_id id;
    node::local_state local_state;
    chunked_vector<topic_status> topics;
    std::optional<drain_manager::drain_status> drain_status;

    auto serde_fields() {
        return std::tie(id, local_state, topics, drain_status);
    }

    node_health_report_serde() = default;

    node_health_report_serde(
      model::node_id id,
      node::local_state local_state,
      chunked_vector<topic_status> topics,
      std::optional<drain_manager::drain_status> drain_status)
      : id(id)
      , local_state(std::move(local_state))
      , topics(std::move(topics))
      , drain_status(drain_status) {}

    node_health_report_serde copy() const {
        return {id, local_state, topics.copy(), drain_status};
    }

    explicit node_health_report_serde(const node_health_report& hr);

    node_health_report to_in_memory() && {
        return node_health_report{
          id,
          std::move(local_state),
          std::move(topics),
          std::move(drain_status)};
    }

    friend std::ostream&
    operator<<(std::ostream&, const node_health_report_serde&);

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
    friend std::ostream&
    operator<<(std::ostream&, const cluster_health_report&);

    friend bool
    operator==(const cluster_health_report&, const cluster_health_report&)
      = default;

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
            node_reports.emplace_back(ss::make_lw_shared<node_health_report>(
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
            node_reports.emplace_back(ss::make_lw_shared<node_health_report>(
              std::move(r).to_in_memory()));
        }
        bytes_in_cloud_storage = read_nested<std::optional<size_t>>(
          in, h._bytes_left_limit);

        if (in.bytes_left() > h._bytes_left_limit) {
            in.skip(in.bytes_left() - h._bytes_left_limit);
        }
    }
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
    // A list of nodes that have been booted up in recovery mode.
    std::vector<model::node_id> nodes_in_recovery_mode;
    std::vector<model::ntp> leaderless_partitions;
    size_t leaderless_count{};
    std::vector<model::ntp> under_replicated_partitions;
    size_t under_replicated_count{};
    std::optional<size_t> bytes_in_cloud_storage;

    friend std::ostream&
    operator<<(std::ostream&, const cluster_health_overview&);
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

    friend bool operator==(const partitions_filter&, const partitions_filter&)
      = default;

    friend std::ostream& operator<<(std::ostream& o, const partitions_filter&);

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

    friend bool operator==(const node_report_filter&, const node_report_filter&)
      = default;

    friend std::ostream& operator<<(std::ostream&, const node_report_filter&);

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

    friend std::ostream&
    operator<<(std::ostream&, const cluster_report_filter&);

    friend bool
    operator==(const cluster_report_filter&, const cluster_report_filter&)
      = default;

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
    using rpc_adl_exempt = std::true_type;
    get_node_health_request() = default;
    explicit get_node_health_request(model::node_id target_node_id)
      : _target_node_id(target_node_id) {}

    friend bool
    operator==(const get_node_health_request&, const get_node_health_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const get_node_health_request&);

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
    using rpc_adl_exempt = std::true_type;

    errc error = cluster::errc::success;
    std::optional<node_health_report_serde> report;

    friend bool
    operator==(const get_node_health_reply&, const get_node_health_reply&)
      = default;

    get_node_health_reply copy() const {
        return {
          .error = error,
          .report = report ? std::optional{report->copy()} : std::nullopt,
        };
    }

    friend std::ostream&
    operator<<(std::ostream&, const get_node_health_reply&);

    auto serde_fields() { return std::tie(error, report); }
};

struct get_cluster_health_request
  : serde::envelope<
      get_cluster_health_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
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
      const get_cluster_health_request&, const get_cluster_health_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const get_cluster_health_request&);

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
    using rpc_adl_exempt = std::true_type;
    static constexpr int8_t current_version = 0;

    errc error = cluster::errc::success;
    std::optional<cluster_health_report> report;

    friend bool
    operator==(const get_cluster_health_reply&, const get_cluster_health_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const get_cluster_health_reply&);

    get_cluster_health_reply copy() const;

    auto serde_fields() { return std::tie(error, report); }
};

} // namespace cluster
