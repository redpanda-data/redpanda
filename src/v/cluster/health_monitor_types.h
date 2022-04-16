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
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "reflection/adl.h"
#include "utils/named_type.h"

#include <absl/container/node_hash_map.h>
#include <absl/container/node_hash_set.h>

#include <chrono>

namespace cluster {

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
struct node_state {
    static constexpr int8_t current_version = 0;

    model::node_id id;
    model::membership_state membership_state;
    alive is_alive;
    friend std::ostream& operator<<(std::ostream&, const node_state&);
};

struct partition_status {
    /**
     * We increase a version here 'backward' since incorrect assertion would
     * cause older redpanda versions to crash.
     *
     * Version: -1: added revision_id field
     */
    static constexpr int8_t current_version = -1;

    model::partition_id id;
    model::term_id term;
    std::optional<model::node_id> leader_id;
    model::revision_id revision_id;

    friend std::ostream& operator<<(std::ostream&, const partition_status&);
    friend bool operator==(const partition_status&, const partition_status&)
      = default;
};

struct topic_status {
    static constexpr int8_t current_version = 0;

    model::topic_namespace tp_ns;
    std::vector<partition_status> partitions;
    friend std::ostream& operator<<(std::ostream&, const topic_status&);
    friend bool operator==(const topic_status&, const topic_status&) = default;
};

/**
 * Node health report is collected built based on node local state at given
 * instance of time
 */
struct node_health_report {
    static constexpr int8_t current_version = 2;

    model::node_id id;
    node::local_state local_state;
    std::vector<topic_status> topics;
    std::optional<drain_manager::drain_status> drain_status;

    friend std::ostream& operator<<(std::ostream&, const node_health_report&);
};

struct cluster_health_report {
    static constexpr int8_t current_version = 0;

    std::optional<model::node_id> raft0_leader;
    // we split node status from node health reports since node status is a
    // cluster wide property (currently based on raft0 follower state)
    std::vector<node_state> node_states;

    // node reports are node specific information collected directly on a
    // node
    std::vector<node_health_report> node_reports;
    friend std::ostream&
    operator<<(std::ostream&, const cluster_health_report&);
};

using include_partitions_info = ss::bool_class<struct include_partitions_tag>;

/**
 * Filters are used to limit amout of data returned in health reports
 */
struct partitions_filter {
    static constexpr int8_t current_version = 0;

    using partitions_set_t = absl::node_hash_set<model::partition_id>;
    using topic_map_t = absl::node_hash_map<model::topic, partitions_set_t>;
    using ns_map_t = absl::node_hash_map<model::ns, topic_map_t>;

    bool matches(const model::ntp& ntp) const;
    bool matches(model::topic_namespace_view, model::partition_id) const;

    ns_map_t namespaces;
};

struct node_report_filter {
    static constexpr int8_t current_version = 0;

    include_partitions_info include_partitions = include_partitions_info::yes;

    partitions_filter ntp_filters;

    friend std::ostream& operator<<(std::ostream&, const node_report_filter&);
};

struct cluster_report_filter {
    static constexpr int8_t current_version = 0;
    // filtering that will be applied to node reports
    node_report_filter node_report_filter;
    // list of requested nodes, if empty report will contain all nodes
    std::vector<model::node_id> nodes;

    friend std::ostream&
    operator<<(std::ostream&, const cluster_report_filter&);
};

using force_refresh = ss::bool_class<struct hm_force_refresh_tag>;

/**
 * RPC requests
 */

struct get_node_health_request {
    // version -1: included revision id in partition status
    static constexpr int8_t current_version = -1;

    node_report_filter filter;
    // this field is not serialized
    int8_t decoded_version = current_version;
};

struct get_node_health_reply {
    static constexpr int8_t current_version = 0;

    errc error = cluster::errc::success;
    std::optional<node_health_report> report;
};

struct get_cluster_health_request {
    // version -1: included revision id in partition status
    static constexpr int8_t current_version = -1;
    cluster_report_filter filter;
    // if set to true will force node health metadata refresh
    force_refresh refresh = force_refresh::no;
    // this field is not serialized
    int8_t decoded_version = current_version;
};

struct get_cluster_health_reply {
    static constexpr int8_t current_version = 0;

    errc error = cluster::errc::success;
    std::optional<cluster_health_report> report;
};

} // namespace cluster

namespace reflection {

template<>
struct adl<cluster::node_health_report> {
    void to(iobuf&, cluster::node_health_report&&);
    cluster::node_health_report from(iobuf_parser&);
};

template<>
struct adl<cluster::cluster_health_report> {
    void to(iobuf&, cluster::cluster_health_report&&);
    cluster::cluster_health_report from(iobuf_parser&);
};

template<>
struct adl<cluster::node_state> {
    void to(iobuf&, cluster::node_state&&);
    cluster::node_state from(iobuf_parser&);
};

template<>
struct adl<cluster::partition_status> {
    void to(iobuf&, cluster::partition_status&&);
    cluster::partition_status from(iobuf_parser&);
};

template<>
struct adl<cluster::topic_status> {
    void to(iobuf&, cluster::topic_status&&);
    cluster::topic_status from(iobuf_parser&);
};

template<>
struct adl<cluster::partitions_filter> {
    struct raw_tp_filter {
        model::topic topic;
        std::vector<model::partition_id> partitions;
    };

    struct raw_ns_filter {
        model::ns ns;
        std::vector<raw_tp_filter> topics;
    };

    void to(iobuf&, cluster::partitions_filter&&);
    cluster::partitions_filter from(iobuf_parser&);
};

template<>
struct adl<cluster::node_report_filter> {
    void to(iobuf&, cluster::node_report_filter&&);
    cluster::node_report_filter from(iobuf_parser&);
};

template<>
struct adl<cluster::cluster_report_filter> {
    void to(iobuf&, cluster::cluster_report_filter&&);
    cluster::cluster_report_filter from(iobuf_parser&);
};

template<>
struct adl<cluster::get_node_health_request> {
    void to(iobuf&, cluster::get_node_health_request&&);
    cluster::get_node_health_request from(iobuf_parser&);
};

template<>
struct adl<cluster::get_node_health_reply> {
    void to(iobuf&, cluster::get_node_health_reply&&);
    cluster::get_node_health_reply from(iobuf_parser&);
};

template<>
struct adl<cluster::get_cluster_health_request> {
    void to(iobuf&, cluster::get_cluster_health_request&&);
    cluster::get_cluster_health_request from(iobuf_parser&);
};

template<>
struct adl<cluster::get_cluster_health_reply> {
    void to(iobuf&, cluster::get_cluster_health_reply&&);
    cluster::get_cluster_health_reply from(iobuf_parser&);
};

} // namespace reflection
