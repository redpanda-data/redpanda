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
#pragma once
#include "bytes/iobuf_parser.h"
#include "cluster/errc.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "reflection/adl.h"
#include "utils/named_type.h"

#include <absl/container/node_hash_map.h>
#include <absl/container/node_hash_set.h>

namespace cluster {

/**
 * Health reports
 */

using alive = ss::bool_class<struct node_alive_tag>;
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

struct node_disk_space {
    static constexpr int8_t current_version = 0;

    ss::sstring path;
    uint64_t free;
    uint64_t total;
    friend std::ostream& operator<<(std::ostream&, const node_disk_space&);
    friend bool operator==(const node_disk_space&, const node_disk_space&)
      = default;
};

struct partition_status {
    static constexpr int8_t current_version = 0;

    model::partition_id id;
    model::term_id term;
    std::optional<model::node_id> leader_id;
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
    static constexpr int8_t current_version = 0;

    model::node_id id;
    application_version redpanda_version;
    // we store a vector to be ready to operate with multiple data
    // directories
    std::vector<node_disk_space> disk_space;
    std::vector<topic_status> topics;

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
struct adl<cluster::node_disk_space> {
    void to(iobuf&, cluster::node_disk_space&&);
    cluster::node_disk_space from(iobuf_parser&);
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
} // namespace reflection
