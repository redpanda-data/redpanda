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

#include "model/fundamental.h"
#include "net/unresolved_address.h"
#include "seastarx.h"
#include "serde/envelope.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <absl/hash/hash.h>
#include <bits/stdint-intn.h>
#include <boost/functional/hash.hpp>

#include <compare>
#include <optional>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace model {
using node_id = named_type<int32_t, struct node_id_model_type>;

/**
 * Reserved to represent the node_id value yet to be assigned
 * when node is configured for automatic assignment of node_ids.
 * Never used in node configuration.
 */
constexpr node_id unassigned_node_id(-1);

/**
 * We use revision_id to identify entities evolution in time. f.e. NTP that was
 * first created and then removed, raft configuration
 */
using revision_id = named_type<int64_t, struct revision_id_model_type>;

/**
 * Revision id that the partition had when the topic was just created.
 * The revision_id of the partition might change when the partition is moved
 * between the nodes.
 */
using initial_revision_id
  = named_type<int64_t, struct initial_revision_id_model_type>;

/// Rack id type
using rack_id = named_type<ss::sstring, struct rack_id_model_type>;
struct broker_properties
  : serde::
      envelope<broker_properties, serde::version<0>, serde::compat_version<0>> {
    uint32_t cores;
    uint32_t available_memory_gb;
    uint32_t available_disk_gb;
    std::vector<ss::sstring> mount_paths;
    // key=value properties in /etc/redpanda/machine_properties.yaml
    std::unordered_map<ss::sstring, ss::sstring> etc_props;

    bool operator==(const broker_properties& other) const {
        return cores == other.cores
               && available_memory_gb == other.available_memory_gb
               && available_disk_gb == other.available_disk_gb
               && mount_paths == other.mount_paths
               && etc_props == other.etc_props;
    }

    friend std::ostream&
    operator<<(std::ostream&, const model::broker_properties&);

    auto serde_fields() {
        return std::tie(
          cores,
          available_memory_gb,
          available_disk_gb,
          mount_paths,
          etc_props);
    }
};

struct broker_endpoint final
  : serde::
      envelope<broker_endpoint, serde::version<0>, serde::compat_version<0>> {
    ss::sstring name;
    net::unresolved_address address;

    broker_endpoint() = default;

    broker_endpoint(ss::sstring name, net::unresolved_address address) noexcept
      : name(std::move(name))
      , address(std::move(address)) {}

    explicit broker_endpoint(net::unresolved_address address) noexcept
      : address(std::move(address)) {}

    bool operator==(const broker_endpoint&) const = default;
    friend std::ostream& operator<<(std::ostream&, const broker_endpoint&);

    auto serde_fields() { return std::tie(name, address); }
};

/**
 * Node membership can be in one of three states: active, draining and removed.
 *
 *                                              no partitions
 *                      decommission          left on current node
 *                ┌──────────────────────┐          │
 *                │                      │          │
 *           ┌────┴─────┐          ┌─────▼──────┐   ▼    ┌───────────┐
 *    join   │          │          │            │  done  │           │
 *    ------>│  active  │          │  draining  │------->│  removed  │
 *           │          │          │            │        │           │
 *           └────▲─────┘          └─────┬──────┘        └───────────┘
 *                │                      │
 *                └──────────────────────┘
 *                     recommission
 *
 * Membership states have following implications:
 *
 * active - when node joins the cluster it is automatically assigned to active
 *          state, active is a default cluster member state
 *
 * draining - in this state redpanda started to drain all existing partition
 *            replicas from given node, node is no longer accepting new
 *            partitions, but can still handle Kafka requests
 *
 * removed - after node is drained and it has no replicas assigned it is finally
 *           marked as removed, at the same time the node is no longer cluster
 *           member and can be shut down.
 *
 * TODO: keep removed nodes in members_state. Currently we keep brokers in raft0
 * configuration and it is our source of information about cluster members,
 * basing on configuration updates we build `cluster::members_table`. It would
 * be ideal to migrate brokers out of the raft configuration and manage them in
 * cluster layer, this way Raft can operate solely on node ids.
 */
enum class membership_state : int8_t { active, draining, removed };

/*
 * Broker maintenance mode
 */
enum class maintenance_state { active, inactive };

std::ostream& operator<<(std::ostream&, membership_state);
std::ostream& operator<<(std::ostream&, maintenance_state);

class broker
  : public serde::
      envelope<broker, serde::version<0>, serde::compat_version<0>> {
public:
    broker() noexcept = default;

    broker(
      node_id id,
      std::vector<broker_endpoint> kafka_advertised_listeners,
      net::unresolved_address rpc_address,
      std::optional<rack_id> rack,
      broker_properties props) noexcept
      : _id(id)
      , _kafka_advertised_listeners(std::move(kafka_advertised_listeners))
      , _rpc_address(std::move(rpc_address))
      , _rack(std::move(rack))
      , _properties(std::move(props)) {}

    broker(
      node_id id,
      net::unresolved_address kafka_advertised_listener,
      net::unresolved_address rpc_address,
      std::optional<rack_id> rack,
      broker_properties props) noexcept
      : broker(
        id,
        {broker_endpoint(std::move(kafka_advertised_listener))},
        std::move(rpc_address),
        std::move(rack),
        std::move(props)) {}

    broker(broker&&) noexcept = default;
    broker& operator=(broker&&) noexcept = default;
    broker& operator=(const broker&) noexcept = default;
    broker(const broker&) = default;
    const node_id& id() const { return _id; }

    const broker_properties& properties() const { return _properties; }
    const std::vector<broker_endpoint>& kafka_advertised_listeners() const {
        return _kafka_advertised_listeners;
    }
    const net::unresolved_address& rpc_address() const { return _rpc_address; }
    const std::optional<rack_id>& rack() const { return _rack; }

    void replace_unassigned_node_id(const node_id id) {
        vassert(
          _id == unassigned_node_id,
          "Cannot replace an assigned node_id in model::broker");
        _id = id;
    }

    bool operator==(const model::broker& other) const = default;
    bool operator<(const model::broker& other) const { return _id < other._id; }

    auto serde_fields() {
        return std::tie(
          _id, _kafka_advertised_listeners, _rpc_address, _rack, _properties);
    }

private:
    node_id _id;
    std::vector<broker_endpoint> _kafka_advertised_listeners;
    net::unresolved_address _rpc_address;
    std::optional<rack_id> _rack;
    broker_properties _properties;

    friend std::ostream& operator<<(std::ostream&, const broker&);
};

/// type representing single replica assignment it contains the id of a broker
/// and id of this broker shard.
struct broker_shard {
    model::node_id node_id;
    /// this is the same as a ss::shard_id
    /// however, seastar uses unsized-ints (unsigned)
    /// and for predictability we need fixed-sized ints
    uint32_t shard;
    friend std::ostream& operator<<(std::ostream&, const broker_shard&);
    bool operator==(const broker_shard&) const = default;
    auto operator<=>(const model::broker_shard&) const = default;

    template<typename H>
    friend H AbslHashValue(H h, const broker_shard& s) {
        return H::combine(std::move(h), s.node_id(), s.shard);
    }

    friend void write(iobuf& out, broker_shard bs) {
        using serde::write;
        write(out, bs.node_id);
        write(out, bs.shard);
    }

    friend void read_nested(
      iobuf_parser& in, broker_shard& bs, std::size_t const bytes_left_limit) {
        using serde::read_nested;
        read_nested(in, bs.node_id, bytes_left_limit);
        read_nested(in, bs.shard, bytes_left_limit);
    }
};

struct partition_metadata
  : serde::envelope<
      partition_metadata,
      serde::version<0>,
      serde::compat_version<0>> {
    partition_metadata() noexcept = default;
    explicit partition_metadata(partition_id p) noexcept
      : id(p) {}
    partition_id id;
    std::vector<broker_shard> replicas;
    std::optional<model::node_id> leader_node;

    friend std::ostream& operator<<(std::ostream&, const partition_metadata&);
    friend bool operator==(const partition_metadata&, const partition_metadata&)
      = default;

    auto serde_fields() { return std::tie(id, replicas, leader_node); }
};

enum class isolation_level : int8_t {
    read_uncommitted = 0,
    read_committed = 1,
};

struct topic_namespace_view {
    topic_namespace_view(const model::ns& n, const model::topic& t)
      : ns(n)
      , tp(t) {}

    explicit topic_namespace_view(const ntp& ntp)
      : topic_namespace_view(ntp.ns, ntp.tp.topic) {}

    template<typename H>
    friend H AbslHashValue(H h, const topic_namespace_view& tp_ns) {
        return H::combine(std::move(h), tp_ns.ns, tp_ns.tp);
    }

    const model::ns& ns;
    const model::topic& tp;

    friend std::ostream& operator<<(std::ostream&, const topic_namespace_view&);
};

struct topic_namespace {
    topic_namespace() = default;

    topic_namespace(model::ns n, model::topic t)
      : ns(std::move(n))
      , tp(std::move(t)) {}

    explicit topic_namespace(topic_namespace_view view)
      : ns(view.ns)
      , tp(view.tp) {}

    operator topic_namespace_view() { return topic_namespace_view(ns, tp); }

    operator topic_namespace_view() const {
        return topic_namespace_view(ns, tp);
    }

    bool operator==(const topic_namespace_view& other) const {
        return tp == other.tp && ns == other.ns;
    }

    friend bool operator==(const topic_namespace&, const topic_namespace&)
      = default;

    template<typename H>
    friend H AbslHashValue(H h, const topic_namespace& tp_ns) {
        return H::combine(std::move(h), tp_ns.ns, tp_ns.tp);
    }

    friend void write(iobuf& out, topic_namespace t) {
        using serde::write;
        write(out, std::move(t.ns));
        write(out, std::move(t.tp));
    }

    friend void read_nested(
      iobuf_parser& in,
      topic_namespace& t,
      std::size_t const bytes_left_limit) {
        using serde::read_nested;
        read_nested(in, t.ns, bytes_left_limit);
        read_nested(in, t.tp, bytes_left_limit);
    }

    model::ns ns;
    model::topic tp;

    friend std::ostream& operator<<(std::ostream&, const topic_namespace&);
};

struct topic_namespace_hash {
    using is_transparent = void;

    size_t operator()(topic_namespace_view v) const {
        return absl::Hash<topic_namespace_view>{}(v);
    }

    size_t operator()(const topic_namespace& v) const {
        return absl::Hash<topic_namespace>{}(v);
    }
};

struct topic_namespace_eq {
    using is_transparent = void;

    bool operator()(topic_namespace_view lhs, topic_namespace_view rhs) const {
        return lhs.ns == rhs.ns && lhs.tp == rhs.tp;
    }

    bool
    operator()(const topic_namespace& lhs, const topic_namespace& rhs) const {
        return lhs.ns == rhs.ns && lhs.tp == rhs.tp;
    }

    bool
    operator()(const topic_namespace& lhs, topic_namespace_view rhs) const {
        return lhs.ns == rhs.ns && lhs.tp == rhs.tp;
    }

    bool
    operator()(topic_namespace_view lhs, const topic_namespace& rhs) const {
        return lhs.ns == rhs.ns && lhs.tp == rhs.tp;
    }
};

struct topic_metadata
  : serde::
      envelope<topic_metadata, serde::version<0>, serde::compat_version<0>> {
    topic_metadata() noexcept = default;
    explicit topic_metadata(topic_namespace v) noexcept
      : tp_ns(std::move(v)) {}
    topic_namespace tp_ns;
    std::vector<partition_metadata> partitions;

    friend std::ostream& operator<<(std::ostream&, const topic_metadata&);
    friend bool operator==(const topic_metadata&, const topic_metadata&)
      = default;

    auto serde_fields() { return std::tie(tp_ns, partitions); }
};

enum class cloud_credentials_source {
    config_file = 0,
    aws_instance_metadata = 1,
    sts = 2,
    gcp_instance_metadata = 3,
};

std::ostream& operator<<(std::ostream& os, const cloud_credentials_source& cs);

enum class partition_autobalancing_mode {
    off = 0,
    node_add,
    continuous,
};

inline std::ostream&
operator<<(std::ostream& o, const partition_autobalancing_mode& m) {
    switch (m) {
    case model::partition_autobalancing_mode::off:
        return o << "off";
    case model::partition_autobalancing_mode::node_add:
        return o << "node_add";
    case model::partition_autobalancing_mode::continuous:
        return o << "continuous";
    }
}

enum class cloud_storage_backend {
    aws = 0,
    google_s3_compat = 1,
    azure = 2,
    minio = 3,
    unknown = 4,
};

inline std::ostream& operator<<(std::ostream& os, cloud_storage_backend csb) {
    switch (csb) {
    case cloud_storage_backend::aws:
        return os << "aws";
    case cloud_storage_backend::google_s3_compat:
        return os << "google_s3_compat";
    case cloud_storage_backend::azure:
        return os << "azure";
    case cloud_storage_backend::minio:
        return os << "minio";
    case cloud_storage_backend::unknown:
        return os << "unknown";
    }
}

enum class leader_balancer_mode : uint8_t {
    greedy_balanced_shards = 0,
    random_hill_climbing = 1,
};

constexpr const char*
leader_balancer_mode_to_string(leader_balancer_mode mode) {
    switch (mode) {
    case leader_balancer_mode::greedy_balanced_shards:
        return "greedy_balanced_shards";
    case leader_balancer_mode::random_hill_climbing:
        return "random_hill_climbing";
    default:
        throw std::invalid_argument("unknown leader_balancer_mode");
    }
}

std::ostream& operator<<(std::ostream&, leader_balancer_mode);
std::istream& operator>>(std::istream&, leader_balancer_mode&);

namespace internal {
/*
 * Old version for use in backwards compatibility serialization /
 * deserialization helpers.
 */
struct broker_v0 {
    model::node_id id;
    net::unresolved_address kafka_address;
    net::unresolved_address rpc_address;
    std::optional<rack_id> rack;
    model::broker_properties properties;

    model::broker to_v3() const {
        return model::broker(id, kafka_address, rpc_address, rack, properties);
    }
};

} // namespace internal
} // namespace model

template<>
struct fmt::formatter<model::isolation_level> final
  : fmt::formatter<std::string_view> {
    using isolation_level = model::isolation_level;
    template<typename FormatContext>
    auto format(const isolation_level& s, FormatContext& ctx) const {
        std::string_view str = "unknown";
        switch (s) {
        case isolation_level::read_uncommitted:
            str = "read_uncommitted";
            break;
        case isolation_level::read_committed:
            str = "read_committed";
            break;
        }
        return formatter<string_view>::format(str, ctx);
    }
};

namespace std {
template<>
struct hash<model::broker_shard> {
    size_t operator()(const model::broker_shard& bs) const {
        size_t h = 0;
        boost::hash_combine(h, std::hash<model::node_id>()(bs.node_id));
        boost::hash_combine(h, std::hash<uint32_t>()(bs.shard));
        return h;
    }
};

template<>
struct hash<model::broker_endpoint> {
    size_t operator()(const model::broker_endpoint& ep) const {
        size_t h = 0;
        boost::hash_combine(h, std::hash<ss::sstring>()(ep.name));
        boost::hash_combine(
          h, std::hash<net::unresolved_address>()(ep.address));
        return h;
    }
};

template<>
struct hash<model::broker_properties> {
    size_t operator()(const model::broker_properties& b) const {
        size_t h = 0;
        boost::hash_combine(h, std::hash<uint32_t>()(b.cores));
        boost::hash_combine(h, std::hash<uint32_t>()(b.available_memory_gb));
        boost::hash_combine(h, std::hash<uint32_t>()(b.available_disk_gb));
        for (const auto& path : b.mount_paths) {
            boost::hash_combine(h, std::hash<ss::sstring>()(path));
        }
        for (const auto& [k, v] : b.etc_props) {
            boost::hash_combine(h, std::hash<ss::sstring>()(k));
            boost::hash_combine(h, std::hash<ss::sstring>()(v));
        }
        return h;
    }
};

template<>
struct hash<model::broker> {
    size_t operator()(const model::broker& b) const {
        size_t h = 0;
        boost::hash_combine(h, std::hash<model::node_id>()(b.id()));
        for (const auto& ep : b.kafka_advertised_listeners()) {
            boost::hash_combine(h, std::hash<model::broker_endpoint>()(ep));
        }
        boost::hash_combine(
          h, std::hash<net::unresolved_address>()(b.rpc_address()));
        boost::hash_combine(
          h, std::hash<model::broker_properties>()(b.properties()));
        boost::hash_combine(
          h, std::hash<std::optional<ss::sstring>>()(b.rack()));
        return h;
    }
};
} // namespace std
