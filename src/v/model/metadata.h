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

#include "model/fundamental.h"
#include "seastarx.h"
#include "utils/named_type.h"
#include "utils/unresolved_address.h"

#include <seastar/core/sstring.hh>

#include <absl/hash/hash.h>
#include <boost/functional/hash.hpp>

#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace model {
using node_id = named_type<int32_t, struct node_id_model_type>;
/**
 * We use revision_id to identify entities evolution in time. f.e. NTP that was
 * first created and then removed, raft configuration
 */
using revision_id = named_type<int64_t, struct revision_id_model_type>;
struct broker_properties {
    uint32_t cores;
    uint32_t available_memory;
    uint32_t available_disk;
    std::vector<ss::sstring> mount_paths;
    // key=value properties in /etc/redpanda/machine_properties.yaml
    std::unordered_map<ss::sstring, ss::sstring> etc_props;

    bool operator==(const broker_properties& other) const {
        return cores == other.cores
               && available_memory == other.available_memory
               && available_disk == other.available_disk
               && mount_paths == other.mount_paths
               && etc_props == other.etc_props;
    }
};

struct broker_endpoint final {
    ss::sstring name;
    unresolved_address address;

    // required for yaml serde
    broker_endpoint() = default;

    broker_endpoint(ss::sstring name, unresolved_address address) noexcept
      : name(std::move(name))
      , address(std::move(address)) {}

    explicit broker_endpoint(unresolved_address address) noexcept
      : address(std::move(address)) {}

    bool operator==(const broker_endpoint&) const = default;
    friend std::ostream& operator<<(std::ostream&, const broker_endpoint&);
};

std::ostream& operator<<(std::ostream&, const broker_endpoint&);

class broker {
public:
    broker(
      node_id id,
      std::vector<broker_endpoint> kafka_advertised_listeners,
      unresolved_address rpc_address,
      std::optional<ss::sstring> rack,
      broker_properties props) noexcept
      : _id(id)
      , _kafka_advertised_listeners(std::move(kafka_advertised_listeners))
      , _rpc_address(std::move(rpc_address))
      , _rack(std::move(rack))
      , _properties(std::move(props)) {}

    broker(
      node_id id,
      unresolved_address kafka_advertised_listener,
      unresolved_address rpc_address,
      std::optional<ss::sstring> rack,
      broker_properties props) noexcept
      : broker(
        id,
        {broker_endpoint(std::move(kafka_advertised_listener))},
        std::move(rpc_address),
        std::move(rack),
        std::move(props)) {}

    broker(broker&&) noexcept = default;
    broker& operator=(broker&&) noexcept = default;
    broker(const broker&) = default;
    const node_id& id() const { return _id; }

    const broker_properties& properties() const { return _properties; }
    const std::vector<broker_endpoint>& kafka_advertised_listeners() const {
        return _kafka_advertised_listeners;
    }
    const unresolved_address& rpc_address() const { return _rpc_address; }
    const std::optional<ss::sstring>& rack() const { return _rack; }

    bool operator==(const model::broker& other) const = default;
    bool operator<(const model::broker& other) const { return _id < other._id; }

private:
    node_id _id;
    std::vector<broker_endpoint> _kafka_advertised_listeners;
    unresolved_address _rpc_address;
    std::optional<ss::sstring> _rack;
    broker_properties _properties;

    friend std::ostream& operator<<(std::ostream&, const broker&);
};

std::ostream& operator<<(std::ostream&, const broker&);

/// type representing single replica assignment it contains the id of a broker
/// and id of this broker shard.
struct broker_shard {
    model::node_id node_id;
    /// this is the same as a ss::shard_id
    /// however, seastar uses unsized-ints (unsigned)
    /// and for predictability we need fixed-sized ints
    uint32_t shard;
    friend std::ostream& operator<<(std::ostream&, const broker_shard&);
};

struct partition_metadata {
    partition_metadata() noexcept = default;
    explicit partition_metadata(partition_id p) noexcept
      : id(p) {}
    partition_id id;
    std::vector<broker_shard> replicas;
    std::optional<model::node_id> leader_node;

    friend std::ostream& operator<<(std::ostream&, const partition_metadata&);
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

    template<typename H>
    friend H AbslHashValue(H h, const topic_namespace& tp_ns) {
        return H::combine(std::move(h), tp_ns.ns, tp_ns.tp);
    }

    model::ns ns;
    model::topic tp;

    friend std::ostream& operator<<(std::ostream&, const topic_namespace&);
};

std::ostream& operator<<(std::ostream&, const topic_namespace&);
std::ostream& operator<<(std::ostream&, const topic_namespace_view&);

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

struct topic_metadata {
    explicit topic_metadata(topic_namespace v) noexcept
      : tp_ns(std::move(v)) {}
    topic_namespace tp_ns;
    std::vector<partition_metadata> partitions;

    friend std::ostream& operator<<(std::ostream&, const topic_metadata&);
};

namespace internal {
/*
 * Old version for use in backwards compatibility serialization /
 * deserialization helpers.
 */
struct broker_v0 {
    model::node_id id;
    unresolved_address kafka_address;
    unresolved_address rpc_address;
    std::optional<ss::sstring> rack;
    model::broker_properties properties;

    model::broker to_v3() const {
        return model::broker(id, kafka_address, rpc_address, rack, properties);
    }
};

} // namespace internal
} // namespace model

namespace std {

template<>
struct hash<model::broker_endpoint> {
    size_t operator()(const model::broker_endpoint& ep) const {
        size_t h = 0;
        boost::hash_combine(h, std::hash<ss::sstring>()(ep.name));
        boost::hash_combine(h, std::hash<unresolved_address>()(ep.address));
        return h;
    }
};

template<>
struct hash<model::broker_properties> {
    size_t operator()(const model::broker_properties& b) const {
        size_t h = 0;
        boost::hash_combine(h, std::hash<uint32_t>()(b.cores));
        boost::hash_combine(h, std::hash<uint32_t>()(b.available_memory));
        boost::hash_combine(h, std::hash<uint32_t>()(b.available_disk));
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
          h, std::hash<unresolved_address>()(b.rpc_address()));
        boost::hash_combine(
          h, std::hash<model::broker_properties>()(b.properties()));
        boost::hash_combine(
          h, std::hash<std::optional<ss::sstring>>()(b.rack()));
        return h;
    }
};
} // namespace std
