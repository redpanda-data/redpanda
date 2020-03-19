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

class broker {
public:
    broker(
      node_id id,
      unresolved_address kafka_api_address,
      unresolved_address rpc_address,
      std::optional<ss::sstring> rack,
      broker_properties props) noexcept
      : _id(id)
      , _kafka_api_address(std::move(kafka_api_address))
      , _rpc_address(std::move(rpc_address))
      , _rack(std::move(rack))
      , _properties(std::move(props)) {}

    broker(broker&&) noexcept = default;
    broker& operator=(broker&&) noexcept = default;
    broker(const broker&) = default;
    const node_id& id() const { return _id; }

    const broker_properties& properties() const { return _properties; }
    const unresolved_address& kafka_api_address() const {
        return _kafka_api_address;
    }
    const unresolved_address& rpc_address() const { return _rpc_address; }
    const std::optional<ss::sstring>& rack() const { return _rack; }

    inline bool operator==(const model::broker& other) const {
        return _id == other._id
               && _kafka_api_address == other._kafka_api_address
               && _rpc_address == other._rpc_address && _rack == other._rack
               && _properties == other._properties;
    }

    inline bool operator!=(const model::broker& other) const {
        return !(*this == other);
    }

    bool operator<(const model::broker& other) const { return _id < other._id; }

private:
    node_id _id;
    unresolved_address _kafka_api_address;
    unresolved_address _rpc_address;
    std::optional<ss::sstring> _rack;
    broker_properties _properties;
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
};

struct partition_metadata {
    partition_metadata() noexcept = default;
    explicit partition_metadata(partition_id p) noexcept
      : id(p) {}
    partition_id id;
    std::vector<broker_shard> replicas;
    std::optional<model::node_id> leader_node;
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

} // namespace model

namespace std {
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
        boost::hash_combine(
          h, std::hash<unresolved_address>()(b.kafka_api_address()));
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
