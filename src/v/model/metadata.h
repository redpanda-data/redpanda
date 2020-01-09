#pragma once

#include "model/fundamental.h"
#include "seastarx.h"
#include "utils/named_type.h"
#include "utils/unresolved_address.h"

#include <seastar/core/sstring.hh>

#include <boost/container/flat_map.hpp>
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
    model::node_id leader_node;
};

struct topic_metadata {
    explicit topic_metadata(topic v) noexcept
      : tp(std::move(v)) {}
    topic tp;
    std::vector<partition_metadata> partitions;
};

namespace internal {
struct hash_by_topic_name {
    size_t operator()(const topic_metadata& tm) const {
        return std::hash<model::topic>()(tm.tp);
    }
};

struct equals_by_topic_name {
    bool
    operator()(const topic_metadata& tm1, const topic_metadata& tm2) const {
        return tm1.tp == tm2.tp;
    }
};

} // namespace internal

using topic_metadata_map = std::unordered_set<
  topic_metadata,
  internal::hash_by_topic_name,
  internal::equals_by_topic_name>;

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
