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

#include "base/seastarx.h"
#include "base/units.h"
#include "model/fundamental.h"
#include "serde/envelope.h"
#include "serde/rw/named_type.h"
#include "serde/rw/optional.h"
#include "serde/rw/rw.h"
#include "serde/rw/tristate_rw.h"
#include "serde/rw/vector.h"
#include "utils/named_type.h"
#include "utils/unresolved_address.h"
#include "utils/xid.h"

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

/// Rack id type
using rack_id = named_type<ss::sstring, struct rack_id_model_type>;
struct broker_properties
  : serde::
      envelope<broker_properties, serde::version<2>, serde::compat_version<0>> {
    uint32_t cores;
    uint32_t available_memory_gb;
    uint32_t available_disk_gb;
    std::vector<ss::sstring> mount_paths;
    // key=value properties in /etc/redpanda/machine_properties.yaml
    std::unordered_map<ss::sstring, ss::sstring> etc_props;
    uint64_t available_memory_bytes = 0;
    fips_mode_flag in_fips_mode = fips_mode_flag::disabled;

    bool operator==(const broker_properties& other) const = default;

    friend std::ostream&
    operator<<(std::ostream&, const model::broker_properties&);

    auto serde_fields() {
        return std::tie(
          cores,
          available_memory_gb,
          available_disk_gb,
          mount_paths,
          etc_props,
          available_memory_bytes,
          in_fips_mode);
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

    static std::optional<ss::sstring>
    validate_not_is_addr_any(const broker_endpoint& ep) {
        bool is_any = [](const broker_endpoint& ep) {
            try {
                // Try a numerical parse of the hostname and check whether it is
                // ADDR_ANY
                return ss::net::inet_address{ep.address.host()}.is_addr_any();
            } catch (...) {
                // Parse failed, so probably a DNS name or invalid. Either way,
                // not ADDR_ANY
                return false;
            }
        }(ep);

        if (is_any) {
            return ssx::sformat(
              "listener '{}' improperly configured with ADDR_ANY ({})",
              ep.name,
              ep.address.host());
        }
        return std::nullopt;
    }

    static std::optional<ss::sstring>
    validate_many(const std::vector<broker_endpoint>& v) {
        for (const auto& ep : v) {
            auto err = broker_endpoint::validate_not_is_addr_any(ep);
            if (err) {
                return err;
            }
        }
        return std::nullopt;
    }

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

    /// Returns the memory for this broker in bytes.
    uint64_t memory_bytes() const {
        // For redpanda < 23.3, available_memory_bytes is not populated and
        // will be zero. Fallback to available_memory_gb in that case.
        if (_properties.available_memory_bytes > 0) {
            return _properties.available_memory_bytes;
        }

        return _properties.available_memory_gb * 1_GiB;
    }

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
      iobuf_parser& in, broker_shard& bs, const std::size_t bytes_left_limit) {
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

    bool operator<(const topic_namespace_view& other) const {
        return std::make_tuple(ns, tp) < std::make_tuple(other.ns, other.tp);
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

    bool operator<(const topic_namespace_view& other) const {
        return topic_namespace_view(*this) < other;
    }
    bool operator<(const topic_namespace& other) const {
        return *this < topic_namespace_view(other);
    }

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
      const std::size_t bytes_left_limit) {
        using serde::read_nested;
        read_nested(in, t.ns, bytes_left_limit);
        read_nested(in, t.tp, bytes_left_limit);
    }

    model::ns ns;
    model::topic tp;

    ss::sstring path() const;

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
    azure_aks_oidc_federation = 4,
    azure_vm_instance_metadata = 5,
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
    oracle_s3_compat = 4,
    unknown
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
    case cloud_storage_backend::oracle_s3_compat:
        return os << "oracle_s3_compat";
    case cloud_storage_backend::unknown:
        return os << "unknown";
    }
}

enum class cloud_storage_chunk_eviction_strategy {
    eager = 0,
    capped = 1,
    predictive = 2,
};

inline std::ostream&
operator<<(std::ostream& os, cloud_storage_chunk_eviction_strategy st) {
    switch (st) {
    case cloud_storage_chunk_eviction_strategy::eager:
        return os << "eager";
    case cloud_storage_chunk_eviction_strategy::capped:
        return os << "capped";
    case cloud_storage_chunk_eviction_strategy::predictive:
        return os << "predictive";
    }
}

enum class fetch_read_strategy : uint8_t {
    polling = 0,
    non_polling = 1,
    non_polling_with_debounce = 2,
    non_polling_with_pid = 3,
};

constexpr const char* fetch_read_strategy_to_string(fetch_read_strategy s) {
    switch (s) {
    case fetch_read_strategy::polling:
        return "polling";
    case fetch_read_strategy::non_polling:
        return "non_polling";
    case fetch_read_strategy::non_polling_with_debounce:
        return "non_polling_with_debounce";
    case fetch_read_strategy::non_polling_with_pid:
        return "non_polling_with_pid";
    default:
        throw std::invalid_argument("unknown fetch_read_strategy");
    }
}

std::ostream& operator<<(std::ostream&, fetch_read_strategy);
std::istream& operator>>(std::istream&, fetch_read_strategy&);

/**
 * Type representing MPX virtual cluster. MPX uses XID to identify clusters.
 */
using vcluster_id = named_type<xid, struct v_cluster_id_tag>;

/**
 * Type that represents the cluster wide write caching mode.
 */
enum class write_caching_mode : uint8_t {
    // true by default for all topics
    default_true = 0,
    // false by default for all topics
    default_false = 1,
    // disabled across all topics even for those
    // with overrides. kill switch.
    disabled = 2
};

constexpr const char* write_caching_mode_to_string(write_caching_mode s) {
    switch (s) {
    case write_caching_mode::default_true:
        return "true";
    case write_caching_mode::default_false:
        return "false";
    case write_caching_mode::disabled:
        return "disabled";
    default:
        throw std::invalid_argument("unknown write_caching_mode");
    }
}

std::optional<write_caching_mode>
  write_caching_mode_from_string(std::string_view);

std::ostream& operator<<(std::ostream&, write_caching_mode);
std::istream& operator>>(std::istream&, write_caching_mode&);

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

enum class recovery_validation_mode : std::uint16_t {
    // ensure that either the manifest is in TS or that no manifest is present.
    // download issues will fail the validation
    check_manifest_existence = 0,
    // download the manifest and check the most recent segments up to
    // max_segment_depth
    check_manifest_and_segment_metadata = 1,
    // do not perform any check, validation is considered successful
    no_check = 0xff,
};

std::ostream& operator<<(std::ostream&, recovery_validation_mode);
std::istream& operator>>(std::istream&, recovery_validation_mode&);

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
        return fmt::format_to(ctx.out(), "{}", str);
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
        boost::hash_combine(
          h, std::hash<model::fips_mode_flag>()(b.in_fips_mode));
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
