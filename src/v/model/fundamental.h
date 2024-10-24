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
#include "base/vassert.h"
#include "bytes/iobuf.h"
#include "serde/rw/named_type.h"
#include "serde/rw/rw.h"
#include "serde/rw/scalar.h"
#include "serde/rw/sstring.h"
#include "ssx/sformat.h"
#include "utils/named_type.h"
#include "utils/uuid.h"

#include <seastar/core/sstring.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/bool_class.hh>

#include <boost/container_hash/hash.hpp>

#include <cstdint>
#include <limits>
#include <ostream>
#include <string_view>
#include <type_traits>

namespace kafka {

using offset = named_type<int64_t, struct kafka_offset_type>;
using offset_delta = named_type<int64_t, struct kafka_offset_delta_type>;

inline offset next_offset(offset p) {
    if (p < offset{0}) {
        return offset{0};
    } else if (p == offset::max()) {
        return offset::max();
    }
    return p + offset{1};
}

inline constexpr offset prev_offset(offset o) {
    if (o <= offset{0}) {
        return offset{};
    }
    return o - offset{1};
}

inline constexpr offset operator+(offset o, offset_delta d) {
    if (o >= offset{0}) {
        if (d() <= offset::max() - o) {
            return offset{o() + d()};
        } else {
            return offset::max();
        }
    } else {
        if (d() >= offset::min() - o) {
            return offset{o() + d()};
        } else {
            return offset::min();
        }
    }
}

inline constexpr offset operator-(offset o, offset_delta d) { return o + (-d); }

} // namespace kafka

namespace cloud_storage_clients {

using bucket_name = named_type<ss::sstring, struct s3_bucket_name>;

} // namespace cloud_storage_clients

namespace cloud_storage {
/// Segment path in S3, expected format:
/// <prefix>/<ns>/<topic>/<part-id>_<rev>/<base-offset>-<term-id>-<revision>.log.<archiver-term>
using remote_segment_path
  = named_type<std::filesystem::path, struct archival_remote_segment_path_t>;
} // namespace cloud_storage

namespace model {

using node_uuid = named_type<uuid_t, struct node_uuid_type>;
using cluster_uuid = named_type<uuid_t, struct cluster_uuid_type>;

inline constexpr cluster_uuid default_cluster_uuid{};

using node_id = named_type<int32_t, struct node_id_model_type>;

/**
 * Reserved to represent the node_id value yet to be assigned
 * when node is configured for automatic assignment of node_ids.
 * Never used in node configuration.
 */
inline constexpr node_id unassigned_node_id(-1);

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

// tracking evolution of the shard table
using shard_revision_id
  = named_type<int64_t, struct shard_revision_id_model_type>;

// Named after Kafka cleanup.policy topic property
enum class cleanup_policy_bitflags : uint8_t {
    none = 0,
    deletion = 1U,
    compaction = 1U << 1U
};

inline cleanup_policy_bitflags
operator|(cleanup_policy_bitflags a, cleanup_policy_bitflags b) {
    return cleanup_policy_bitflags(
      std::underlying_type_t<cleanup_policy_bitflags>(a)
      | std::underlying_type_t<cleanup_policy_bitflags>(b));
}

inline void operator|=(cleanup_policy_bitflags& a, cleanup_policy_bitflags b) {
    a = (a | b);
}

inline cleanup_policy_bitflags
operator&(cleanup_policy_bitflags a, cleanup_policy_bitflags b) {
    return cleanup_policy_bitflags(
      std::underlying_type_t<cleanup_policy_bitflags>(a)
      & std::underlying_type_t<cleanup_policy_bitflags>(b));
}

inline void operator&=(cleanup_policy_bitflags& a, cleanup_policy_bitflags b) {
    a = (a & b);
}

inline bool is_compaction_enabled(cleanup_policy_bitflags flags) {
    return (flags & cleanup_policy_bitflags::compaction)
           == cleanup_policy_bitflags::compaction;
}

inline bool is_deletion_enabled(cleanup_policy_bitflags flags) {
    return (flags & cleanup_policy_bitflags::deletion)
           == cleanup_policy_bitflags::deletion;
}

std::ostream& operator<<(std::ostream&, cleanup_policy_bitflags);
std::istream& operator>>(std::istream&, cleanup_policy_bitflags&);

// Named after Kafka compaction.strategy topic property
enum class compaction_strategy : int8_t {
    /// \brief offset compaction means the old schoold kafka compacted topics
    /// strategy before KIP 280
    offset,
    /// \brief timestamp compaction is not yet supported
    timestamp,
    /// \brief header field compaction is not yet supported
    header,
};
std::ostream& operator<<(std::ostream&, compaction_strategy);
std::istream& operator>>(std::istream&, compaction_strategy&);

using term_id = named_type<int64_t, struct model_raft_term_id_type>;

using run_id = named_type<int64_t, struct model_raft_run_id_type>;

using partition_id = named_type<int32_t, struct model_partition_id_type>;

using topic_view = named_type<std::string_view, struct model_topic_view_type>;

class topic : public named_type<ss::sstring, struct model_topic_type> {
public:
    using named_type<ss::sstring, struct model_topic_type>::named_type;

    topic(model::topic_view view) // NOLINT - see topic_view_tests.cc
      : named_type<ss::sstring, struct model_topic_type>(ss::sstring(view())) {}

    friend void
    read_nested(iobuf_parser& in, topic& t, const size_t bytes_left_limit) {
        using serde::read_nested;
        return read_nested(in, t._value, bytes_left_limit);
    }

    friend void write(iobuf& out, topic t) {
        using serde::write;
        return write(out, std::move(t._value));
    }

    operator topic_view() { return topic_view(_value); }

    operator topic_view() const { return topic_view(_value); }
};

/// \brief namespace is reserved in c++;  use ns
using ns = named_type<ss::sstring, struct model_ns_type>;

using offset = named_type<int64_t, struct model_offset_type>;

/// Delta between redpanda and kafka offsets. It supposed to be used
/// by offset translation facilities.
using offset_delta = named_type<int64_t, struct model_offset_delta_type>;

/// \brief conversion from kafka offset to redpanda offset
inline constexpr model::offset
operator+(kafka::offset o, model::offset_delta d) {
    return model::offset{o() + d()};
}

/// \brief conversion from redpanda offset to kafka offset
inline constexpr kafka::offset
operator-(model::offset o, model::offset_delta d) {
    return kafka::offset{o() - d()};
}

/// \brief get offset delta from pair of offsets
inline constexpr model::offset_delta
operator-(model::offset r, kafka::offset k) {
    return model::offset_delta{r() - k()};
}

/// \brief cast to model::offset
///
/// The purpose of this function is to mark every place where we converting
/// from offset-delta to model::offset. This is done in places where the delta
/// is represetnted as an instance of the model::offset. Once we convert every
/// delta offset to model::delta_offset we will be able to depricate and remove
/// this function.
inline constexpr model::offset offset_cast(model::offset_delta d) {
    return model::offset{d()};
}

/// \brief cast to kafka::offset
///
/// This function is used when we have a field which is incorrectly represented
/// as model::offset instead of kafka::offset and we need to convert it to
/// proper type.
inline constexpr kafka::offset offset_cast(model::offset r) {
    return kafka::offset{r()};
}

/// \brief cast to model::offset_delta
///
/// This function is used when we have a field which is incorrectly represented
/// as model::offset instead of model::offset_delta and we need to convert it to
/// proper type.
inline constexpr model::offset_delta offset_delta_cast(model::offset r) {
    return model::offset_delta{r()};
}

inline constexpr model::offset next_offset(model::offset o) {
    if (o < model::offset{0}) {
        return model::offset{0};
    } else if (o == model::offset::max()) {
        return model::offset::max();
    }
    return o + model::offset{1};
}

inline constexpr model::offset prev_offset(model::offset o) {
    if (o <= model::offset{0}) {
        return model::offset{};
    }
    return o - model::offset{1};
}

// An invalid offset indicating that actual LSO is not yet ready to be returned.
// Follows the policy that LSO is the next offset of the decided offset.
inline constexpr model::offset invalid_lso{next_offset(model::offset::min())};

struct topic_partition_view {
    topic_partition_view(model::topic_view tp, model::partition_id p)
      : topic(tp)
      , partition(p) {}

    model::topic_view topic;
    model::partition_id partition;
    template<typename H>
    friend H AbslHashValue(H h, const topic_partition_view& tp) {
        return H::combine(std::move(h), tp.topic, tp.partition);
    }
};
struct topic_partition {
    topic_partition() = default;
    topic_partition(model::topic t, model::partition_id i)
      : topic(std::move(t))
      , partition(i) {}

    explicit topic_partition(model::topic_partition_view view)
      : topic_partition(model::topic(view.topic), view.partition) {}

    model::topic topic;
    model::partition_id partition;

    bool operator==(const topic_partition& other) const {
        return partition == other.partition && topic == other.topic;
    }

    bool operator!=(const topic_partition& other) const {
        return !(*this == other);
    }

    bool operator<(const topic_partition& other) const {
        return topic < other.topic
               || (topic == other.topic && partition < other.partition);
    }

    auto operator<=>(const topic_partition& other) const noexcept = default;

    operator topic_partition_view() {
        return topic_partition_view(topic, partition);
    }

    operator topic_partition_view() const {
        return topic_partition_view(topic, partition);
    }

    friend std::ostream& operator<<(std::ostream&, const topic_partition&);

    friend void read_nested(
      iobuf_parser& in, topic_partition& tp, const size_t bytes_left_limit) {
        using serde::read_nested;

        read_nested(in, tp.topic, bytes_left_limit);
        read_nested(in, tp.partition, bytes_left_limit);
    }

    friend void write(iobuf& out, topic_partition tp) {
        using serde::write;

        write(out, std::move(tp.topic));
        write(out, tp.partition);
    }
    template<typename H>
    friend H AbslHashValue(H h, const topic_partition& tp) {
        return H::combine(std::move(h), tp.topic, tp.partition);
    }
};

struct ntp {
    ntp() = default;
    ntp(model::ns::type n, model::topic::type t, model::partition_id::type i)
      : ns(model::ns(std::move(n)))
      , tp(model::topic(std::move(t)), model::partition_id(i)) {}
    ntp(model::ns n, model::topic t, model::partition_id i)
      : ns(std::move(n))
      , tp(std::move(t), i) {}
    ntp(model::ns n, topic_partition t)
      : ns(std::move(n))
      , tp(std::move(t)) {}
    model::ns ns;
    topic_partition tp;

    bool operator==(const ntp& other) const {
        return tp == other.tp && ns == other.ns;
    }

    bool operator!=(const ntp& other) const { return !(*this == other); }

    bool operator<(const ntp& other) const {
        return ns < other.ns || (ns == other.ns && tp < other.tp);
    }

    friend void
    read_nested(iobuf_parser& in, ntp& ntp, const size_t bytes_left_limit) {
        using serde::read_nested;

        read_nested(in, ntp.ns, bytes_left_limit);
        read_nested(in, ntp.tp, bytes_left_limit);
    }

    friend void write(iobuf& out, ntp ntp) {
        using serde::write;

        write(out, std::move(ntp.ns));
        write(out, std::move(ntp.tp));
    }

    ss::sstring path() const;
    std::filesystem::path topic_path() const;

    friend std::ostream& operator<<(std::ostream&, const ntp&);
};

/**
 * Enum describing Kafka's control record types. The Control record key schema
 * is defined as a tuple of [version: int16_t, type: control_record_type].
 * Currently Kafka protocol uses version 0 of control record schema.
 *
 * Internal redpanda control records are propagated with unknown type.
 * Unknown control records are expected to be ignored by Kafka clients
 *
 */
enum class control_record_type : int16_t {
    tx_abort = 0,
    tx_commit = 1,
    unknown = -1
};
std::ostream& operator<<(std::ostream&, const control_record_type&);

using control_record_version
  = named_type<int16_t, struct control_record_version_tag>;

inline constexpr control_record_version current_control_record_version{0};

enum class shadow_indexing_mode : uint8_t {
    // Upload is disabled
    disabled = 0,
    // Only upload data to the object storage
    archival = 1,
    // Enable download from object storage
    fetch = 2,
    // Enable both upload and download
    full = 3,
    // Remove archival flag (used for incremental updates)
    drop_archival = 0xfe,
    // Remove fetch flag (used for incremental updates)
    drop_fetch = 0xfd,
    // Remove both fetch and archival flags
    drop_full = 0xfc,
};

inline bool is_archival_enabled(shadow_indexing_mode m) {
    return m == shadow_indexing_mode::archival
           || m == shadow_indexing_mode::full;
}

inline bool is_fetch_enabled(shadow_indexing_mode m) {
    return m == shadow_indexing_mode::fetch || m == shadow_indexing_mode::full;
}

/// Set 'rhs' flag in 'lhs'
constexpr shadow_indexing_mode
add_shadow_indexing_flag(shadow_indexing_mode lhs, shadow_indexing_mode rhs) {
    using underlying = std::underlying_type_t<shadow_indexing_mode>;
    if (
      rhs == shadow_indexing_mode::drop_archival
      || rhs == shadow_indexing_mode::drop_fetch
      || rhs == shadow_indexing_mode::drop_full) {
        auto combined = underlying(lhs) & underlying(rhs);
        return shadow_indexing_mode(combined);
    }
    auto combined = underlying(lhs) | underlying(rhs);
    return shadow_indexing_mode(combined);
}

/// Turn normal shadow indexing flag into a 'drop_' flag. This flag
/// can be used with 'add_shadow_indexing_flag' function to remove the flag.
constexpr shadow_indexing_mode
negate_shadow_indexing_flag(shadow_indexing_mode m) {
    using underlying = std::underlying_type_t<shadow_indexing_mode>;
    return shadow_indexing_mode(~underlying(m));
}

static_assert(
  add_shadow_indexing_flag(
    shadow_indexing_mode::fetch,
    negate_shadow_indexing_flag(shadow_indexing_mode::fetch))
  == shadow_indexing_mode::disabled);
static_assert(
  add_shadow_indexing_flag(
    shadow_indexing_mode::fetch, shadow_indexing_mode::drop_fetch)
  == shadow_indexing_mode::disabled);
static_assert(
  add_shadow_indexing_flag(
    shadow_indexing_mode::archival,
    negate_shadow_indexing_flag(shadow_indexing_mode::archival))
  == shadow_indexing_mode::disabled);
static_assert(
  add_shadow_indexing_flag(
    shadow_indexing_mode::archival, shadow_indexing_mode::drop_archival)
  == shadow_indexing_mode::disabled);
static_assert(
  add_shadow_indexing_flag(
    shadow_indexing_mode::full,
    negate_shadow_indexing_flag(shadow_indexing_mode::full))
  == shadow_indexing_mode::disabled);
static_assert(
  add_shadow_indexing_flag(
    shadow_indexing_mode::full, shadow_indexing_mode::drop_full)
  == shadow_indexing_mode::disabled);

std::ostream& operator<<(std::ostream&, const shadow_indexing_mode&);

using client_address_t = ss::socket_address;

enum class fips_mode_flag : uint8_t {
    // FIPS mode disabled
    disabled = 0,
    // FIPS mode enabled with permissive environment checks
    permissive = 1,
    // FIPS mode enabled with strict environment checks
    enabled = 2,
};

constexpr std::string_view to_string_view(fips_mode_flag f) {
    switch (f) {
    case fips_mode_flag::disabled:
        return "disabled";
    case fips_mode_flag::permissive:
        return "permissive";
    case fips_mode_flag::enabled:
        return "enabled";
    }
}

std::ostream& operator<<(std::ostream& os, const fips_mode_flag& f);
std::istream& operator>>(std::istream& is, fips_mode_flag& f);
} // namespace model

namespace kafka {

/// \brief cast to model::offset
///
/// The purpose of this function is to mark every place where we converting
/// from kafka offset to model::offset. This is done in places where the kafka
/// offset is represetnted as an instance of the model::offset. Once we convert
/// every such field to kafka::delta_offset we will be able to depricate and
/// remove this function.
inline constexpr model::offset offset_cast(kafka::offset k) {
    return model::offset{k()};
}

} // namespace kafka

namespace detail {
inline size_t ntp_hash(
  std::string_view ns, std::string_view topic, model::partition_id partition) {
    size_t h = 0;
    boost::hash_combine(h, std::hash<std::string_view>()(ns));
    boost::hash_combine(h, std::hash<std::string_view>()(topic));
    boost::hash_combine(h, std::hash<model::partition_id>()(partition));
    return h;
}
} // namespace detail

namespace std {
template<>
struct hash<model::topic> {
    size_t operator()(const model::topic& tp) const {
        return std::hash<ss::sstring>()(tp());
    }
};

template<>
struct hash<model::topic_partition> {
    size_t operator()(const model::topic_partition& tp) const {
        size_t h = 0;
        boost::hash_combine(h, hash<ss::sstring>()(tp.topic));
        boost::hash_combine(h, hash<model::partition_id>()(tp.partition));
        return h;
    }
};

template<>
struct hash<model::ntp> {
    size_t operator()(const model::ntp& ntp) const {
        return detail::ntp_hash(ntp.ns(), ntp.tp.topic(), ntp.tp.partition);
    }
};

} // namespace std
