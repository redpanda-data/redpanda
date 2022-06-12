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

#include "bytes/iobuf.h"
#include "seastarx.h"
#include "serde/serde.h"
#include "ssx/sformat.h"
#include "utils/named_type.h"
#include "vassert.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/bool_class.hh>

#include <boost/container_hash/hash.hpp>

#include <cstdint>
#include <limits>
#include <ostream>
#include <string_view>
#include <type_traits>

namespace model {

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
    read_nested(iobuf_parser& in, topic& t, size_t const bytes_left_limit) {
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

inline constexpr model::offset next_offset(model::offset o) {
    if (o < model::offset{0}) {
        return model::offset{0};
    }
    return o + model::offset{1};
}

inline constexpr model::offset prev_offset(model::offset o) {
    if (o <= model::offset{0}) {
        return model::offset{};
    }
    return o - model::offset{1};
}

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
        return topic == other.topic && partition == other.partition;
    }

    bool operator!=(const topic_partition& other) const {
        return !(*this == other);
    }

    bool operator<(const topic_partition& other) const {
        return topic < other.topic
               || (topic == other.topic && partition < other.partition);
    }

    operator topic_partition_view() {
        return topic_partition_view(topic, partition);
    }

    operator topic_partition_view() const {
        return topic_partition_view(topic, partition);
    }

    friend std::ostream& operator<<(std::ostream&, const topic_partition&);

    friend void read_nested(
      iobuf_parser& in, topic_partition& tp, size_t const bytes_left_limit) {
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
        return ns == other.ns && tp == other.tp;
    }

    bool operator!=(const ntp& other) const { return !(*this == other); }

    bool operator<(const ntp& other) const {
        return ns < other.ns || (ns == other.ns && tp < other.tp);
    }

    friend void
    read_nested(iobuf_parser& in, ntp& ntp, size_t const bytes_left_limit) {
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

using control_record_version
  = named_type<int16_t, struct control_record_version_tag>;

static constexpr control_record_version current_control_record_version{0};

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

} // namespace model

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
        size_t h = 0;
        boost::hash_combine(h, hash<ss::sstring>()(ntp.ns));
        boost::hash_combine(h, hash<ss::sstring>()(ntp.tp.topic));
        boost::hash_combine(h, hash<model::partition_id>()(ntp.tp.partition));
        return h;
    }
};

} // namespace std
