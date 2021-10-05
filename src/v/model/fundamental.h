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

#include "seastarx.h"
#include "ssx/sformat.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/bool_class.hh>

#include <boost/container_hash/hash.hpp>

#include <cstdint>
#include <limits>
#include <ostream>
#include <string_view>

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

    operator topic_view() { return topic_view(_value); }

    operator topic_view() const { return topic_view(_value); }
};

/// \brief View wrapper for a materialized topic format
/// The format for a materialized_topic will not pass kafka topic validators
struct materialized_topic {
    model::topic_view src;
    model::topic_view dest;
};

/// Parses a topic formatted as a materialized topic
/// \return materialized_topic view or std::nullopt if parameter fails to be
/// parsed correctly
std::optional<materialized_topic> make_materialized_topic(const model::topic&);

/// \brief Returns true if the schema obeys the $<src>.<dest>$ pattern
inline bool is_materialized_topic(const model::topic& t) {
    return make_materialized_topic(t).has_value();
}

inline model::topic get_source_topic(const model::topic& topic) {
    const std::optional<materialized_topic> maybe_materialized
      = make_materialized_topic(topic);
    return maybe_materialized ? model::topic(maybe_materialized->src) : topic;
}

inline model::topic
to_materialized_topic(const model::topic& src, const model::topic& dest) {
    return model::topic(ssx::sformat("{}.${}$", src(), dest()));
}

/// \brief namespace is reserved in c++;  use ns
using ns = named_type<ss::sstring, struct model_ns_type>;

using offset = named_type<int64_t, struct model_offset_type>;

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

    template<typename H>
    friend H AbslHashValue(H h, const topic_partition& tp) {
        return H::combine(std::move(h), tp.topic, tp.partition);
    }
};

std::ostream& operator<<(std::ostream&, const topic_partition&);

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

    ss::sstring path() const;
    std::filesystem::path topic_path() const;

    friend std::ostream& operator<<(std::ostream&, const ntp&);
};

class materialized_ntp {
public:
    explicit materialized_ntp(model::ntp ntp) noexcept;
    materialized_ntp(const materialized_ntp& other) noexcept;
    materialized_ntp(materialized_ntp&& other) noexcept;

    materialized_ntp& operator=(const materialized_ntp&) = delete;
    materialized_ntp& operator=(materialized_ntp&&) = delete;

    ~materialized_ntp() = default;

    const model::ntp& input_ntp() const { return _input; }
    const model::ntp& source_ntp() const { return _source_or_input; }
    bool is_materialized() const { return _maybe_source.has_value(); }

    bool operator==(const materialized_ntp& other) const {
        return _input == other._input && _maybe_source == other._maybe_source;
    }
    bool operator!=(const materialized_ntp& other) const {
        return !(*this == other);
    }

    friend std::ostream& operator<<(std::ostream&, const ntp&);

private:
    std::optional<model::ntp> make_materialized_src_ntp(const model::ntp& ntp);

    model::ntp _input;
    std::optional<model::ntp> _maybe_source;
    const model::ntp& _source_or_input;
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

enum class shadow_indexing_mode : int8_t {
    // Upload is disabled
    disabled = 0,
    // Only upload data to the object storage
    archival_storage = 1,
    // Upload data and enable shadow indexing
    shadow_indexing = 2,
};

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
