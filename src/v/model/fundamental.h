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

/// \brief namespace is reserved in c++;  use ns
using ns = named_type<ss::sstring, struct model_ns_type>;

using offset = named_type<int64_t, struct model_offset_type>;

struct topic_partition {
    using compaction = ss::bool_class<struct compaction_tag>;

    topic_partition() = default;
    topic_partition(model::topic t, model::partition_id i)
      : topic(std::move(t))
      , partition(i) {}
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

    friend std::ostream& operator<<(std::ostream&, const topic_partition&);
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
