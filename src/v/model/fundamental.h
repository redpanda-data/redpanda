#pragma once

#include "seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <boost/container_hash/hash.hpp>

#include <cstdint>
#include <limits>
#include <string_view>

namespace model {

using term_id = named_type<int64_t, struct model_raft_term_id_type>;

using partition_id = named_type<int32_t, struct model_partition_id_type>;

using topic_view = named_type<std::string_view, struct model_topic_view_type>;

using topic = named_type<sstring, struct model_topic_type>;

/// \brief namespace is reserved in c++;  use ns
using ns = named_type<sstring, struct model_ns_type>;

struct topic_partition {
    model::topic topic;
    model::partition_id partition;

    bool operator==(const topic_partition& other) const {
        return topic == other.topic && partition == other.partition;
    }

    bool operator!=(const topic_partition& other) const {
        return !(*this == other);
    }
};

std::ostream& operator<<(std::ostream&, const topic_partition&);

struct ntp {
    model::ns ns;
    topic_partition tp;

    bool operator==(const ntp& other) const {
        return ns == other.ns && tp == other.tp;
    }

    bool operator!=(const ntp& other) const {
        return !(*this == other);
    }

    sstring path() const;
};

std::ostream& operator<<(std::ostream&, const ntp&);

struct offset : named_type<uint64_t, struct model_offset_type> {
    offset() = default;
    explicit offset(const type&& t)
      : named_type<uint64_t, struct model_offset_type>(t) {
    }
    type value() const {
        return *this;
    }

    /// \brief used by the kafka _signed_ integer api
    offset operator+(int32_t val) const {
        return offset(_value + static_cast<type>(val));
    }
    /// \brief used by the kafka _signed_ integer api
    offset operator+(int64_t val) const {
        return offset(_value + static_cast<type>(val));
    }
    /// \brief used by the kafka _signed_ integer api
    offset& operator+=(int64_t val) {
        _value += static_cast<type>(val);
        return *this;
    }
};

std::ostream& operator<<(std::ostream&, offset);

} // namespace model

namespace std {

template<>
struct hash<model::ntp> {
    size_t operator()(const model::ntp& ntp) const {
        size_t h = 0;
        boost::hash_combine(h, hash<sstring>()(ntp.ns));
        boost::hash_combine(h, hash<sstring>()(ntp.tp.topic));
        boost::hash_combine(h, hash<model::partition_id>()(ntp.tp.partition));
        return h;
    }
};

} // namespace std
