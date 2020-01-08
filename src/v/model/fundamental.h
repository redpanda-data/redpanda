#pragma once

#include "seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/bool_class.hh>

#include <boost/container_hash/hash.hpp>

#include <cstdint>
#include <limits>
#include <string_view>

namespace model {

using term_id = named_type<int64_t, struct model_raft_term_id_type>;

using partition_id = named_type<int32_t, struct model_partition_id_type>;

using topic_view = named_type<std::string_view, struct model_topic_view_type>;

class topic : public named_type<sstring, struct model_topic_type> {
public:
    using named_type<sstring, struct model_topic_type>::named_type;

    topic(model::topic_view view) // NOLINT - see topic_view_tests.cc
      : named_type<sstring, struct model_topic_type>(sstring(view())) {}

    operator topic_view() { return topic_view(_value); }

    operator topic_view() const { return topic_view(_value); }
};

/// \brief namespace is reserved in c++;  use ns
using ns = named_type<sstring, struct model_ns_type>;

using offset = named_type<uint64_t, struct model_offset_type>;

struct topic_partition {
    using compaction = bool_class<struct compaction_tag>;

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

    bool operator!=(const ntp& other) const { return !(*this == other); }

    sstring path() const;
};

std::ostream& operator<<(std::ostream&, const ntp&);

} // namespace model

namespace std {
template<>
struct hash<model::topic> {
    size_t operator()(const model::topic& tp) const {
        return std::hash<sstring>()(tp());
    }
};

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
