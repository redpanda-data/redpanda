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

class topic_view {
public:
    explicit topic_view(std::string_view topic_name) noexcept
      : _topic_name(topic_name) {
    }

    std::string_view name() const {
        return _topic_name;
    }

    bool operator==(const topic_view& other) const {
        return _topic_name == other._topic_name;
    }

    bool operator!=(const topic_view& other) const {
        return !(*this == other);
    }

private:
    std::string_view _topic_name;
};

std::ostream& operator<<(std::ostream&, const topic_view&);

struct topic {
    topic() noexcept = default;
    explicit topic(sstring topic_name) noexcept
      : name(std::move(topic_name)) {
    }

    topic_view view() const {
        return topic_view(name);
    }

    bool operator==(const topic& other) const {
        return name == other.name;
    }

    bool operator!=(const topic& other) const {
        return !(*this == other);
    }

    const sstring name;
};

std::ostream& operator<<(std::ostream&, const topic&);

/// \brief namespace is reserved in c++;  use ns
struct ns {
    ns() noexcept = default;
    explicit ns(sstring namespace_name) noexcept
      : name(std::move(namespace_name)) {
    }

    bool operator==(const ns& other) const {
        return name == other.name;
    }

    bool operator!=(const ns& other) const {
        return !(*this == other);
    }

    const sstring name;
};

std::ostream& operator<<(std::ostream&, const ns&);

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
    offset(const type&& t)
      : named_type<uint64_t, struct model_offset_type>(t) {
    }
    type value() const {
        return *this;
    }

    /// \brief used by the kafka _signed_ integer api
    offset operator+(int32_t val) const {
        return _value + static_cast<type>(val);
    }
    /// \brief used by the kafka _signed_ integer api
    offset operator+(int64_t val) const {
        return _value + static_cast<type>(val);
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
        boost::hash_combine(h, hash<sstring>()(ntp.ns.name));
        boost::hash_combine(h, hash<sstring>()(ntp.tp.topic.name));
        boost::hash_combine(h, hash<model::partition_id>()(ntp.tp.partition));
        return h;
    }
};

} // namespace std
