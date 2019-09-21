#pragma once

#include "seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <boost/container_hash/hash.hpp>

#include <cstdint>
#include <limits>
#include <string_view>

namespace model {

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

std::ostream& operator<<(std::ostream&, topic_view);

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

struct namespaced_topic_partition {
    model::ns ns;
    topic_partition tp;

    bool operator==(const namespaced_topic_partition& other) const {
        return ns == other.ns && tp == other.tp;
    }

    bool operator!=(const namespaced_topic_partition& other) const {
        return !(*this == other);
    }

    sstring path() const;
};

std::ostream& operator<<(std::ostream&, const namespaced_topic_partition&);

class offset {
public:
    using type = uint64_t;
    offset() noexcept = default;

    explicit offset(uint64_t val) noexcept
      : _val(val) {
    }

    uint64_t value() const {
        return _val;
    }

    offset operator+(int64_t val) const {
        return offset(_val + val);
    }

    offset& operator+=(int64_t val) {
        _val += val;
        return *this;
    }

    bool operator<(const offset& other) const {
        return _val < other._val;
    }

    bool operator<=(const offset& other) const {
        return _val <= other._val;
    }

    bool operator>(const offset& other) const {
        return other._val < _val;
    }

    bool operator>=(const offset& other) const {
        return other._val <= _val;
    }

    bool operator==(const offset& other) const {
        return _val == other._val;
    }

    bool operator!=(const offset& other) const {
        return !(*this == other);
    }

private:
    uint64_t _val = 0;
};

std::ostream& operator<<(std::ostream&, offset);

} // namespace model

namespace std {

template<>
struct hash<model::namespaced_topic_partition> {
    size_t operator()(const model::namespaced_topic_partition& ntp) const {
        size_t h = 0;
        boost::hash_combine(h, hash<sstring>()(ntp.ns.name));
        boost::hash_combine(h, hash<sstring>()(ntp.tp.topic.name));
        boost::hash_combine(h, hash<model::partition_id>()(ntp.tp.partition));
        return h;
    }
};

} // namespace std
