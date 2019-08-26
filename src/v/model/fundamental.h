#pragma once

#include "seastarx.h"

#include <seastar/core/sstring.hh>

#include <cstdint>
#include <limits>
#include <string_view>

namespace model {

struct partition {
    using type = int32_t;

    static constexpr const type min = std::numeric_limits<type>::min();

    partition() noexcept = default;

    constexpr explicit partition(type id) noexcept
      : value(id) {
    }

    bool operator==(partition other) const {
        return value == other.value;
    }

    bool operator!=(partition other) const {
        return !(*this == other);
    }

    const type value = min;
};

std::ostream& operator<<(std::ostream&, partition);

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
    model::partition partition;

    bool operator==(const topic_partition& other) const {
        return topic == other.topic && partition == other.partition;
    }

    bool operator!=(const topic_partition& other) const {
        return !(*this == other);
    }
};

std::ostream& operator<<(std::ostream&, topic_partition);

struct namespaced_topic_partition {
    model::ns ns;
    topic_partition tp;

    bool operator==(const namespaced_topic_partition& other) const {
        return ns == other.ns && tp == other.tp;
    }

    bool operator!=(const namespaced_topic_partition& other) const {
        return !(*this == other);
    }
};

std::ostream& operator<<(std::ostream&, namespaced_topic_partition);

class offset {
public:
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
    size_t operator()(model::namespaced_topic_partition ntp) const {
        return hash<sstring>()(ntp.ns.name) ^ hash<sstring>()(ntp.tp.topic.name)
               ^ hash<model::partition::type>()(ntp.tp.partition.value);
    }
};

} // namespace std
