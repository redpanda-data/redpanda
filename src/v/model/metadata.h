#pragma once

#include "model/fundamental.h"
#include "seastarx.h"

#include <seastar/core/sstring.hh>

#include <optional>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace model {

class node_id {
public:
    constexpr explicit node_id(int32_t value) noexcept
      : _value(value) {
    }

    int32_t value() const {
        return _value;
    }

private:
    int32_t _value;
};

class broker {
public:
    broker(
      node_id id,
      sstring host,
      int32_t port,
      std::optional<sstring> rack) noexcept
      : _id(id)
      , _host(std::move(host))
      , _port(port)
      , _rack(rack) {
    }

    node_id id() const {
        return _id;
    }

    const sstring host() const {
        return _host;
    }

    int32_t port() const {
        return _port;
    }

    const std::optional<sstring>& rack() const {
        return _rack;
    }

private:
    node_id _id;
    sstring _host;
    int32_t _port;
    std::optional<sstring> _rack;
};

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

class topic {
public:
    explicit topic(sstring topic_name) noexcept
      : _topic_name(std::move(topic_name)) {
    }

    const sstring& name() const {
        return _topic_name;
    }

    topic_view view() const {
        return topic_view(_topic_name);
    }

    bool operator==(const topic& other) const {
        return _topic_name == other._topic_name;
    }

    bool operator!=(const topic& other) const {
        return !(*this == other);
    }

private:
    sstring _topic_name;
};

struct partition_metadata {
    partition_type partition;
};

struct topic_metadata {
    topic_view topic;
    std::vector<partition_metadata> partitions;
};

namespace internal {
struct hash_by_topic_name {
    size_t operator()(const topic_metadata& tm) const {
        return std::hash<std::string_view>()(tm.topic.name());
    }
};

struct equals_by_topic_name {
    bool
    operator()(const topic_metadata& tm1, const topic_metadata& tm2) const {
        return tm1.topic.name() == tm2.topic.name();
    }
};

} // namespace internal

using topic_metadata_map = std::unordered_set<
  topic_metadata,
  internal::hash_by_topic_name,
  internal::equals_by_topic_name>;

} // namespace model

namespace std {

template<>
struct hash<model::topic_view> {
    size_t operator()(model::topic_view v) const {
        return hash<std::string_view>()(v.name());
    }
};

template<>
struct hash<model::topic> {
    size_t operator()(model::topic t) const {
        return hash<sstring>()(t.name());
    }
};

} // namespace std
