#pragma once

#include "model/fundamental.h"
#include "seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <boost/functional/hash.hpp>

#include <optional>
#include <unordered_set>
#include <vector>

namespace model {
using node_id = named_type<int32_t, struct node_id_model_type>;

class broker {
public:
    broker(
      node_id id,
      sstring host,
      int32_t port,
      std::optional<sstring> rack) noexcept
      : _id(std::move(id))
      , _host(std::move(host))
      , _port(port)
      , _rack(rack) {
    }
    broker(broker&&) noexcept = default;
    broker& operator=(broker&&) noexcept = default;
    const node_id& id() const {
        return _id;
    }

    const sstring& host() const {
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

struct partition_metadata {
    partition_metadata() noexcept = default;
    partition_metadata(partition_id p) noexcept
      : id(std::move(p)) {
    }
    partition_id id;
};

struct topic_metadata {
    topic_metadata(topic_view v) noexcept
      : topic(std::move(v)) {
    }
    topic_view topic;
    std::vector<partition_metadata> partitions;
};

namespace internal {
struct hash_by_topic_name {
    size_t operator()(const topic_metadata& tm) const {
        return std::hash<std::string_view>()(tm.topic());
    }
};

struct equals_by_topic_name {
    bool
    operator()(const topic_metadata& tm1, const topic_metadata& tm2) const {
        return tm1.topic == tm2.topic;
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
struct hash<model::broker> {
    size_t operator()(const model::broker& b) const {
        size_t h = 0;
        boost::hash_combine(h, std::hash<model::node_id>()(b.id()));
        boost::hash_combine(h, std::hash<seastar::sstring>()(b.host()));
        boost::hash_combine(h, std::hash<int32_t>()(b.port()));
        boost::hash_combine(
          h, std::hash<std::optional<seastar::sstring>>()(b.rack()));
        return h;
    }
};

} // namespace std
