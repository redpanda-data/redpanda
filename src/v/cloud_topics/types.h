/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "serde/envelope.h"
#include "utils/named_type.h"
#include "utils/uuid.h"

#include <fmt/core.h>

#include <cstdint>

namespace experimental::cloud_topics {

/// Offset in the cloud storage object
using first_byte_offset_t = named_type<uint64_t, struct first_byte_offset_tag>;

/// Size of the span in the cloud storage object in bytes
using byte_range_size_t = named_type<uint64_t, struct byte_range_size_tag>;

/// An epoch is a monotonically increasing value across the cluster.
using cluster_epoch = named_type<int64_t, struct cloud_topics_epoch>;

/// Return the previous cluster epoch value.
inline constexpr cluster_epoch prev_cluster_epoch(cluster_epoch e) {
    if (e <= cluster_epoch{0}) {
        return cluster_epoch::min();
    }
    return cluster_epoch(e() - 1);
}

/// Is the identifier of a cloud topic object L0 object, it is a combination
/// of a unique name (UUIDv4) and a cluster epoch.
struct object_id
  : serde::envelope<object_id, serde::version<0>, serde::version<0>> {
    cluster_epoch epoch;
    uuid_t name;
    static object_id create(cluster_epoch epoch) {
        return {.epoch = epoch, .name = uuid_t::create()};
    }
    auto serde_fields() { return std::tie(epoch, name); }
    bool operator==(const object_id& other) const = default;
    auto operator<=>(const object_id& other) const = default;

    template<typename H>
    friend H AbslHashValue(H h, const object_id& id) {
        return H::combine(std::move(h), id.epoch(), id.name);
    }
};

/// Type of ownership
enum class ctp_stm_object_ownership {
    exclusive = 0,
    shared = 1,
};

} // namespace experimental::cloud_topics

template<>
struct fmt::formatter<experimental::cloud_topics::object_id>
  : fmt::formatter<std::string_view> {
    auto format(
      const experimental::cloud_topics::object_id&,
      fmt::format_context& ctx) const -> decltype(ctx.out());
};

template<>
struct fmt::formatter<experimental::cloud_topics::ctp_stm_object_ownership>
  : fmt::formatter<std::string_view> {
    auto format(
      experimental::cloud_topics::ctp_stm_object_ownership,
      fmt::format_context& ctx) const -> decltype(ctx.out());
};
