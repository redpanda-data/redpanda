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

#include "base/seastarx.h"
#include "random/generators.h"
#include "serde/envelope.h"
#include "utils/named_type.h"
#include "utils/uuid.h"

#include <seastar/util/bool_class.hh>

#include <fmt/core.h>

#include <cstdint>

namespace cloud_topics {

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
/// of a unique name (UUIDv4), a cluster epoch, and a random 3-digit numeric
/// prefix.
struct object_id
  : serde::envelope<object_id, serde::version<1>, serde::compat_version<0>> {
    cluster_epoch epoch;
    uuid_t name;
    using prefix_t = uint16_t;
    prefix_t prefix;
    static object_id create(cluster_epoch epoch) {
        return {
          .epoch = epoch,
          .name = uuid_t::create(),
          .prefix = random_generators::get_int<prefix_t>(0, prefix_max)};
    }
    auto serde_fields() { return std::tie(epoch, name, prefix); }
    bool operator==(const object_id& other) const = default;
    auto operator<=>(const object_id& other) const = default;

    template<typename H>
    friend H AbslHashValue(H h, const object_id& id) {
        return H::combine(std::move(h), id.epoch(), id.name, id.prefix);
    }

    static constexpr prefix_t prefix_max = 999;
};

/// Type of ownership
enum class ctp_stm_object_ownership {
    exclusive = 0,
    shared = 1,
};

using allow_materialization_failure
  = ss::bool_class<struct allow_materialization_failure_tag>;

} // namespace cloud_topics

template<>
struct fmt::formatter<cloud_topics::object_id>
  : fmt::formatter<std::string_view> {
    auto format(const cloud_topics::object_id&, fmt::format_context& ctx) const
      -> decltype(ctx.out());
};

template<>
struct fmt::formatter<cloud_topics::ctp_stm_object_ownership>
  : fmt::formatter<std::string_view> {
    auto format(
      cloud_topics::ctp_stm_object_ownership, fmt::format_context& ctx) const
      -> decltype(ctx.out());
};
