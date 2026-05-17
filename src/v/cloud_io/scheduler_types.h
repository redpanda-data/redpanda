/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/format_to.h"

#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace cloud_io {

/// Identifies a cloud_io::scheduler admission policy.
enum class policy_type : uint8_t {
    /// No-op admission gate; the client pool's capacity is the only
    /// constraint.
    passthrough,
};

constexpr std::string_view to_string_view(policy_type t) {
    switch (t) {
    case policy_type::passthrough:
        return "passthrough";
    }
    std::unreachable();
}

inline fmt::iterator format_to(policy_type t, fmt::iterator out) {
    return fmt::format_to(out, "{}", to_string_view(t));
}

/// Caller-supplied intent label for a cloud_io operation. Used as the
/// scheduling key for cloud_io::scheduler.
enum class group_id : uint8_t {
    /// Object uploads on the Kafka produce path. Latency-critical.
    producer_upload,
    /// Reads serving Kafka fetch requests. Latency-critical.
    consumer_fetch,
    /// Everything else: manifest I/O, archival writes, hydration,
    /// replication, housekeeping.
    default_group,
};

inline constexpr size_t num_group_ids = 3;

constexpr std::string_view to_string_view(group_id g) {
    switch (g) {
    case group_id::producer_upload:
        return "producer_upload";
    case group_id::consumer_fetch:
        return "consumer_fetch";
    case group_id::default_group:
        return "default_group";
    }
    std::unreachable();
}

inline fmt::iterator format_to(group_id g, fmt::iterator out) {
    return fmt::format_to(out, "{}", to_string_view(g));
}

namespace detail {
template<size_t... Is>
constexpr std::array<group_id, num_group_ids>
make_all_group_ids(std::index_sequence<Is...>) {
    return {static_cast<group_id>(Is)...};
}
} // namespace detail

/// All group_id values in enum order. Bounded by num_group_ids; when
/// adding a group_id enumerator, update num_group_ids to match — the
/// static_assert below catches the most common slip.
inline constexpr auto all_group_ids = detail::make_all_group_ids(
  std::make_index_sequence<num_group_ids>{});

static_assert(
  std::to_underlying(group_id::default_group) + 1 == num_group_ids,
  "num_group_ids must equal the number of group_id enumerators; "
  "update both when adding or removing a group.");

/// Fixed-size array of T indexed by group_id.
template<typename T>
struct per_group {
    std::array<T, num_group_ids> data;

    T& operator[](group_id g) noexcept { return data[static_cast<size_t>(g)]; }
    const T& operator[](group_id g) const noexcept {
        return data[static_cast<size_t>(g)];
    }

    auto begin() noexcept { return data.begin(); }
    auto end() noexcept { return data.end(); }
    auto begin() const noexcept { return data.begin(); }
    auto end() const noexcept { return data.end(); }
};

/// Runtime configuration for cloud_io::scheduler. Populated at startup
/// from cluster config (see cloud_storage::configuration::get_config)
/// and passed by value down to client_pool / scheduler.
struct scheduler_config {
    policy_type policy = policy_type::passthrough;
};

} // namespace cloud_io
