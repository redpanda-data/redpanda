/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/object_utils.h"

#include "base/vassert.h"
#include "ssx/sformat.h"

#include <charconv>

constexpr auto level_zero_data_dir_str = "level_zero/data/";

/*
 * We are using full-width int64 padding here. Using that many digits means
 * we don't need to think at all about the choice in terms of future growth.
 * We can revisit the width if for some reason we want a lower limit.
 */
constexpr size_t epoch_digits = 18;
static_assert(std::numeric_limits<int64_t>::digits10 == epoch_digits);

constexpr size_t prefix_digits = 3;

namespace cloud_topics {

cloud_storage_clients::object_key
object_path_factory::level_zero_path(object_id id) {
    vassert(id.epoch() >= 0, "level zero object has negative epoch: {}", id);
    return cloud_storage_clients::object_key(
      ssx::sformat(
        "{0}{5:0{4}}/{2:0{1}}/{3}",
        level_zero_data_dir_str,
        epoch_digits,
        id.epoch(),
        id.name,
        prefix_digits,
        id.prefix));
}

cloud_storage_clients::object_key object_path_factory::level_zero_data_dir() {
    return cloud_storage_clients::object_key(level_zero_data_dir_str);
}

std::expected<cluster_epoch, std::string>
object_path_factory::level_zero_path_to_epoch(std::string_view key) {
    // find the level zero prefix and chop it off
    auto name = key;
    auto it = name.find(level_zero_data_dir_str);
    if (it == std::string_view::npos) {
        return std::unexpected(
          fmt::format("L0 object name missing prefix: {}", key));
    }
    name.remove_prefix(it + std::strlen(level_zero_data_dir_str));

    if (name.size() < prefix_digits + 1) {
        return std::unexpected(
          fmt::format("L0 object name is too short: {}", key));
    }
    name.remove_prefix(prefix_digits + 1);

    if (name.size() < epoch_digits) {
        return std::unexpected(
          fmt::format("L0 object name is too short: {}", key));
    }

    // remove the tail so that all should be left is the epoch
    name.remove_suffix(name.size() - epoch_digits);

    // parse the epoch into an integer
    int64_t epoch{0};
    auto res = std::from_chars(name.data(), name.data() + name.size(), epoch);
    if (res.ptr != name.data() + name.size() || res.ec != std::errc{}) {
        return std::unexpected(
          fmt::format("L0 object name has invalid epoch: {}", key));
    }

    return cluster_epoch(epoch);
}

std::expected<object_id::prefix_t, std::string>
object_path_factory::level_zero_path_to_prefix(std::string_view key) {
    // find the level zero prefix and chop it off
    auto name = key;
    auto it = name.find(level_zero_data_dir_str);
    if (it == std::string_view::npos) {
        return std::unexpected(
          fmt::format("L0 object name missing prefix: {}", key));
    }
    name.remove_prefix(it + std::strlen(level_zero_data_dir_str));

    if (name.size() < prefix_digits + 1) {
        return std::unexpected(
          fmt::format("L0 object name is too short: {}", key));
    }
    name.remove_suffix(name.size() - prefix_digits);

    // parse the prefix into a uint16_t
    object_id::prefix_t pfx{0};
    auto res = std::from_chars(name.data(), name.data() + name.size(), pfx);
    if (res.ptr != name.data() + name.size() || res.ec != std::errc{}) {
        return std::unexpected(
          fmt::format("L0 object name has invalid prefix: {}", key));
    }

    return pfx;
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
prefix_range_inclusive::prefix_range_inclusive(T min, T max)
  : min(min)
  , max(max) {
    vassert(max <= t_max, "prefix_range: Invalid max: {}", max);
}

bool prefix_range_inclusive::contains(T v) const {
    return v >= min && v <= max;
}

bool prefix_range_inclusive::operator==(
  const prefix_range_inclusive& other) const {
    return min == other.min && max == other.max;
}

fmt::iterator prefix_range_inclusive::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "[{},{}]", min, max);
}

} // namespace cloud_topics
