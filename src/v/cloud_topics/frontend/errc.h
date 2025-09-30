/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include <fmt/format.h>

namespace cloud_topics {
// Frontend error codes
enum class frontend_errc : uint8_t {
    offset_not_available,
    invalid_topic_exception,
    not_leader_for_partition,
    offset_out_of_range,
    timeout,
};
} // namespace cloud_topics
template<>
struct fmt::formatter<cloud_topics::frontend_errc>
  : fmt::formatter<std::string_view> {
    auto
    format(const cloud_topics::frontend_errc&, fmt::format_context& ctx) const
      -> decltype(ctx.out());
};
