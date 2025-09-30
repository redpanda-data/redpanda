/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/frontend/errc.h"

auto fmt::formatter<cloud_topics::frontend_errc>::format(
  const cloud_topics::frontend_errc& err, fmt::format_context& ctx) const
  -> decltype(ctx.out()) {
    using enum cloud_topics::frontend_errc;
    switch (err) {
    case offset_not_available:
        return fmt::format_to(
          ctx.out(), "cloud_topics::frontend_errc::offset_not_available");
    case invalid_topic_exception:
        return fmt::format_to(
          ctx.out(), "cloud_topics::frontend_errc::invalid_topic_exception");
    case not_leader_for_partition:
        return fmt::format_to(
          ctx.out(), "cloud_topics::frontend_errc::not_leader_for_partition");
    case offset_out_of_range:
        return fmt::format_to(
          ctx.out(), "cloud_topics::frontend_errc::offset_out_of_range");
    case timeout:
        return fmt::format_to(
          ctx.out(), "cloud_topics::frontend_errc::timeout");
    }
}
