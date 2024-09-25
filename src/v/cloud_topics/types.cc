/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/types.h"

auto fmt::formatter<experimental::cloud_topics::dl_stm_key>::format(
  experimental::cloud_topics::dl_stm_key key,
  fmt::format_context& ctx) const -> decltype(ctx.out()) {
    switch (key) {
    case experimental::cloud_topics::dl_stm_key::overlay:
        return fmt::format_to(ctx.out(), "overlay");
    default:
        return fmt::format_to(ctx.out(), "unknown");
    }
}
