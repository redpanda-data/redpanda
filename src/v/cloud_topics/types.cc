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

#include <fmt/core.h>

auto fmt::formatter<experimental::cloud_topics::dl_stm_key>::format(
  experimental::cloud_topics::dl_stm_key key,
  fmt::format_context& ctx) const -> decltype(ctx.out()) {
    switch (key) {
    case experimental::cloud_topics::dl_stm_key::push_overlay:
        return fmt::format_to(ctx.out(), "push_overlay");
    case experimental::cloud_topics::dl_stm_key::start_snapshot:
        return fmt::format_to(ctx.out(), "start_snapshot");
    case experimental::cloud_topics::dl_stm_key::
      remove_snapshots_before_version:
        return fmt::format_to(ctx.out(), "remove_snapshots_before_version");
    }
    return fmt::format_to(
      ctx.out(), "unknown dl_stm_key({})", static_cast<int>(key));
}

auto fmt::formatter<experimental::cloud_topics::object_id>::format(
  const experimental::cloud_topics::object_id& id,
  fmt::format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{}", id());
}

auto fmt::formatter<experimental::cloud_topics::dl_stm_object_ownership>::
  format(
    experimental::cloud_topics::dl_stm_object_ownership ownership,
    fmt::format_context& ctx) const -> decltype(ctx.out()) {
    switch (ownership) {
    case experimental::cloud_topics::dl_stm_object_ownership::exclusive:
        return fmt::format_to(ctx.out(), "exclusive");
    case experimental::cloud_topics::dl_stm_object_ownership::shared:
        return fmt::format_to(ctx.out(), "shared");
    }
}
