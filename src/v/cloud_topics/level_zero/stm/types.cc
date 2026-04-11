/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/stm/types.h"

#include <fmt/core.h>

auto fmt::formatter<cloud_topics::ctp_stm_key>::format(
  cloud_topics::ctp_stm_key key, fmt::format_context& ctx) const
  -> decltype(ctx.out()) {
    switch (key) {
    case cloud_topics::ctp_stm_key::advance_reconciled_offset:
        return fmt::format_to(ctx.out(), "advance_reconciled_offset");
    case cloud_topics::ctp_stm_key::set_start_offset:
        return fmt::format_to(ctx.out(), "set_start_offset");
    case cloud_topics::ctp_stm_key::advance_epoch:
        return fmt::format_to(ctx.out(), "advance_epoch");
    case cloud_topics::ctp_stm_key::reset_state:
        return fmt::format_to(ctx.out(), "reset_state");
    }
    return fmt::format_to(
      ctx.out(), "unknown ctp_stm_key({})", static_cast<int>(key));
}
