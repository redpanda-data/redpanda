/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "utils/to_string.h"

fmt::format_context::iterator fmt::formatter<std::monostate>::format(
  const std::monostate&, format_context& ctx) {
    return fmt::format_to(ctx.out(), "{{}}");
}

fmt::format_context::iterator fmt::formatter<absl::Duration>::format(
  const absl::Duration& d, fmt::format_context& ctx) {
    std::string s = absl::FormatDuration(d);
    return fmt::format_to(ctx.out(), "{}", s);
}

fmt::format_context::iterator fmt::formatter<absl::Time>::format(
  const absl::Time& t, fmt::format_context& ctx) {
    std::string s = absl::FormatTime(t);
    return fmt::format_to(ctx.out(), "{}", s);
}
