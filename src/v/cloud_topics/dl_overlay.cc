// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/dl_overlay.h"

auto fmt::formatter<experimental::cloud_topics::dl_overlay_object>::format(
  const experimental::cloud_topics::dl_overlay_object& o,
  fmt::format_context& ctx) const -> decltype(ctx.out()) {
    return formatter<std::string_view>::format(
      fmt::format(
        "{{id:{}, first_byte_offset:{}, byte_range_size:{}, ownership:{}}}",
        o.id,
        o.first_byte_offset,
        o.byte_range_size,
        o.ownership),
      ctx);
}

auto fmt::formatter<experimental::cloud_topics::dl_overlay>::format(
  const experimental::cloud_topics::dl_overlay& o,
  fmt::format_context& ctx) const -> decltype(ctx.out()) {
    return formatter<std::string_view>::format(
      fmt::format(
        "{{base_offset:{}, last_offset:{}, base_timestamp:{}, "
        "last_timestamp:{}, last_offset_by_term.size():{}, object:{}}}",
        o.base_offset,
        o.last_offset,
        o.base_timestamp,
        o.last_timestamp,
        o.last_offset_by_term.size(),
        o.object),
      ctx);
}

namespace experimental::cloud_topics {

std::ostream& operator<<(std::ostream& os, const dl_overlay& dl_overlay) {
    fmt::print(os, "{}", dl_overlay);
    return os;
}

} // namespace experimental::cloud_topics
