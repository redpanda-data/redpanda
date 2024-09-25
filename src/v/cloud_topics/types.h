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

#include "utils/named_type.h"
#include "utils/uuid.h"

#include <fmt/core.h>

#include <cstdint>

namespace experimental::cloud_topics {

enum class dl_stm_key {
    overlay,
    // TODO: add all commands
};

/// Offset in the cloud storage object
using first_byte_offset_t = named_type<uint64_t, struct first_byte_offset_tag>;

/// Size of the span in the cloud storage object in bytes
using byte_range_size_t = named_type<uint64_t, struct byte_range_size_tag>;

/// 128-bit unique id of the object
using object_id = named_type<uuid_t, struct object_id_tag>;

/// Type of ownership
enum class dl_stm_object_ownership {
    exclusive,
    shared,
};

} // namespace experimental::cloud_topics

template<>
struct fmt::formatter<experimental::cloud_topics::dl_stm_key>
  : fmt::formatter<std::string_view> {
    auto format(
      experimental::cloud_topics::dl_stm_key,
      fmt::format_context& ctx) const -> decltype(ctx.out());
};
