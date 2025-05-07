// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cloud_topics/types.h"
#include "model/fundamental.h"

#include <fmt/core.h>

namespace experimental::cloud_topics {

// This struct contains extent information (location of the byte slice in the
// object + the object uuid) and the kafka metadata (base offset and committed
// offset).
// Timestamps are not here because the timequery is handled in the control plane
// and the results of this are converted to kafka offsets.
struct extent_meta {
    // Extent information
    object_id id;
    first_byte_offset_t first_byte_offset;
    byte_range_size_t byte_range_size;

    // Kafka metadata
    kafka::offset base_offset;
    kafka::offset committed_offset;
};

} // namespace experimental::cloud_topics

template<>
struct fmt::formatter<experimental::cloud_topics::extent_meta>
  : fmt::formatter<std::string_view> {
    template<class Context>
    constexpr auto format(
      const experimental::cloud_topics::extent_meta& o, Context& ctx) const {
        return format_to(
          ctx.out(),
          "{{id:{}, first_byte_offset:{}, byte_range_size:{}, "
          "base_offset:{}, committed_offset:{}}}",
          o.id,
          o.first_byte_offset,
          o.byte_range_size,
          o.base_offset,
          o.committed_offset);
    }
};
