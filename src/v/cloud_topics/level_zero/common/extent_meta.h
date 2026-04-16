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

#include "cloud_topics/types.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"

#include <fmt/core.h>

namespace cloud_topics {

// This struct contains extent information (location of the byte slice in the
// object + the object uuid) and the kafka metadata (base offset and committed
// offset). It can be used to reference individual record batch or the continuos
// run that belongs to the same partition.
//
// Timestamps are not here because the timequery is handled in the metadata
// layer and the results of this are converted to kafka offsets.
//
// The type is generic and is supposed to work with both L0 and L1. When we will
// settle on the object name format for both L0 and L1 it will have to be
// updated to reflect this.
//
// The kafka offsets are not strictly necessary but they are used by the L0 read
// path. Without them the read path will end up being more complicated because
// the offsets will have to be written into the header after they're fetched.
// Putting this fields into the extent_meta allows data layer to put correct
// offsets into the headers before they are returned to the caller.
struct extent_meta {
    // Extent information
    object_id id;
    // TODO: the extent meta struct has to be updated
    // to match the RFC.
    first_byte_offset_t first_byte_offset;
    byte_range_size_t byte_range_size;

    // Kafka metadata
    kafka::offset base_offset;
    kafka::offset last_offset;
};

// The result of a successful upload of data in level zero.
struct upload_meta {
    // The shard that uploaded the data
    ss::shard_id shard;
    chunked_vector<extent_meta> extents;
};

} // namespace cloud_topics

template<>
struct fmt::formatter<cloud_topics::extent_meta>
  : fmt::formatter<std::string_view> {
    template<class Context>
    constexpr auto
    format(const cloud_topics::extent_meta& o, Context& ctx) const {
        return format_to(
          ctx.out(),
          "{{id:{}, first_byte_offset:{}, byte_range_size:{}, "
          "base_offset:{}, committed_offset:{}}}",
          o.id,
          o.first_byte_offset,
          o.byte_range_size,
          o.base_offset,
          o.last_offset);
    }
};
