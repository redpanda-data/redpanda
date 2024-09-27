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

#include "cloud_topics/types.h"
#include "serde/envelope.h"
#include "serde/rw/named_type.h"
#include "serde/rw/uuid.h"

// This header contains definition of the dl_placeholder batch

namespace experimental::cloud_topics {

struct dl_placeholder // NOLINT
  : serde::
      envelope<dl_placeholder, serde::version<0>, serde::compat_version<0>> {
    // unique object id
    object_id id;
    // byte range
    first_byte_offset_t offset;
    byte_range_size_t size_bytes;

    auto serde_fields() { return std::tie(id, offset, size_bytes); }
};

enum class dl_placeholder_record_key {
    // The record contains metadata
    payload,
    // The record is used to align with raft_data batch
    empty,
};

} // namespace experimental::cloud_topics
