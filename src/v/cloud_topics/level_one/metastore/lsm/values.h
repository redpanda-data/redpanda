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

#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/state.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "serde/envelope.h"

namespace cloud_topics::l1 {

struct metadata_row_value
  : public serde::envelope<
      metadata_row_value,
      serde::version<1>,
      serde::compat_version<0>> {
    auto serde_fields() {
        return std::tie(
          start_offset, next_offset, compaction_epoch, size, num_extents);
    }
    kafka::offset start_offset{};
    kafka::offset next_offset{};
    partition_state::compaction_epoch_t compaction_epoch{0};
    // Partition's size in bytes, updated incrementally as extents are
    // added/removed.
    size_t size{0};
    // Number of extents in the partition, updated incrementally.
    size_t num_extents{0};
};

struct extent_row_value
  : public serde::
      envelope<extent_row_value, serde::version<0>, serde::compat_version<0>> {
    auto serde_fields() {
        return std::tie(last_offset, max_timestamp, filepos, len, oid);
    }
    kafka::offset last_offset;
    model::timestamp max_timestamp;
    size_t filepos{0};
    size_t len{0};
    object_id oid{};
};

struct term_row_value
  : public serde::
      envelope<term_row_value, serde::version<0>, serde::compat_version<0>> {
    auto serde_fields() { return std::tie(term_start_offset); }
    kafka::offset term_start_offset{};
};

struct compaction_row_value
  : public serde::envelope<
      compaction_row_value,
      serde::version<0>,
      serde::compat_version<0>> {
    auto serde_fields() { return std::tie(state); }
    compaction_state state{};
};

struct object_row_value
  : public serde::
      envelope<object_row_value, serde::version<0>, serde::compat_version<0>> {
    auto serde_fields() { return std::tie(object); }
    object_entry object{};
};

} // namespace cloud_topics::l1
