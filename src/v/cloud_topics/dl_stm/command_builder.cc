/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/dl_stm/command_builder.h"

#include "cloud_topics/dl_stm/commands.h"
#include "model/record_batch_types.h"
#include "serde/rw/map.h"
#include "serde/rw/rw.h"
#include "serde/rw/uuid.h"

namespace experimental::cloud_topics {

command_builder::command_builder()
  : _builder(
      model::record_batch_type::version_fence /*TODO: replace*/,
      model::offset(0)) {}

void command_builder::add_overlay_batch(
  object_id id,
  first_byte_offset_t offset,
  byte_range_size_t size_bytes,
  dl_stm_object_ownership ownership,
  kafka::offset base_offset,
  kafka::offset last_offset,
  model::timestamp base_ts,
  model::timestamp last_ts,
  absl::btree_map<model::term_id, kafka::offset> terms) {
    dl_overlay payload{
      .base_offset = base_offset,
      .last_offset = last_offset,
      .base_ts = base_ts,
      .last_ts = last_ts,
      .terms = std::move(terms),
      .id = id,
      .ownership = ownership,
      .offset = offset,
      .size_bytes = size_bytes,
    };
    _builder.add_raw_kv(
      serde::to_iobuf(dl_stm_key::overlay),
      serde::to_iobuf(std::move(payload)));
}

model::record_batch command_builder::build() && {
    return std::move(_builder).build();
}
} // namespace experimental::cloud_topics
