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
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "storage/record_batch_builder.h"

#include <absl/container/btree_map.h>

namespace experimental::cloud_topics {

class command_builder {
public:
    command_builder();

    void add_overlay_batch(
      object_id id,
      first_byte_offset_t offset,
      byte_range_size_t size_bytes,
      dl_stm_object_ownership ownership,
      kafka::offset base_offset,
      kafka::offset last_offset,
      model::timestamp base_ts,
      model::timestamp last_ts,
      absl::btree_map<model::term_id, kafka::offset> terms = {});

    // TODO: add fencing mechanism
    // for PoC assume that we're running one instance of the STM
    // per partition/broker and persisted_stm<>::sync() is called
    // after the leadership is acquired.

    model::record_batch build() &&;

private:
    storage::record_batch_builder _builder;
};
} // namespace experimental::cloud_topics
