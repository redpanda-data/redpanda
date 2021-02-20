/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/record.h"
#include "model/record_batch_reader.h"
#include "random/generators.h"

namespace storage::test {
using namespace random_generators; // NOLINT

struct record_batch_spec {
    model::offset offset{0};
    bool allow_compression{true};
    int count{0};
    model::record_batch_type bt{1};
    bool enable_idempotence{false};
    int64_t producer_id{0};
    int16_t producer_epoch{0};
    int32_t base_sequence{0};
};

/**
 * Makes random batch starting at requested offset.
 *
 * Note: it can create batches with timestamps from the past.
 */
model::record_batch make_random_batch(
  model::offset o,
  int num_records,
  bool allow_compression,
  model::record_batch_type bt);

model::record_batch
make_random_batch(model::offset o, int num_records, bool allow_compression);

model::record_batch make_random_batch(record_batch_spec);

model::record_batch
make_random_batch(model::offset o, bool allow_compression = true);

ss::circular_buffer<model::record_batch>
make_random_batches(model::offset o, int count, bool allow_compression = true);

ss::circular_buffer<model::record_batch>
make_random_batches(model::offset o = model::offset(0));

ss::circular_buffer<model::record_batch>
make_random_batches(record_batch_spec spec);

model::record_batch_reader make_random_memory_record_batch_reader(
  model::offset, int, int, bool allow_compression = true);

model::record_batch_reader
make_random_memory_record_batch_reader(record_batch_spec, int);

} // namespace storage::test
