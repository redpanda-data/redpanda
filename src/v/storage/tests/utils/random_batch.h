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

#include <seastar/core/circular_buffer.hh>

namespace storage::test {
/**
 * Makes random batch starting at requested offset.
 *
 * Note: it can create batches with timestamps from the past.
 */
model::record_batch make_random_batch(
  model::offset o,
  int num_records,
  bool allow_compression,
  model::record_batch_type bt = model::record_batch_type(1));

model::record_batch
make_random_batch(model::offset o, bool allow_compression = true);

ss::circular_buffer<model::record_batch>
make_random_batches(model::offset o, int count, bool allow_compression = true);

ss::circular_buffer<model::record_batch>
make_random_batches(model::offset o = model::offset(0));

} // namespace storage::test
