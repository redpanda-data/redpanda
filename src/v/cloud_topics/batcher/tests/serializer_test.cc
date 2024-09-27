/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/batcher/serializer.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "model/timeout_clock.h"
#include "random/generators.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/manual_clock.hh>

#include <gtest/gtest.h>

#include <chrono>

namespace cloud_topics = experimental::cloud_topics;

model::record_batch_reader
get_random_batches(int num_batches, int num_records_per_batch) { // NOLINT
    ss::circular_buffer<model::record_batch> batches;
    model::offset o{0};
    for (int ix_batch = 0; ix_batch < num_batches; ix_batch++) {
        auto batch = model::test::make_random_batch(
          o, num_records_per_batch, false);
        o = model::next_offset(batch.last_offset());
        batches.push_back(std::move(batch));
    }
    return model::make_memory_record_batch_reader(std::move(batches));
}

TEST(serializer, consume) {
    auto test_data = get_random_batches(1000, 1);
    // The function returns a ready future because the reader is not actually
    // asynchronous.
    auto res = cloud_topics::details::serialize_in_memory_record_batch_reader(
                 std::move(test_data))
                 .get();
    ASSERT_TRUE(res.payload.size_bytes() > 0);
    ASSERT_TRUE(res.batches.size() == 1000);
    ASSERT_TRUE(
      res.batches.back().physical_offset + res.batches.back().size_bytes
      == res.payload.size_bytes());
}
