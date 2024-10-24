/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "bytes/random.h"
#include "cloud_topics/reconciler/range_batch_consumer.h"
#include "model/record_batch_reader.h"
#include "storage/record_batch_builder.h"

#include <gtest/gtest.h>

using consumer = experimental::cloud_topics::reconciler::range_batch_consumer;

model::record_batch_reader make_reader(
  int offset, int record_size, int num_batches, int records_per_batch) {
    ss::chunked_fifo<model::record_batch> batches;
    for (int i = 0; i < num_batches; i++) {
        storage::record_batch_builder b(
          model::record_batch_type::raft_data, model::offset(offset));
        for (int j = 0; j < records_per_batch; j++) {
            b.add_raw_kv(random_generators::make_iobuf(record_size), iobuf());
            offset += 1;
        }
        batches.push_back(std::move(b).build());
    }
    return model::make_fragmented_memory_record_batch_reader(
      std::move(batches));
}

TEST(RangeBatchConsumer, EmptyReader) {
    auto reader = model::make_empty_record_batch_reader();
    auto range = std::move(reader).consume(consumer{}, model::no_timeout).get();
    ASSERT_FALSE(range.has_value());
}

TEST(RangeBatchConsumer, MultipleBatchesRecords) {
    size_t record_size = 100;
    size_t base_offset = 11;
    for (int i = 1; i < 4; i++) {
        for (int j = 1; j < 2; j++) {
            auto reader = make_reader(base_offset, record_size, i, j);
            auto range
              = std::move(reader).consume(consumer{}, model::no_timeout).get();
            ASSERT_TRUE(range.has_value());
            ASSERT_EQ(range->info.base_offset(), base_offset);
            ASSERT_EQ(range->info.last_offset(), base_offset + (i * j) - 1);
        }
    }
}
