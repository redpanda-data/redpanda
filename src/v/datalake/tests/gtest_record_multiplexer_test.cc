/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/record_multiplexer.h"
#include "datalake/tests/test_data_writer.h"
#include "model/tests/random_batch.h"

#include <gtest/gtest.h>

TEST(DatalakeMultiplexerTest, TestMultiplexer) {
    int record_count = 10;
    int batch_count = 10;
    auto writer_factory
      = std::make_unique<datalake::test_data_writer_factory>();
    datalake::record_multiplexer multiplexer(std::move(writer_factory));

    model::test::record_batch_spec batch_spec;
    batch_spec.records = record_count;
    batch_spec.count = batch_count;
    ss::circular_buffer<model::record_batch> batches
      = model::test::make_random_batches(batch_spec).get0();

    auto reader = model::make_generating_record_batch_reader(
      [batches = std::move(batches)]() mutable {
          return ss::make_ready_future<model::record_batch_reader::data_t>(
            std::move(batches));
      });

    auto result
      = reader.consume(std::move(multiplexer), model::no_timeout).get0();

    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0].row_count, record_count * batch_count);
}
