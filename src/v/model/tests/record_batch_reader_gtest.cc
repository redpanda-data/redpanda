// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "test_utils/test.h"

template<typename... Offsets>
chunked_circular_buffer<model::record_batch> make_batches(Offsets... o) {
    chunked_circular_buffer<model::record_batch> batches;
    (batches.emplace_back(
       model::test::make_random_batch(model::offset(o), 1, true)),
     ...);
    return batches;
}

template<typename Container>
auto copy_batches(const Container& batches) {
    Container copy;
    for (auto& batch : batches) {
        copy.push_back(batch.copy());
    }
    return copy;
}

TEST_CORO(RecordBatchReaderReadahead, BasicReadahead) {
    // Test that readahead reader produces same results as underlying reader
    auto batches = make_batches(10, 20, 30, 40, 50);
    auto r0 = make_memory_record_batch_reader(copy_batches(batches));
    auto r1 = model::make_readahead_record_batch_reader(
      make_memory_record_batch_reader(copy_batches(batches)));

    auto r0_materialized = co_await model::consume_reader_to_chunked_vector(
      std::move(r0), model::no_timeout);
    auto r1_materialized = co_await model::consume_reader_to_chunked_vector(
      std::move(r1), model::no_timeout);

    ASSERT_EQ_CORO(r0_materialized.size(), 5);
    ASSERT_EQ_CORO(r1_materialized.size(), r0_materialized.size());
    for (int i = 0; i < 5; ++i) {
        ASSERT_EQ_CORO(r1_materialized[i], r0_materialized[i]);
    }
}

TEST_CORO(RecordBatchReaderReadahead, EmptyReader) {
    // Test that readahead reader handles empty underlying reader
    auto reader = model::make_readahead_record_batch_reader(
      model::make_empty_record_batch_reader());

    auto materialized = co_await model::consume_reader_to_chunked_vector(
      std::move(reader), model::no_timeout);
    ASSERT_TRUE_CORO(materialized.empty());
}

TEST_CORO(RecordBatchReaderReadahead, SingleBatch) {
    // Test readahead with single batch
    auto batches = make_batches(100);
    auto reader = model::make_readahead_record_batch_reader(
      make_memory_record_batch_reader(copy_batches(batches)));

    auto materialized = co_await model::consume_reader_to_chunked_vector(
      std::move(reader), model::no_timeout);

    ASSERT_EQ_CORO(materialized.size(), 1);
    ASSERT_EQ_CORO(materialized[0].base_offset(), model::offset(100));
}
