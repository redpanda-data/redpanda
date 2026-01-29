// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "gmock/gmock.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "test_utils/test.h"

#include <gtest/gtest-matchers.h>

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

TEST_CORO(RecordBatchReaderGenerator, EmptyReader) {
    auto reader = model::make_empty_record_batch_reader();
    auto gen = std::move(reader).generator(model::no_timeout);
    while (auto batch = co_await gen()) {
        ASSERT_TRUE_CORO(false) << "No batches expected";
    }
    auto slice_gen = model::make_empty_record_batch_reader().slice_generator(
      model::no_timeout);
    while (auto batch = co_await slice_gen()) {
        ASSERT_TRUE_CORO(false) << "No batches expected";
    }
}

TEST_CORO(RecordBatchReaderGenerator, SmallSetMemory) {
    auto batches = make_batches(1, 2, 3, 4);
    auto r0 = make_memory_record_batch_reader(copy_batches(batches));
    auto r1 = make_memory_record_batch_reader(copy_batches(batches));
    auto r2 = make_memory_record_batch_reader(copy_batches(batches));

    auto r0_materialized = co_await model::consume_reader_to_chunked_vector(
      std::move(r0), model::no_timeout);

    chunked_vector<model::record_batch> r1_materialized;
    auto gen1 = std::move(r1).generator(model::no_timeout);
    while (auto batch = co_await gen1()) {
        r1_materialized.push_back(std::move(batch->get()));
    }

    chunked_vector<model::record_batch> r2_materialized;
    auto gen2 = std::move(r2).slice_generator(model::no_timeout);
    while (auto batches = co_await gen2()) {
        for (auto& batch : batches->get()) {
            r2_materialized.push_back(std::move(batch));
        }
    }

    ASSERT_EQ_CORO(r0_materialized.size(), 4);
    ASSERT_EQ_CORO(r1_materialized.size(), r0_materialized.size());
    ASSERT_EQ_CORO(r2_materialized.size(), r0_materialized.size());
    for (int i = 0; i < 4; ++i) {
        ASSERT_EQ_CORO(r1_materialized[i], r0_materialized[i]);
        ASSERT_EQ_CORO(r2_materialized[i], r0_materialized[i]);
    }
}
