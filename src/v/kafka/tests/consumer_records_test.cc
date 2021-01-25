// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/requests/batch_consumer.h"
#include "kafka/requests/consumer_records.h"
#include "model/timeout_clock.h"
#include "redpanda/tests/fixture.h"
#include "storage/tests/utils/random_batch.h"
#include "test_utils/async.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/std-coroutine.hh>

#include <chrono>
#include <limits>

SEASTAR_THREAD_TEST_CASE(consumer_records_last_offset) {
    constexpr model::offset init_offset{0};

    // Create some batches
    auto input = storage::test::make_random_batches(init_offset, 40);
    BOOST_REQUIRE(!input.empty());
    const auto expected_last_offset = input.back().last_offset();

    // Serialise the batches
    auto mem_res = model::make_memory_record_batch_reader(std::move(input))
                     .consume(
                       kafka::kafka_batch_serializer{}, model::no_timeout)
                     .get();

    // Functionality under test
    auto crs = kafka::consumer_records(std::move(mem_res.data));

    BOOST_REQUIRE(crs);
    BOOST_REQUIRE_EQUAL(crs.last_offset(), expected_last_offset);
}

SEASTAR_THREAD_TEST_CASE(consumer_records_consume_record_batch) {
    constexpr model::offset init_offset{0};

    // Create some batches
    auto input = storage::test::make_random_batches(init_offset, 40);
    BOOST_REQUIRE(!input.empty());
    const auto expected_last_offset = input.back().last_offset();

    // Serialise the batches
    auto mem_res = model::make_memory_record_batch_reader(std::move(input))
                     .consume(
                       kafka::kafka_batch_serializer{}, model::no_timeout)
                     .get();

    // Functionality under test
    auto crs = kafka::consumer_records(std::move(mem_res.data));

    model::offset last_offset{};
    while (!crs.empty()) {
        auto kba = crs.consume_record_batch();
        BOOST_REQUIRE(kba.v2_format);
        BOOST_REQUIRE(kba.valid_crc);
        BOOST_REQUIRE(kba.batch);
        last_offset = kba.batch->last_offset();
    }

    BOOST_REQUIRE_EQUAL(last_offset, expected_last_offset);
}

SEASTAR_THREAD_TEST_CASE(consumer_records_as_reader) {
    constexpr model::offset init_offset{0};

    // Create some batches
    auto input = storage::test::make_random_batches(init_offset, 40);
    BOOST_REQUIRE(!input.empty());
    const auto expected_last_offset = input.back().last_offset();

    // Serialise the batches
    auto mem_res = model::make_memory_record_batch_reader(std::move(input))
                     .consume(
                       kafka::kafka_batch_serializer{}, model::no_timeout)
                     .get();

    // Functionality under test
    auto rdr = model::make_record_batch_reader<kafka::consumer_records>(
      std::move(mem_res.data));
    auto output = model::consume_reader_to_memory(
                    std::move(rdr), model::no_timeout)
                    .get();

    BOOST_REQUIRE(!output.empty());
    BOOST_REQUIRE_EQUAL(output.back().last_offset(), expected_last_offset);
    BOOST_REQUIRE_EQUAL(
      output.back().last_offset()() + 1, init_offset() + mem_res.record_count);
}
