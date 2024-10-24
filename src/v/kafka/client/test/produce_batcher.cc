// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/produce_batcher.h"

#include "cluster/simple_batch_builder.h"
#include "kafka/client/test/utils.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/produce.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "reflection/adl.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/when_all.hh>
#include <seastar/testing/thread_test_case.hh>

namespace kc = kafka::client;

struct produce_batcher_context {
    const model::partition_id partition_id{2};
    const model::offset base_offset{42};
    model::offset client_req_offset{base_offset};
    model::offset broker_req_offset{base_offset};

    kc::produce_batcher batcher;
    // expected client offset
    std::vector<model::offset> expected_offsets;
    // future client offset
    std::vector<ss::future<kafka::produce_response::partition>> produce_futs{};
    // expected broker offset and broker batch
    ss::circular_buffer<std::pair<model::offset, model::record_batch>>
      broker_batches{};

    void produce(int32_t count) {
        auto batch = make_batch(client_req_offset, count);
        expected_offsets.push_back(client_req_offset);
        produce_futs.push_back(batcher.produce(std::move(batch)));
        client_req_offset += count;
    }
    ss::future<int32_t> consume() {
        model::record_batch batch = co_await batcher.consume();
        auto record_count = batch.record_count();
        broker_batches.emplace_back(broker_req_offset, std::move(batch));
        broker_req_offset += record_count;
        co_return record_count;
    }
    auto handle_response(kafka::error_code error = kafka::error_code::none) {
        auto batch = kc::consume_front(broker_batches);
        batcher.handle_response(kafka::produce_response::partition{
          .partition_index{partition_id},
          .error_code = error,
          .base_offset{
            error == kafka::error_code::none ? batch.first : model::offset{-1}},
          .log_append_time_ms{model::timestamp{0}},
          .log_start_offset{model::offset{-1}},
        });
        return batch.second.record_count();
    }
    ss::future<std::vector<kafka::produce_response::partition>>
    get_responses() {
        return ss::when_all_succeed(produce_futs.begin(), produce_futs.end());
    }
    ss::future<std::vector<model::offset>> get_response_offsets() {
        return get_responses().then(
          [](std::vector<kafka::produce_response::partition> results) {
              std::vector<model::offset> offsets;
              offsets.reserve(results.size());
              std::transform(
                std::make_move_iterator(results.begin()),
                std::make_move_iterator(results.end()),
                std::back_inserter(offsets),
                [](kafka::produce_response::partition p) {
                    return p.base_offset;
                });
              return offsets;
          });
    }
};

SEASTAR_THREAD_TEST_CASE(test_partition_producer_single) {
    produce_batcher_context ctx;

    ctx.produce(2);
    BOOST_REQUIRE(ctx.consume().get() == 2);
    BOOST_REQUIRE(ctx.handle_response() == 2);

    auto offsets = ctx.get_response_offsets().get();
    BOOST_REQUIRE(offsets == ctx.expected_offsets);

    BOOST_REQUIRE(ctx.consume().get() == 0);
}

SEASTAR_THREAD_TEST_CASE(test_partition_producer_seq) {
    produce_batcher_context ctx;

    ctx.produce(2);
    ctx.produce(2);
    BOOST_REQUIRE(ctx.consume().get() == 4);
    BOOST_REQUIRE(ctx.handle_response() == 4);

    ctx.produce(2);
    ctx.produce(2);
    BOOST_REQUIRE(ctx.consume().get() == 4);
    BOOST_REQUIRE(ctx.handle_response() == 4);

    auto offsets = ctx.get_response_offsets().get();
    BOOST_REQUIRE(offsets == ctx.expected_offsets);

    BOOST_REQUIRE(ctx.consume().get() == 0);
}

SEASTAR_THREAD_TEST_CASE(test_partition_producer_overlapped) {
    produce_batcher_context ctx;

    ctx.produce(2);
    ctx.produce(2);
    BOOST_REQUIRE(ctx.consume().get() == 4);

    ctx.produce(2);
    ctx.produce(2);
    BOOST_REQUIRE(ctx.consume().get() == 4);

    BOOST_REQUIRE(ctx.handle_response() == 4);
    BOOST_REQUIRE(ctx.handle_response() == 4);

    auto offsets = ctx.get_response_offsets().get();
    BOOST_REQUIRE(offsets == ctx.expected_offsets);

    BOOST_REQUIRE(ctx.consume().get() == 0);
}

SEASTAR_THREAD_TEST_CASE(test_partition_producer_error) {
    produce_batcher_context ctx;

    ctx.produce(2);
    ctx.produce(2);
    BOOST_REQUIRE(ctx.consume().get() == 4);

    ctx.produce(2);
    ctx.produce(2);
    BOOST_REQUIRE(ctx.consume().get() == 4);

    BOOST_REQUIRE(ctx.handle_response() == 4);
    BOOST_REQUIRE(
      ctx.handle_response(kafka::error_code::not_leader_for_partition) == 4);

    auto responses = ctx.get_responses().get();
    BOOST_REQUIRE(responses.size() == 4);
    // successes
    BOOST_REQUIRE(responses[0].base_offset == model::offset(42));
    BOOST_REQUIRE(responses[0].error_code == kafka::error_code::none);
    BOOST_REQUIRE(responses[1].base_offset == model::offset(44));
    BOOST_REQUIRE(responses[1].error_code == kafka::error_code::none);
    // failures
    BOOST_REQUIRE(responses[2].base_offset == model::offset(-1));
    BOOST_REQUIRE(
      responses[2].error_code == kafka::error_code::not_leader_for_partition);
    BOOST_REQUIRE(responses[3].base_offset == model::offset(-1));
    BOOST_REQUIRE(
      responses[3].error_code == kafka::error_code::not_leader_for_partition);

    BOOST_REQUIRE(ctx.consume().get() == 0);
}
