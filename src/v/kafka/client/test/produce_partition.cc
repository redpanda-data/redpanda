// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/produce_partition.h"

#include "kafka/client/configuration.h"
#include "kafka/client/test/utils.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/produce.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "test_utils/async.h"

#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

namespace kc = kafka::client;

SEASTAR_THREAD_TEST_CASE(test_produce_partition_record_count) {
    std::vector<model::record_batch> consumed_batches;
    auto consumer = [&consumed_batches](model::record_batch&& batch) {
        consumed_batches.push_back(std::move(batch));
    };

    auto cfg = kc::configuration{};
    // large
    cfg.produce_batch_size_bytes.set_value(1024);
    // configuration under test
    cfg.produce_batch_record_count.set_value(3);

    kc::produce_partition producer(
      kc::producer_configuration::from_config_store(cfg), consumer);

    auto c_res0_fut = producer.produce(make_batch(model::offset(0), 2));
    auto c_res1_fut = producer.produce(make_batch(model::offset(2), 1));

    tests::cooperative_spin_wait_with_timeout(5s, [&consumed_batches]() {
        return consumed_batches.size() > 0;
    }).get();

    producer.handle_response(
      kafka::produce_response::partition{
        .partition_index{model::partition_id{42}},
        .error_code = kafka::error_code::none,
        .base_offset{model::offset{0}}});

    BOOST_REQUIRE_EQUAL(consumed_batches.size(), 1);
    BOOST_REQUIRE_EQUAL(consumed_batches[0].record_count(), 3);
    auto c_res0 = c_res0_fut.get();
    BOOST_REQUIRE_EQUAL(c_res0.base_offset, model::offset{0});
    auto c_res1 = c_res1_fut.get();
    BOOST_REQUIRE_EQUAL(c_res1.base_offset, model::offset{2});

    auto c_res2_fut = producer.produce(make_batch(model::offset(3), 3));
    tests::cooperative_spin_wait_with_timeout(5s, [&consumed_batches]() {
        return consumed_batches.size() > 1;
    }).get();
    producer.handle_response(
      kafka::produce_response::partition{
        .partition_index{model::partition_id{42}},
        .error_code = kafka::error_code::none,
        .base_offset{model::offset{3}}});

    BOOST_REQUIRE_EQUAL(consumed_batches.size(), 2);
    BOOST_REQUIRE_EQUAL(consumed_batches[1].record_count(), 3);
    auto c_res2 = c_res2_fut.get();
    BOOST_REQUIRE_EQUAL(c_res2.base_offset, model::offset{3});
    producer.stop().get();
}

// produce_partition dispatches at most one request at a time. The test puts
// one batch in flight, then produces five records that each exceed the
// flush thresholds: none may dispatch while the first is outstanding. After
// handle_response() they go out as one batch. Per-partition ordering rests
// on this: dispatches that never overlap cannot reorder.
SEASTAR_THREAD_TEST_CASE(test_produce_partition_serializes_dispatch) {
    int outstanding = 0;
    int consumed = 0;
    auto consumer = [&](model::record_batch&&) {
        BOOST_REQUIRE_EQUAL(outstanding, 0);
        ++outstanding;
        ++consumed;
    };

    auto cfg = kc::configuration{};
    // force a flush attempt on every produce
    cfg.produce_batch_size_bytes.set_value(1);
    cfg.produce_batch_record_count.set_value(1);

    kc::produce_partition producer(
      kc::producer_configuration::from_config_store(cfg), consumer);

    auto c_res0_fut = producer.produce(make_batch(model::offset(0), 1));
    RPTEST_REQUIRE_EVENTUALLY(5s, [&consumed]() { return consumed == 1; });

    std::vector<ss::future<kc::produce_partition::response>> futs;
    futs.reserve(5);
    for (int i = 1; i <= 5; ++i) {
        futs.push_back(producer.produce(make_batch(model::offset(i), 1)));
    }
    // give the flush timers every chance to (wrongly) fire
    ss::sleep(100ms).get();
    BOOST_REQUIRE_EQUAL(consumed, 1);

    --outstanding;
    producer.handle_response(
      kafka::produce_response::partition{
        .partition_index{model::partition_id{42}},
        .error_code = kafka::error_code::none,
        .base_offset{model::offset{0}}});
    BOOST_REQUIRE_EQUAL(c_res0_fut.get().base_offset, model::offset{0});

    // the buffered records go out as one request
    RPTEST_REQUIRE_EVENTUALLY(5s, [&consumed]() { return consumed == 2; });

    --outstanding;
    producer.handle_response(
      kafka::produce_response::partition{
        .partition_index{model::partition_id{42}},
        .error_code = kafka::error_code::none,
        .base_offset{model::offset{1}}});
    for (auto& fut : futs) {
        fut.get();
    }
    producer.stop().get();
}
