// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/produce_partition.h"

#include "kafka/client/brokers.h"
#include "kafka/client/configuration.h"
#include "kafka/client/test/utils.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/produce.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "test_utils/async.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

#include <exception>
#include <system_error>

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

    kc::produce_partition producer(cfg, consumer);

    auto c_res0_fut = producer.produce(make_batch(model::offset(0), 2));
    auto c_res1_fut = producer.produce(make_batch(model::offset(2), 1));

    tests::cooperative_spin_wait_with_timeout(5s, [&consumed_batches]() {
        return consumed_batches.size() > 0;
    }).get();

    producer.handle_response(kafka::produce_response::partition{
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
    producer.handle_response(kafka::produce_response::partition{
      .partition_index{model::partition_id{42}},
      .error_code = kafka::error_code::none,
      .base_offset{model::offset{3}}});

    BOOST_REQUIRE_EQUAL(consumed_batches.size(), 2);
    BOOST_REQUIRE_EQUAL(consumed_batches[1].record_count(), 3);
    auto c_res2 = c_res2_fut.get();
    BOOST_REQUIRE_EQUAL(c_res2.base_offset, model::offset{3});
    producer.stop().get();
}
