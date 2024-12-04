/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/interfaces/cluster_partition_manager.h"
#include "cloud_topics/interfaces/tests/partition_data_generator.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "redpanda/tests/fixture.h"
#include "utils/move_canary.h"

#include <seastar/core/io_priority_class.hh>

#include <gtest/gtest.h>

namespace cloud_topics = experimental::cloud_topics;

static ss::logger test_log("test_log");

struct basic_fixture
  : redpanda_thread_fixture
  , ::testing::Test {
    basic_fixture() { wait_for_controller_leadership().get(); }
};

struct counting_consumer {
    explicit counting_consumer(size_t expected_num_records)
      : _expected(expected_num_records) {}

    counting_consumer(const counting_consumer&) = delete;
    counting_consumer& operator=(const counting_consumer&) = delete;
    counting_consumer(counting_consumer&&) = default;
    counting_consumer& operator=(counting_consumer&&) = default;

    ~counting_consumer() {
        if (!_canary.is_moved_from() && _count != _expected) {
            GTEST_MESSAGE_(
              fmt::format(
                "Unexpected count. Expected {}, got {}.", _expected, _count)
                .c_str(),
              ::testing::TestPartResult::kFatalFailure);
        }
    }

    ss::future<ss::stop_iteration> operator()(model::record_batch rb) {
        vlog(test_log.debug, "Got record batch: {}", rb.header());
        if (rb.header().type == model::record_batch_type::raft_data) {
            _count += rb.record_count();
        }
        co_return ss::stop_iteration::no;
    }

    void end_of_stream() {}

    size_t _count{0};
    size_t _expected;
    move_canary _canary;
};

TEST_F(basic_fixture, create_topic) {
    const model::topic topic_name("tapioca");
    model::ntp ntp(model::kafka_namespace, topic_name, 0);
    cluster::topic_properties props;
    add_topic({model::kafka_namespace, topic_name}, 1, props).get();
    wait_for_leader(ntp).get();

    auto partition = app.partition_manager.local().get(ntp);
    tests::partition_data_generator gen(make_kafka_client().get(), *partition);
    const size_t num_records = 1000;
    const size_t rec_per_batch = 5;
    auto total_records = gen.total_number_of_records(num_records)
                           .records_per_batch(rec_per_batch)
                           .produce()
                           .get();

    ASSERT_GE(total_records, num_records);

    auto pm = cloud_topics::make_cluster_partition_manager(
      app.partition_manager.local());
    auto part = pm->get_partition(ntp);
    storage::log_reader_config cfg(
      model::offset(0),
      model::offset(num_records),
      ss::default_priority_class());
    auto reader = part->make_reader(cfg).get();

    ss::sleep(1s).get();

    reader.consume(counting_consumer(num_records), model::no_timeout).get();
}
