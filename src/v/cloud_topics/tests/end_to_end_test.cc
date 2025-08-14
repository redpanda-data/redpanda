/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/tests/s3_imposter.h"
#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "config/configuration.h"
#include "kafka/server/tests/list_offsets_utils.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "random/generators.h"
#include "redpanda/tests/fixture.h"
#include "ssx/sformat.h"
#include "storage/ntp_config.h"
#include "test_utils/async.h"
#include "test_utils/scoped_config.h"

#include <gtest/gtest.h>

#include <iterator>

using tests::kafka_consume_transport;
using tests::kafka_produce_transport;
using tests::kv_t;

static ss::logger e2e_test_log("e2e_test");

class e2e_fixture
  : public s3_imposter_fixture
  , public redpanda_thread_fixture
  , public ::testing::Test {
public:
    e2e_fixture()
      : redpanda_thread_fixture(init_cloud_topics_tag{}, httpd_port_number()) {
        // No expectations: tests will PUT and GET organically.
        set_expectations_and_listen({});
        wait_for_controller_leadership().get();
    }

    scoped_config test_local_cfg;
};

TEST_F(e2e_fixture, test_create_cloud_topic) {
    const model::topic topic_name("tapioca");
    model::ntp ntp(model::kafka_namespace, topic_name, 0);

    cluster::topic_properties props;
    props.cloud_topic_enabled = true;
    props.shadow_indexing = model::shadow_indexing_mode::disabled;

    add_topic({model::kafka_namespace, topic_name}, 1, props).get();

    wait_for_leader(ntp).get();

    auto partition = app.partition_manager.local().get(ntp);
    ASSERT_TRUE(
      partition->raft()
        ->stm_manager()
        ->get<experimental::cloud_topics::ctp_stm>()
      != nullptr);
}

TEST_F(e2e_fixture, test_l0_path) {
    const model::topic topic_name("tapioca");
    model::ntp ntp(model::kafka_namespace, topic_name, 0);

    cluster::topic_properties props;
    props.cloud_topic_enabled = true;
    props.shadow_indexing = model::shadow_indexing_mode::disabled;

    add_topic({model::kafka_namespace, topic_name}, 1, props).get();

    wait_for_leader(ntp).get();

    auto partition = app.partition_manager.local().get(ntp);
    // Produce data to the partition
    kafka_produce_transport producer(make_kafka_client().get());
    producer.start().get();
    auto deferred_close = ss::defer([&producer] { producer.stop().get(); });

    size_t total_records = 100;
    size_t records_per_batch = 1;
    std::vector<kv_t> records;
    for (size_t i = 0; i < total_records; i += records_per_batch) {
        std::vector<kv_t> batch;
        for (size_t j = 0; j < records_per_batch; j++) {
            records.emplace_back(
              ssx::sformat("key{}", i + j), ssx::sformat("val{}", i + j));
            batch.push_back(records.back());
        }
        producer.produce_to_partition(topic_name, model::partition_id(0), batch)
          .get();
    }

    kafka_consume_transport consumer(make_kafka_client().get());
    consumer.start().get();
    auto deferred_c_close = ss::defer([&consumer] { consumer.stop().get(); });
    auto consumed_records = consumer
                              .consume_from_partition(
                                topic_name,
                                model::partition_id(0),
                                model::offset(0))
                              .get();
    BOOST_CHECK_EQUAL(records.size(), consumed_records.size());
    for (size_t i = 0; i < records.size(); ++i) {
        BOOST_CHECK_EQUAL(records[i].key, consumed_records[i].key);
        BOOST_CHECK_EQUAL(records[i].val, consumed_records[i].val);
    }

    ss::sleep(10s).get();
}
