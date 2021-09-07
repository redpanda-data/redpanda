/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/tests/fixtures/coproc_test_fixture.h"
#include "coproc/tests/utils/coprocessor.h"
#include "coproc/types.h"
#include "kafka/client/transport.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/metadata.h"
#include "storage/tests/utils/random_batch.h"

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

static kafka::client::transport make_kafka_client() {
    return kafka::client::transport(rpc::base_transport::configuration{
      .server_addr = config::shard_local_cfg().kafka_api()[0].address,
    });
}

FIXTURE_TEST(test_metadata_request, coproc_test_fixture) {
    model::topic input_topic("intpc1");
    model::topic output_topic = model::to_materialized_topic(
      input_topic, identity_coprocessor::identity_topic);
    setup({{input_topic, 1}}).get();
    model::ntp input_ntp(
      model::kafka_namespace, input_topic, model::partition_id(0));
    model::ntp output_ntp(
      model::kafka_namespace, output_topic, model::partition_id(0));

    /// Create coprocessor
    enable_coprocessors(
      {{.id = 1234,
        .data{
          .tid = coproc::registry::type_identifier::identity_coprocessor,
          .topics = {std::make_pair<>(
            input_topic, coproc::topic_ingestion_policy::stored)}}}})
      .get();

    /// Deploy some data onto the input topic
    push(
      input_ntp,
      storage::test::make_random_memory_record_batch_reader(
        model::offset(0), 4, 4))
      .get();

    /// Make a metadata request specifically for the materialized topic
    kafka::metadata_request req{
      .data = {.topics = {{{output_topic}}}},
      .list_all_topics = false,
    };
    auto client = make_kafka_client();
    client.connect().get();
    auto resp = client.dispatch(req, kafka::api_version(4)).get0();
    client.stop().get();
    BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 1);
    BOOST_REQUIRE_EQUAL(
      resp.data.topics[0].error_code, kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(resp.data.topics[0].name, output_topic);
    BOOST_REQUIRE_EQUAL(resp.data.topics[0].partitions.size(), 1);
}

FIXTURE_TEST(test_read_from_materialized_topic, coproc_test_fixture) {
    model::topic input_topic("foo");
    model::topic output_topic = model::to_materialized_topic(
      input_topic, identity_coprocessor::identity_topic);
    setup({{input_topic, 1}}).get();
    model::ntp input_ntp(
      model::kafka_namespace, input_topic, model::partition_id(0));
    model::ntp output_ntp(
      model::kafka_namespace, output_topic, model::partition_id(0));

    /// Create coprocessor
    enable_coprocessors(
      {{.id = 1234,
        .data{
          .tid = coproc::registry::type_identifier::identity_coprocessor,
          .topics = {std::make_pair<>(
            input_topic, coproc::topic_ingestion_policy::stored)}}}})
      .get();

    /// Deploy some data onto the input topic
    push(
      input_ntp,
      storage::test::make_random_memory_record_batch_reader(
        model::offset(0), 4, 4))
      .get();

    // read the materialized topic from disk
    const auto data = drain(output_ntp, 16).get0();
    BOOST_REQUIRE(data);

    // Connect a kafka client to the expected output topic
    kafka::fetch_request req;
    req.data.max_bytes = std::numeric_limits<int32_t>::max();
    req.data.min_bytes = 1; // At LEAST 'bytes_written' in src topic
    req.data.max_wait_ms = 2s;
    req.data.topics = {
      {.name = output_topic,
       .fetch_partitions = {
         {.partition_index = model::partition_id(0),
          .fetch_offset = model::offset(0)}}}};

    // .. and read the same partition using a kafka client
    auto client = make_kafka_client();
    client.connect().get();
    auto resp = client.dispatch(req, kafka::api_version(4)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 1);
    BOOST_REQUIRE_EQUAL(resp.data.topics[0].name, output_topic);
    BOOST_REQUIRE_EQUAL(
      resp.data.topics[0].partitions[0].error_code, kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(
      resp.data.topics[0].partitions[0].partition_index,
      model::partition_id(0));
    BOOST_REQUIRE(resp.data.topics[0].partitions[0].records);
    // TODO(rob) fix this assertion
    // BOOST_REQUIRE_EQUAL(
    //   std::move(*resp.partitions[0].responses[0].record_set).release(),
    //   data);
}
