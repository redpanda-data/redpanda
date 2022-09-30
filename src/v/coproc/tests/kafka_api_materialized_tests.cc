/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/tests/fixtures/coproc_test_fixture.h"
#include "coproc/tests/utils/batch_utils.h"
#include "coproc/tests/utils/coprocessor.h"
#include "coproc/types.h"
#include "kafka/client/transport.h"
#include "kafka/protocol/delete_topics.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/find_coordinator.h"
#include "kafka/protocol/metadata.h"

#include <seastar/core/coroutine.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

static kafka::client::transport make_kafka_client() {
    return kafka::client::transport(
      net::base_transport::configuration{
        .server_addr = config::node().kafka_api()[0].address,
      },
      "test_client");
}

class push_some_data_fixture : public coproc_test_fixture {
public:
    ss::future<model::record_batch_reader::data_t> start() {
        co_await setup({{input_topic, 1}});
        co_await enable_coprocessors(
          {{.id = 1234,
            .data{
              .tid = coproc::registry::type_identifier::identity_coprocessor,
              .topics = {
                {input_topic, coproc::topic_ingestion_policy::stored}}}}});
        co_await produce(input_ntp, make_random_batch(160));
        co_return co_await consume(output_ntp, 160);
    }

    static const inline model::topic input_topic{"intpc1"};
    static const inline model::topic output_topic{
      to_materialized_topic(input_topic, identity_coprocessor::identity_topic)};
    static const inline model::ntp input_ntp{
      model::kafka_namespace, input_topic, model::partition_id(0)};
    static const inline model::ntp output_ntp{
      model::kafka_namespace, output_topic, model::partition_id(0)};
};

FIXTURE_TEST(test_metadata_request, push_some_data_fixture) {
    const auto data = start().get();

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

FIXTURE_TEST(test_read_from_materialized_topic, push_some_data_fixture) {
    const auto data = start().get();

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

ss::future<std::vector<kafka::metadata_response_topic>>
list_all_topics(kafka::client::transport& cli) {
    kafka::metadata_request list_topics{.list_all_topics = true};
    return cli.dispatch(list_topics, kafka::api_version(4))
      .then([](auto all_topics) { return std::move(all_topics.data.topics); });
}

bool topic_exists(
  std::vector<kafka::metadata_response_topic>& topics,
  const model::topic& target) {
    return std::find_if(
             topics.begin(),
             topics.end(),
             [&](const kafka::metadata_response_topic& mrt) {
                 return mrt.name == target;
             })
           != topics.end();
}

FIXTURE_TEST(test_delete_materialized_topic, push_some_data_fixture) {
    const auto data = start().get();
    auto client = make_kafka_client();
    client.connect().get();

    /// List all topics, ensuring expected topics exists
    auto topics = list_all_topics(client).get();
    BOOST_REQUIRE(topic_exists(topics, input_topic));
    BOOST_REQUIRE(topic_exists(topics, output_topic));

    /// Attempt to delete source topic, this should fail
    kafka::delete_topics_request drt{
      .data{.topic_names{input_topic}, .timeout_ms = 1s}};
    auto drt_response = client.dispatch(drt, kafka::api_version(3)).get();
    BOOST_REQUIRE_EQUAL(drt_response.data.responses.size(), 1);
    BOOST_REQUIRE_EQUAL(drt_response.data.responses[0].name, input_topic);
    BOOST_REQUIRE_EQUAL(
      drt_response.data.responses[0].error_code,
      kafka::error_code::cluster_authorization_failed);
    topics = list_all_topics(client).get();
    BOOST_REQUIRE(topic_exists(topics, input_topic));

    /// Attempt to delete materialized topic, this should work
    kafka::delete_topics_request drto{
      .data{.topic_names{output_topic}, .timeout_ms = 1s}};
    drt_response = client.dispatch(drto, kafka::api_version(3)).get();
    BOOST_REQUIRE_EQUAL(drt_response.data.responses.size(), 1);
    BOOST_REQUIRE_EQUAL(drt_response.data.responses[0].name, output_topic);
    BOOST_REQUIRE_EQUAL(
      drt_response.data.responses[0].error_code, kafka::error_code::none);

    /// Verify only materialized topic was deleted
    topics = list_all_topics(client).get();
    BOOST_REQUIRE(!topic_exists(topics, output_topic));
    BOOST_REQUIRE(topic_exists(topics, input_topic));
    client.stop().get();
}

FIXTURE_TEST(
  find_coordinator_for_non_replicatable_topic, redpanda_thread_fixture) {
    wait_for_controller_leadership().get();
    model::topic_namespace src{model::kafka_namespace, model::topic("src")};
    model::topic_namespace dst{model::kafka_namespace, model::topic("dst")};
    add_topic(src).get();
    add_non_replicable_topic(std::move(src), std::move(dst)).get();

    auto client = make_kafka_client().get0();
    client.connect().get();
    kafka::find_coordinator_request req("src");
    kafka::find_coordinator_request req2("dst");
    std::vector<kafka::find_coordinator_response> resps;
    resps.push_back(client.dispatch(req, kafka::api_version(1)).get0());
    resps.push_back(client.dispatch(req2, kafka::api_version(1)).get0());
    client.stop().then([&client] { client.shutdown(); }).get();

    for (const auto& r : resps) {
        BOOST_CHECK(r.data.error_code == kafka::error_code::none);
        BOOST_CHECK(r.data.node_id == model::node_id(1));
        BOOST_CHECK(r.data.host == "127.0.0.1");
        BOOST_CHECK(r.data.port == 9092);
    }
}
