// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/protocol/errors.h"
#include "kafka/protocol/metadata.h"
#include "librdkafka/rdkafkacpp.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/fixture.h"

#include <seastar/core/when_all.hh>

#include <boost/test/tools/old/interface.hpp>

#include <optional>
#include <vector>

#if 0
/*
 * Disabled tests based on librdkafka that rely on an alien thread pool. There
 * is a bug that has started popping up with that after a recent seastar update.
 * that will need to be tracked down.
 */
#include <cppkafka/cppkafka.h>
#include <v/native_thread_pool.h>

ss::ss::future<> get_metadata(v::ThreadPool& tp) {
    return tp.submit([]() {
        cppkafka::Configuration config = {
          {"metadata.broker.list", "127.0.0.1:9092"}};

        cppkafka::Producer producer(config);
        cppkafka::Metadata metadata = producer.get_metadata();

        if (!metadata.get_topics().empty()) {
            throw new std::runtime_error("expected topic set to be empty");
        }
    });
}

FIXTURE_TEST(get_metadadata, redpanda_thread_fixture) {
    v::ThreadPool thread_pool(1, 1, 0);
    thread_pool.start().get();
    get_metadata(thread_pool).get();
    thread_pool.stop().get();
}
#endif

static kafka::metadata_request all_topics() {
    kafka::metadata_request req;
    req.data.topics = std::nullopt;
    return req;
}

// Valid only for Metadata API versions >= 1
static kafka::metadata_request no_topics() {
    kafka::metadata_request req;
    req.data.topics
      = std::make_optional<std::vector<kafka::metadata_request_topic>>();
    return req;
}

// https://github.com/apache/kafka/blob/8968cdd/core/src/test/scala/unit/kafka/server/MetadataRequestTest.scala#L117
FIXTURE_TEST(test_no_topics_request, redpanda_thread_fixture) {
    wait_for_controller_leadership().get();
    // FIXME: Create some topics to make this test meaningfull
    auto req = no_topics();

    auto client = make_kafka_client().get0();
    client.connect().get();
    auto resp = client.dispatch(req, kafka::api_version(1)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE(resp.data.topics.empty());
}

// https://github.com/apache/kafka/blob/8968cdd/core/src/test/scala/unit/kafka/server/MetadataRequestTest.scala#L52
FIXTURE_TEST(cluster_id_with_req_v1, redpanda_thread_fixture) {
    wait_for_controller_leadership().get();
    auto req = all_topics();

    auto client = make_kafka_client().get0();
    client.connect().get();
    auto resp = client.dispatch(req, kafka::api_version(1)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE(resp.data.cluster_id == std::nullopt);
}

// https://github.com/apache/kafka/blob/8968cdd/core/src/test/scala/unit/kafka/server/MetadataRequestTest.scala#L59
// https://app.asana.com/0/1149841353291489/1153248907521420
FIXTURE_TEST_EXPECTED_FAILURES(
  cluster_id_is_valid, redpanda_thread_fixture, 1) {
    wait_for_controller_leadership().get();
    auto req = all_topics();

    auto client = make_kafka_client().get0();
    client.connect().get();
    auto resp = client.dispatch(req, kafka::api_version(2)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_TEST((resp.data.cluster_id && !resp.data.cluster_id->empty()));
}

// https://github.com/apache/kafka/blob/8968cdd/core/src/test/scala/unit/kafka/server/MetadataRequestTest.scala#L87
// https://app.asana.com/0/1149841353291489/1153248907521428
FIXTURE_TEST(rack, redpanda_thread_fixture) {
    wait_for_controller_leadership().get();
    auto req = all_topics();

    auto client = make_kafka_client().get0();
    client.connect().get();
    auto resp = client.dispatch(req, kafka::api_version(1)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    // expected rack name is configured in fixture setup
    BOOST_REQUIRE(!resp.data.brokers.empty());
    BOOST_REQUIRE(resp.data.brokers.size() == 1);
    BOOST_TEST(
      (resp.data.brokers[0].rack
       && resp.data.brokers[0].rack.value() == rack_name));
}

FIXTURE_TEST(test_topic_namespaces, redpanda_thread_fixture) {
    wait_for_controller_leadership().get();

    auto add_topic_for_tp_ns = [&, this](model::topic_namespace tp_ns) {
        add_topic(tp_ns).get();
        model::ntp ntp(tp_ns.ns, tp_ns.tp, model::partition_id(0));
        return tests::cooperative_spin_wait_with_timeout(2s, [ntp, this] {
            auto shard = app.shard_table.local().shard_for(ntp);
            if (!shard) {
                return ss::make_ready_future<bool>(false);
            }
            return app.partition_manager.invoke_on(
              *shard, [ntp](cluster::partition_manager& pm) {
                  return pm.get(ntp)->is_leader();
              });
        });
    };

    const model::topic_namespace test_topic(
      model::kafka_namespace, model::topic("test-topic"));
    const model::topic_namespace test_internal_topic(
      model::kafka_internal_namespace, model::topic("test-internal-topic"));

    ss::when_all_succeed(
      add_topic_for_tp_ns(test_topic), add_topic_for_tp_ns(test_internal_topic))
      .get();

    auto client = make_kafka_client().get();
    client.connect().get();
    auto resp = client.dispatch(all_topics()).get();
    BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 1);
    BOOST_REQUIRE_EQUAL(
      resp.data.topics[0].error_code, kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(resp.data.topics[0].name, test_topic.tp);
    client.stop().then([&client] { client.shutdown(); }).get();
};
