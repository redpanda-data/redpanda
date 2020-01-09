#include "librdkafka/rdkafkacpp.h"
#include "model/fundamental.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/fixture.h"

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
    req.topics = std::nullopt;
    return req;
}

// Valid only for Metadata API versions >= 1
static kafka::metadata_request no_topics() {
    kafka::metadata_request req;
    req.topics = std::make_optional<std::vector<model::topic>>();
    return req;
}

// https://github.com/apache/kafka/blob/8968cdd/core/src/test/scala/unit/kafka/server/MetadataRequestTest.scala#L117
FIXTURE_TEST(test_no_topics_request, redpanda_thread_fixture) {
    // FIXME: Create some topics to make this test meaningfull
    auto req = no_topics();

    auto client = make_kafka_client().get0();
    client.connect().get();
    auto resp = client.dispatch(req, kafka::api_version(1)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE(resp.topics.empty());
}

// https://github.com/apache/kafka/blob/8968cdd/core/src/test/scala/unit/kafka/server/MetadataRequestTest.scala#L52
FIXTURE_TEST(cluster_id_with_req_v1, redpanda_thread_fixture) {
    auto req = all_topics();

    auto client = make_kafka_client().get0();
    client.connect().get();
    auto resp = client.dispatch(req, kafka::api_version(1)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_REQUIRE(resp.cluster_id == std::nullopt);
}

// https://github.com/apache/kafka/blob/8968cdd/core/src/test/scala/unit/kafka/server/MetadataRequestTest.scala#L59
// https://app.asana.com/0/1149841353291489/1153248907521420
FIXTURE_TEST_EXPECTED_FAILURES(
  cluster_id_is_valid, redpanda_thread_fixture, 1) {
    auto req = all_topics();

    auto client = make_kafka_client().get0();
    client.connect().get();
    auto resp = client.dispatch(req, kafka::api_version(2)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_TEST((resp.cluster_id && !resp.cluster_id->empty()));
}

// https://github.com/apache/kafka/blob/8968cdd/core/src/test/scala/unit/kafka/server/MetadataRequestTest.scala#L87
// https://app.asana.com/0/1149841353291489/1153248907521428
FIXTURE_TEST(rack, redpanda_thread_fixture) {
    auto req = all_topics();

    auto client = make_kafka_client().get0();
    client.connect().get();
    auto resp = client.dispatch(req, kafka::api_version(1)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    // expected rack name is configured in fixture setup
    BOOST_REQUIRE(!resp.brokers.empty());
    BOOST_REQUIRE(resp.brokers.size() == 1);
    BOOST_TEST(
      (resp.brokers[0].rack && resp.brokers[0].rack.value() == rack_name));
}
