#include "kafka/requests/find_coordinator_request.h"
#include "redpanda/tests/fixture.h"
#include "resource_mgmt/io_priority.h"
#include "test_utils/async.h"

#include <seastar/core/smp.hh>

#include <chrono>
#include <limits>

FIXTURE_TEST(find_coordinator_unsupported_key, redpanda_thread_fixture) {
    auto client = make_kafka_client().get0();
    client.connect().get();

    kafka::find_coordinator_request req("key");
    req.key_type = kafka::coordinator_type::transaction;

    auto resp = client.dispatch(req, kafka::api_version(1)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_TEST(resp.error == kafka::error_code::unsupported_version);
    BOOST_TEST(resp.node == model::node_id(-1));
    BOOST_TEST(resp.host == "");
    BOOST_TEST(resp.port == -1);
}

// temporary test with fixed return value
FIXTURE_TEST(find_coordinator, redpanda_thread_fixture) {
    auto client = make_kafka_client().get0();
    client.connect().get();

    kafka::find_coordinator_request req("key");

    auto resp = client.dispatch(req, kafka::api_version(1)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_TEST(resp.error == kafka::error_code::none);
    BOOST_TEST(resp.node == model::node_id(0));
    BOOST_TEST(resp.host == "localhost");
    BOOST_TEST(resp.port == 9092);
}
