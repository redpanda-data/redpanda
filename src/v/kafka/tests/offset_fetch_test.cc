#include "kafka/requests/offset_fetch_request.h"
#include "redpanda/tests/fixture.h"
#include "resource_mgmt/io_priority.h"
#include "test_utils/async.h"

#include <seastar/core/smp.hh>

#include <chrono>
#include <limits>

FIXTURE_TEST(offset_fetch, redpanda_thread_fixture) {
    auto client = make_kafka_client().get0();
    client.connect().get();

    kafka::offset_fetch_request req;
    req.group_id = kafka::group_id("g");

    auto resp = client.dispatch(req, kafka::api_version(2)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_TEST(resp.error == kafka::error_code::not_coordinator);
}
