#include "kafka/requests/offset_commit_request.h"
#include "redpanda/tests/fixture.h"
#include "resource_mgmt/io_priority.h"
#include "test_utils/async.h"

#include <seastar/core/smp.hh>

#include <chrono>
#include <limits>

FIXTURE_TEST(
  offset_commit_static_membership_not_supported, redpanda_thread_fixture) {
    auto client = make_kafka_client().get0();
    client.connect().get();

    kafka::offset_commit_request req;
    req.group_instance_id = kafka::group_instance_id("g");
    req.topics = {{
      .name = model::topic("t"),
      .partitions = {{}},
    }};

    auto resp = client.dispatch(req, kafka::api_version(7)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_TEST(
      resp.topics[0].partitions[0].error
      == kafka::error_code::unsupported_version);
}
