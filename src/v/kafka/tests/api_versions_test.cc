#include "kafka/requests/api_versions_request.h"
#include "kafka/types.h"
#include "redpanda/tests/fixture.h"
#include "resource_mgmt/io_priority.h"
#include "seastarx.h"

#include <seastar/core/smp.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>

FIXTURE_TEST(get_api_versions, redpanda_thread_fixture) {
    auto client = make_kafka_client().get0();
    client.connect().get();

    kafka::api_versions_request request;
    auto response = client.dispatch(request, kafka::api_version(0)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();
    BOOST_REQUIRE(!response.apis.empty());

    auto expected = kafka::get_supported_apis();
    BOOST_TEST(response.apis == expected);
}
