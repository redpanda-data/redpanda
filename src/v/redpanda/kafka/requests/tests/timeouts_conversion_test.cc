#define BOOST_TEST_MODULE requests
#include "redpanda/kafka/requests/timeout.h"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(test_timeouts_conversion) {
    BOOST_REQUIRE_EQUAL(
      kafka::requests::to_timeout(-2).time_since_epoch().count(),
      model::no_timeout.time_since_epoch().count());
    BOOST_REQUIRE_EQUAL(
      kafka::requests::to_timeout(-1).time_since_epoch().count(),
      model::no_timeout.time_since_epoch().count());
    BOOST_REQUIRE_EQUAL(
      kafka::requests::to_timeout(0).time_since_epoch().count(),
      model::no_timeout.time_since_epoch().count());
    using namespace std::chrono_literals;
    BOOST_REQUIRE_GE(
      kafka::requests::to_timeout(5000).time_since_epoch().count(),
      (model::timeout_clock::now() + 5s).time_since_epoch().count());
};
