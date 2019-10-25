#define BOOST_TEST_MODULE requests
#include "redpanda/kafka/requests/timeout.h"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(test_timeouts_conversion) {
    using namespace std::chrono_literals;
    BOOST_REQUIRE_EQUAL(
      kafka::to_timeout(-2ms).time_since_epoch().count(),
      model::no_timeout.time_since_epoch().count());
    BOOST_REQUIRE_EQUAL(
      kafka::to_timeout(-1ms).time_since_epoch().count(),
      model::no_timeout.time_since_epoch().count());
    BOOST_REQUIRE_EQUAL(
      kafka::to_timeout(0ms).time_since_epoch().count(),
      model::no_timeout.time_since_epoch().count());
    BOOST_REQUIRE_GE(
      kafka::to_timeout(5000ms).time_since_epoch().count(),
      (model::timeout_clock::now() + 5s).time_since_epoch().count());
};
