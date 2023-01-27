#include "seastarx.h"
#include "utils/moving_average.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/util/log.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>
#include <ratio>

using namespace std::chrono_literals;

BOOST_AUTO_TEST_CASE(test_moving_average) {
    moving_average<int, 4> ma(0);

    ma.update(1);
    BOOST_REQUIRE_EQUAL(ma.get(), 1);
    ma.update(2);
    BOOST_REQUIRE_EQUAL(ma.get(), 1);
    ma.update(3);
    BOOST_REQUIRE_EQUAL(ma.get(), 2);
    ma.update(6);

    BOOST_REQUIRE_EQUAL(ma.get(), 3);

    ma.update(9);

    BOOST_REQUIRE_EQUAL(ma.get(), 5);
}

BOOST_AUTO_TEST_CASE(test_timed_moving_average) {
    const auto eps = 0.001;
    constexpr auto depth = std::chrono::duration_cast<std::chrono::nanoseconds>(
      1s);
    constexpr auto resolution
      = std::chrono::duration_cast<std::chrono::nanoseconds>(100ms);
    timed_moving_average<double, ss::lowres_clock> ma(0, depth, resolution);
    BOOST_REQUIRE_CLOSE(ma.get(), 00, eps);
    auto bt = ss::lowres_clock::now();
    ma.update(1, bt + 100ms);
    BOOST_REQUIRE_CLOSE(ma.get(), 0.5, eps);
    ma.update(2, bt + 200ms);
    BOOST_REQUIRE_CLOSE(ma.get(), 1.0, eps);
    ma.update(3, bt + 300ms);
    BOOST_REQUIRE_CLOSE(ma.get(), 1.5, eps);
    ma.update(4, bt + 400ms);
    BOOST_REQUIRE_CLOSE(ma.get(), 2.0, eps);
    ma.update(5, bt + 500ms);
    BOOST_REQUIRE_CLOSE(ma.get(), 2.5, eps);
    ma.update(6, bt + 600ms);
    BOOST_REQUIRE_CLOSE(ma.get(), 3.0, eps);
    ma.update(7, bt + 700ms);
    BOOST_REQUIRE_CLOSE(ma.get(), 3.5, eps);
    ma.update(8, bt + 800ms);
    BOOST_REQUIRE_CLOSE(ma.get(), 4.0, eps);
    ma.update(9, bt + 900ms);
    BOOST_REQUIRE_CLOSE(ma.get(), 4.5, eps);
    // saturated, start evict elements
    ma.update(10, bt + 1000ms);
    BOOST_REQUIRE_CLOSE(ma.get(), 5.5, eps);
    ma.update(11, bt + 1100ms);
    BOOST_REQUIRE_CLOSE(ma.get(), 6.5, eps);
    ma.update(12, bt + 1200ms);
    BOOST_REQUIRE_CLOSE(ma.get(), 7.5, eps);
    ma.update(13, bt + 1300ms);
    BOOST_REQUIRE_CLOSE(ma.get(), 8.5, eps);
    ma.update(14, bt + 1400ms);
    BOOST_REQUIRE_CLOSE(ma.get(), 9.5, eps);
    ma.update(15, bt + 1500ms);
    BOOST_REQUIRE_CLOSE(ma.get(), 10.5, eps);
    ma.update(16, bt + 1600ms);
    BOOST_REQUIRE_CLOSE(ma.get(), 11.5, eps);
    ma.update(17, bt + 1700ms);
    BOOST_REQUIRE_CLOSE(ma.get(), 12.5, eps);
    ma.update(18, bt + 1800ms);
    BOOST_REQUIRE_CLOSE(ma.get(), 13.5, eps);
    ma.update(19, bt + 1900ms);
    BOOST_REQUIRE_CLOSE(ma.get(), 14.5, eps);
    // evict everything
    ma.update(0, bt + 5s);
    BOOST_REQUIRE_CLOSE(ma.get(), 0, eps);
    // check samples which are overlapping in time
    ma.update(6, bt + 5s);
    ma.update(3, bt + 5s + 10ms);
    BOOST_REQUIRE_CLOSE(ma.get(), 3, eps);
}