#include "utils/moving_average.h"

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

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
