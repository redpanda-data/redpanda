#define BOOST_TEST_MODULE utils
#include "tristate.h"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(tristate_having_value) {
    // with_value
    tristate<int> has_value(10);
    BOOST_REQUIRE_EQUAL(has_value.is_disabled(), false);
    BOOST_REQUIRE_EQUAL(has_value.has_value(), true);
    BOOST_REQUIRE_EQUAL(*has_value, 10);
    BOOST_REQUIRE_EQUAL(has_value.value(), 10);
}

BOOST_AUTO_TEST_CASE(disabled_tristate) {
    // disabled
    tristate<int> disabled;
    BOOST_REQUIRE_EQUAL(disabled.is_disabled(), true);
    BOOST_REQUIRE_EQUAL(disabled.has_value(), false);
}

BOOST_AUTO_TEST_CASE(tristate_with_empty_value) {
    // empty
    tristate<int> empty(std::nullopt);
    BOOST_REQUIRE_EQUAL(empty.is_disabled(), false);
    BOOST_REQUIRE_EQUAL(empty.has_value(), false);
}
