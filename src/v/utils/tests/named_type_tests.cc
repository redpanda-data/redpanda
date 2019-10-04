#define BOOST_TEST_MODULE utils

#include "seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <boost/test/unit_test.hpp>

#include <set>

BOOST_AUTO_TEST_CASE(named_type_basic) {
    using int_alias = named_type<int32_t, struct int_alias_test_module>;
    constexpr auto x = int_alias(5);
    BOOST_REQUIRE(x == 5);
    BOOST_REQUIRE(x <= 5);
    BOOST_REQUIRE(x < 6);
    BOOST_REQUIRE(x != 50);
    BOOST_REQUIRE(x > 4);
    BOOST_REQUIRE(x >= 5);
}
BOOST_AUTO_TEST_CASE(named_type_set) {
    using int_alias = named_type<int32_t, struct int_alias_test_module>;
    std::set<int_alias> foo;
    for (int32_t i = 0; i < 100; ++i) {
        foo.insert(i);
    }
    BOOST_REQUIRE(foo.find(5) != foo.end());
}

BOOST_AUTO_TEST_CASE(named_type_unordered_map) {
    using int_alias = named_type<int32_t, struct int_alias_test_module>;
    std::unordered_map<int_alias, int_alias> foo;
    for (int32_t i = 0; i < 100; ++i) {
        foo[i] = i;
    }
    BOOST_REQUIRE(foo[5] != 4);
}
