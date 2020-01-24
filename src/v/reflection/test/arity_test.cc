#define BOOST_TEST_MODULE reflection

#include "reflection/arity.h"
#include "rpc/test/test_types.h"

#include <boost/test/unit_test.hpp>

struct inherit_complex_pod : complex_custom {
    int i;
    float j;
};

BOOST_AUTO_TEST_CASE(verify_airty) {
    BOOST_CHECK_EQUAL(reflection::arity<pod>(), 3);
    BOOST_CHECK_EQUAL(reflection::arity<complex_custom>(), 2);
    BOOST_CHECK_EQUAL(reflection::arity<very_packed_pod>(), 2);
    // BOOST_CHECK_EQUAL(reflection::arity<inherit_complex_pod>(), 4);
}
