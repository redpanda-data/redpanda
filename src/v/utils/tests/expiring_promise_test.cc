#include "base/seastarx.h"
#include "utils/expiring_promise.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/testing/thread_test_case.hh>

// testing that expiring_promise immediately becomes available after set
SEASTAR_THREAD_TEST_CASE(test_expiring_promise_availability) {
    expiring_promise<int32_t> promise;
    BOOST_REQUIRE_EQUAL(promise.available(), false);
    promise.set_value(42);
    BOOST_REQUIRE_EQUAL(promise.available(), true);
}
