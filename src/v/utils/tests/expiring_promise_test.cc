#include "seastarx.h"
#include "utils/expiring_promise.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/testing/thread_test_case.hh>

// testing that shared_promise doesn't immediately become available after set
SEASTAR_THREAD_TEST_CASE(test_shared_promise_availability) {
    ss::shared_promise<int32_t> promise;
    auto future = promise.get_shared_future();
    BOOST_REQUIRE_EQUAL(promise.available(), false);
    promise.set_value(42);
    // This is an implementation quirk of shared_promise. This happens
    // because of the behavior of then_wrapped() that moves the calling
    // future state into the continuation.
    //
    // Following is the code snippet form shared_promise implementation.
    // if (_original_future._state.valid()) {
    //    _original_future's result is forwarded to each peer.
    //   (void)_original_future.then_wrapped([s = this->shared_from_this()]
    //   (future_type&& f) mutable {
    //   <---- available() is not set here temporarily as _original_future's
    //   state is invalid ---->
    //   s->resolve(std::move(f));
    //  });
    // }
    BOOST_REQUIRE_EQUAL(promise.available(), false);
    future.get();
    BOOST_REQUIRE_EQUAL(promise.available(), true);
}

// testing that expiring_promise immediately becomes available after set
SEASTAR_THREAD_TEST_CASE(test_expiring_promise_availability) {
    expiring_promise<int32_t> promise;
    BOOST_REQUIRE_EQUAL(promise.available(), false);
    promise.set_value(42);
    BOOST_REQUIRE_EQUAL(promise.available(), true);
}
