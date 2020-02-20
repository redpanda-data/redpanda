#include "cluster/notification_latch.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"

#include <seastar/core/timer.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace std::chrono_literals;

SEASTAR_THREAD_TEST_CASE(test_notify_before_timeout) {
    cluster::notification_latch latch;
    ss::timer<> timer;
    timer.set_callback([&latch] { latch.notify(model::offset(10)); });
    timer.arm(50ms);
    auto r = latch.wait_for(model::offset(10), model::no_timeout).get0();
    BOOST_REQUIRE_EQUAL(r, cluster::errc::success);
}

SEASTAR_THREAD_TEST_CASE(test_notify_after_timeout) {
    cluster::notification_latch latch;
    ss::timer<> timer;
    timer.set_callback([&latch] { latch.notify(model::offset(10)); });
    timer.arm(500ms);
    auto r = latch
               .wait_for(model::offset(10), model::timeout_clock::now() + 1ms)
               .get0();

    BOOST_REQUIRE_EQUAL(r, cluster::errc::notification_wait_timeout);
}