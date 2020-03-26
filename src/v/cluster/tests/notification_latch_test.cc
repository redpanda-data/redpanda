#include "cluster/errc.h"
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

#if 0
SEASTAR_THREAD_TEST_CASE(destroy_before_notify_broken_promise) {
    ss::future<cluster::errc> fut = ss::make_ready_future<cluster::errc>(
      cluster::errc::success);
    {
        cluster::notification_latch latch;
        ss::timer<> timer;
        timer.set_callback([&latch] { latch.notify(model::offset(10)); });
        timer.arm(50ms);
        fut = latch.wait_for(
          model::offset(10), model::timeout_clock::now() + 100ms);
        // stop before destroying
        latch.stop();
    }

    BOOST_REQUIRE_EQUAL(fut.get0(), cluster::errc::shutting_down);
}
#endif