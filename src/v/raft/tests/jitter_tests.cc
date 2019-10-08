#include "raft/timeout_jitter.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

SEASTAR_THREAD_TEST_CASE(base_jitter_gurantees) {
    raft::timeout_jitter jit(100, 75);
    auto base_ms = jit.base_duration().count();
    auto low = std::chrono::milliseconds(base_ms);
    auto high = std::chrono::milliseconds(75);
    BOOST_CHECK_EQUAL(base_ms, 100);
    for (auto i = 0; i < 10; ++i) {
        auto now = raft::clock_type::now();
        auto next = jit();
        BOOST_CHECK(next >= now + low && next <= now + low + high);
    }
}
