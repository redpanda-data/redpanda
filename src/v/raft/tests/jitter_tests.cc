#include "raft/timeout_jitter.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

using namespace std::chrono_literals; // NOLINT

SEASTAR_THREAD_TEST_CASE(base_jitter_gurantees) {
    raft::timeout_jitter jit(100ms, 75ms);
    auto const low = jit.base_duration();
    auto const high = jit.base_duration() + 75ms;
    BOOST_CHECK_EQUAL(low.count(), (100ms).count());
    for (auto i = 0; i < 10; ++i) {
        auto now = raft::clock_type::now();
        auto next = jit();
        BOOST_CHECK(next >= now + low && next <= now + high);
    }
}
