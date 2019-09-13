#include "raft/timeout_jitter.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

SEASTAR_THREAD_TEST_CASE(base_jitter_gurantees) {
    raft::timeout_jitter jit(100, 75);
    auto base_ms = jit.base_duration().count();
    BOOST_CHECK_EQUAL(base_ms, 100);
    for (auto i = 0; i < 10; ++i) {
        raft::duration_type d = jit();
        BOOST_CHECK(d.count() >= base_ms && d.count() <= base_ms + 75);
    }
}
