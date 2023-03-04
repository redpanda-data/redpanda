#include "utils/hdr_hist.h"

#include <seastar/testing/thread_test_case.hh>

SEASTAR_THREAD_TEST_CASE(test_seastar_histograms_match) {
    using namespace std::chrono_literals;

    hdr_hist a{120s, 1ms};
    hdr_hist b{120s, 1ms};

    std::chrono::microseconds one_hundred_secs = 100s;
    a.record(one_hundred_secs.count());

    const auto logform_a = a.seastar_histogram_logform();
    const auto logform_b = b.seastar_histogram_logform();

    for (size_t idx = 0; idx < logform_a.buckets.size(); ++idx) {
        BOOST_CHECK_EQUAL(
          logform_a.buckets[idx].upper_bound,
          logform_b.buckets[idx].upper_bound);
    }
}
