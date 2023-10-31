#include "metrics/metrics.h"
#include "utils/hdr_hist.h"
#include "utils/log_hist.h"

#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

#include <chrono>
#include <cstdint>
#include <random>

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

namespace {
bool approximately_equal(double a, double b) {
    constexpr double precision_error = 0.0001;
    return std::abs(a - b) <= precision_error;
}

template<typename l_hist>
void validate_public_histograms_equal(const hdr_hist& a, const l_hist& b) {
    const auto logform_a = metrics::report_default_histogram(a);
    const auto logform_b = b.public_histogram_logform();

    BOOST_CHECK_EQUAL(logform_a.sample_count, logform_b.sample_count);
    BOOST_CHECK(
      approximately_equal(logform_a.sample_sum, logform_b.sample_sum));

    for (size_t idx = 0; idx < logform_a.buckets.size(); ++idx) {
        BOOST_CHECK(approximately_equal(
          logform_a.buckets[idx].upper_bound,
          logform_b.buckets[idx].upper_bound));
        BOOST_CHECK_EQUAL(
          logform_a.buckets[idx].count, logform_b.buckets[idx].count);
    }
}
} // namespace

// ensures both the log_hist_public and the public hdr_hist return identical
// seastar histograms for values recorded around bucket bounds.
SEASTAR_THREAD_TEST_CASE(test_public_log_hist_and_hdr_hist_equal_bounds) {
    hdr_hist a;
    log_hist_public b;

    a.record(1);
    b.record(1);

    for (unsigned i = 0; i < 17; i++) {
        auto upper_bound
          = (((unsigned)1 << (log_hist_public::first_bucket_exp + i)) - 1);
        a.record(upper_bound);
        a.record(upper_bound + 1);
        b.record(upper_bound);
        b.record(upper_bound + 1);
    }

    validate_public_histograms_equal(a, b);
}

// ensures both the log_hist_public and the public hdr_hist return identical
// seastar histograms for randomly selected values.
SEASTAR_THREAD_TEST_CASE(test_public_log_hist_and_hdr_hist_equal_rand) {
    hdr_hist a;
    log_hist_public b;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> d(1, (1 << (8 + 17)) - 1);

    for (unsigned i = 0; i < 1'000'000; i++) {
        auto sample = d(gen);
        a.record(sample);
        b.record(sample);
    }

    validate_public_histograms_equal(a, b);
}

// Ensures that an internal histogram is properly converted to a public metrics
// histogram.
SEASTAR_THREAD_TEST_CASE(test_internal_hist_to_public_hist_bounds) {
    hdr_hist a;
    log_hist_internal b;

    a.record(1);
    b.record(1);

    for (unsigned i = 0; i < 17; i++) {
        auto upper_bound
          = (((unsigned)1 << (log_hist_internal::first_bucket_exp + i)) - 1);
        a.record(upper_bound);
        a.record(upper_bound + 1);
        b.record(upper_bound);
        b.record(upper_bound + 1);
    }

    validate_public_histograms_equal(a, b);
}

// Ensures that generating a internal seastar histogram from log_hist_public
// results in the additional buckets for the extended lower bounds having counts
// of zero.
SEASTAR_THREAD_TEST_CASE(test_public_hist_to_internal_hist) {
    log_hist_public a;
    log_hist_internal b;

    a.record(1);
    b.record(1);

    for (unsigned i = 0; i < 17; i++) {
        auto upper_bound
          = (((unsigned)1 << (log_hist_internal::first_bucket_exp + i)) - 1);
        a.record(upper_bound);
        a.record(upper_bound + 1);
        b.record(upper_bound);
        b.record(upper_bound + 1);
    }

    auto pub_to_int_hist = a.internal_histogram_logform();
    auto int_to_int_hist = b.internal_histogram_logform();

    const auto public_ub_exp = 8;
    const auto internal_ub_exp = 3;

    // The buckets in the extended lower bounds should be empty
    for (int i = 0; i < public_ub_exp - internal_ub_exp; i++) {
        BOOST_CHECK_EQUAL(pub_to_int_hist.buckets[i].count, 0);
        BOOST_CHECK_NE(int_to_int_hist.buckets[i].count, 0);
    }
}

SEASTAR_THREAD_TEST_CASE(test_log_hist_measure) {
    log_hist_internal a;

    {
        auto m1 = a.auto_measure();
        ss::sleep(std::chrono::microseconds(1)).get();
        auto m2 = a.auto_measure();
        ss::sleep(std::chrono::microseconds(1)).get();
    }

    auto hist = a.internal_histogram_logform();
    BOOST_CHECK_EQUAL(hist.buckets.back().count, 2);
}

SEASTAR_THREAD_TEST_CASE(test_log_hist_measure_pause) {
    using namespace std::chrono_literals;

    log_hist_internal a;

    {
        auto m1 = a.auto_measure();
        auto m2 = a.auto_measure();

        m1->stop();
        m2->stop();

        auto m1_dur = m1->compute_total_latency();
        auto m2_dur = m2->compute_total_latency();

        ss::sleep(std::chrono::microseconds(1)).get();

        BOOST_CHECK_EQUAL((m1->compute_total_latency() - m1_dur).count(), 0);
        BOOST_CHECK_EQUAL((m2->compute_total_latency() - m2_dur).count(), 0);

        m1->start();
        m2->start();

        ss::sleep(std::chrono::microseconds(1)).get();

        BOOST_CHECK_GT((m1->compute_total_latency() - m1_dur).count(), 1);
        BOOST_CHECK_GT((m2->compute_total_latency() - m2_dur).count(), 1);
    }

    auto hist = a.internal_histogram_logform();
    BOOST_CHECK_EQUAL(hist.buckets.back().count, 2);
}
