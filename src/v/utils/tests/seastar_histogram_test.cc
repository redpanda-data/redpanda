#include "utils/hdr_hist.h"
#include "utils/log_hist.h"

#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

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

struct hist_config {
    int64_t scale;
    bool use_approximately_equal;
};

constexpr std::array hist_configs = {
  hist_config{log_hist_public_scale, true}, hist_config{1, false}};

template<typename l_hist>
void validate_histograms_equal(const hdr_hist& a, const l_hist& b) {
    for (auto cfg : hist_configs) {
        const auto logform_a = a.seastar_histogram_logform(
          18, 250, 2.0, cfg.scale);
        const auto logform_b = b.seastar_histogram_logform(cfg.scale);

        BOOST_CHECK_EQUAL(logform_a.sample_count, logform_b.sample_count);
        if (cfg.use_approximately_equal) {
            BOOST_CHECK(
              approximately_equal(logform_a.sample_sum, logform_b.sample_sum));
        } else {
            BOOST_CHECK_EQUAL(logform_a.sample_sum, logform_b.sample_sum);
        }

        for (size_t idx = 0; idx < logform_a.buckets.size(); ++idx) {
            if (cfg.use_approximately_equal) {
                BOOST_CHECK(approximately_equal(
                  logform_a.buckets[idx].upper_bound,
                  logform_b.buckets[idx].upper_bound));
            } else {
                BOOST_CHECK_EQUAL(
                  logform_a.buckets[idx].upper_bound,
                  logform_b.buckets[idx].upper_bound);
            }
            BOOST_CHECK_EQUAL(
              logform_a.buckets[idx].count, logform_b.buckets[idx].count);
        }
    }
}
} // namespace

// ensures both the log_hist_public and the public hdr_hist return identical
// seastar histograms for values recorded around bucket bounds.
SEASTAR_THREAD_TEST_CASE(test_public_log_hist_and_hdr_hist_equal_bounds) {
    using namespace std::chrono_literals;

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

    validate_histograms_equal(a, b);
}

// ensures both the log_hist_public and the public hdr_hist return identical
// seastar histograms for randomly selected values.
SEASTAR_THREAD_TEST_CASE(test_public_log_hist_and_hdr_hist_equal_rand) {
    using namespace std::chrono_literals;

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

    validate_histograms_equal(a, b);
}
