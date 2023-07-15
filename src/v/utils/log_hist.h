/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <seastar/core/metrics_types.hh>
#include <seastar/core/shared_ptr.hh>

#include <boost/intrusive/list.hpp>

#include <bit>
#include <chrono>
#include <cstdint>
#include <vector>

/*
 * A histogram implementation
 * The buckets upper bounds are powers of 2 minus 1.
 * `first_bucket_upper_bound` therefore must be a power of 2.
 * The number of values represented by each bucket increases by powers 2 as
 * well.
 *
 * Assume `number_of_buckets` is 4 and `first_bucket_upper_bound` is 16 the
 * bucket value ranges are;
 *
 * [1, 16), [16, 32), [32, 64), [64, 128)
 *
 * And if 1, 16, 32, and 33 are recorded the buckets will have the following
 * counts;
 *
 * [1, 16)   = 1
 * [16, 32)  = 1
 * [32, 64)  = 2
 * [64, 128) = 0
 */
template<
  typename duration_t,
  int number_of_buckets,
  uint64_t first_bucket_upper_bound>
class log_hist {
    static_assert(
      first_bucket_upper_bound >= 1
        && (first_bucket_upper_bound & (first_bucket_upper_bound - 1)) == 0,
      "first bucket bound must be power of 2");

    using measurement_canary_t = seastar::lw_shared_ptr<bool>;

public:
    static constexpr int first_bucket_clz = std::countl_zero(
      first_bucket_upper_bound - 1);
    static constexpr int first_bucket_exp = 64 - first_bucket_clz;

    using clock_type = std::chrono::high_resolution_clock;

    /// \brief move-only type to tracking durations
    /// if the log_hist ptr goes out of scope, it will detach itself
    /// and the recording will simply be ignored.
    class measurement {
    public:
        explicit measurement(log_hist& h)
          : _canary(h._canary)
          , _h(std::ref(h))
          , _begin_t(log_hist::clock_type::now()) {}
        measurement(const measurement&) = delete;
        measurement& operator=(const measurement&) = delete;
        measurement(measurement&& o) noexcept
          : _canary(o._canary)
          , _h(o._h)
          , _begin_t(o._begin_t) {
            o.cancel();
        }
        measurement& operator=(measurement&& o) noexcept {
            if (this != &o) {
                this->~measurement();
                new (this) measurement(std::move(o));
            }
            return *this;
        }
        ~measurement() noexcept {
            if (_canary && *_canary) {
                _h.get().record(compute_duration());
            }
        }

        // Cancels this measurements and prevents any values from
        // being recorded to the underlying histogram.
        void cancel() { _canary = nullptr; }

    private:
        int64_t compute_duration() const {
            return std::chrono::duration_cast<duration_t>(
                     log_hist::clock_type::now() - _begin_t)
              .count();
        }

        measurement_canary_t _canary;
        std::reference_wrapper<log_hist> _h;
        log_hist::clock_type::time_point _begin_t;
    };

    std::unique_ptr<measurement> auto_measure() {
        return std::make_unique<measurement>(*this);
    }

    log_hist()
      : _canary(seastar::make_lw_shared(true))
      , _counts(number_of_buckets) {}

    ~log_hist() {
        // Notify any active measurements that this object no longer exists.
        *_canary = false;
    }

    /*
     * record expects values of that are equivalent to `duration_t::count()`
     * so make sure the input is scaled correctly.
     */
    void record(uint64_t val) {
        _sample_sum += val;
        const int i = std::clamp(
          first_bucket_clz - std::countl_zero(val),
          0,
          static_cast<int>(_counts.size() - 1));
        _counts[i]++;
    }

    void record(std::unique_ptr<measurement> m) {
        record(m->compute_duration());
    }

    seastar::metrics::histogram seastar_histogram_logform(int64_t scale) const {
        seastar::metrics::histogram hist;
        hist.buckets.resize(_counts.size());
        hist.sample_sum = static_cast<double>(_sample_sum)
                          / static_cast<double>(scale);

        uint64_t cumulative_count = 0;
        for (uint64_t i = 0; i < _counts.size(); i++) {
            auto& bucket = hist.buckets[i];

            cumulative_count += _counts[i];
            bucket.count = cumulative_count;
            uint64_t unscaled_upper_bound = ((uint64_t)1
                                             << (first_bucket_exp + i))
                                            - 1;
            bucket.upper_bound = static_cast<double>(unscaled_upper_bound)
                                 / static_cast<double>(scale);
        }

        hist.sample_count = cumulative_count;
        return hist;
    }

private:
    friend measurement;

    // Used to inform measurements whether `log_hist` has been destroyed
    measurement_canary_t _canary;

    std::vector<uint64_t> _counts;
    uint64_t _sample_sum{0};
};

/*
 * This histogram produces indentical results as the public metric's `hdr_hist`.
 * So if this histogram and `hdr_hist` are create and have the same values
 * recorded to them then `log_hist_public::seastar_histogram_logform(1000000)`
 * will produce the same seastar histogram as
 * `ssx::metrics::report_default_histogram(hdr_hist)`.
 */
using log_hist_public = log_hist<std::chrono::microseconds, 18, 256>;
static constexpr int64_t log_hist_public_scale = 1'000'000;

/*
 * This histogram produces results that are similar, but not indentical to the
 * internal metric's `hdr_hist`. Some of the first buckets will have the
 * following bounds; [log_hist_internal upper bounds, internal hdr_hist upper
 * bounds] [8, 10], [16, 20], [32, 41], [64, 83], [128, 167], [256, 335]
 */
using log_hist_internal = log_hist<std::chrono::microseconds, 26, 8>;
