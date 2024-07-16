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

#include "reflection/type_traits.h"

#include <seastar/core/metrics_types.hh>
#include <seastar/core/shared_ptr.hh>

#include <array>
#include <bit>
#include <chrono>
#include <cstdint>
#include <optional>

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
template<int number_of_buckets, uint64_t first_bucket_upper_bound>
class log_hist {
    static_assert(
      first_bucket_upper_bound >= 1
        && (first_bucket_upper_bound & (first_bucket_upper_bound - 1)) == 0,
      "first bucket bound must be power of 2");

public:
    static constexpr int first_bucket_clz = std::countl_zero(
      first_bucket_upper_bound - 1);
    static constexpr int first_bucket_exp = 64 - first_bucket_clz;

    log_hist()
      : _counts() {}
    log_hist(const log_hist& o) = delete;
    log_hist& operator=(const log_hist&) = delete;
    log_hist(log_hist&& o) = delete;
    log_hist& operator=(log_hist&& o) = delete;

    /*
     * record expects values of that are equivalent to `duration_t::count()`
     * so make sure the input is scaled correctly.
     */
    void record(uint64_t val) {
        _sample_sum += val;
        const unsigned i = std::clamp(
          first_bucket_clz - std::countl_zero(val),
          0,
          static_cast<int>(_counts.size() - 1));
        _counts[i]++;
    }

    template<int64_t _scale, uint64_t _first_bucket_bound, int _bucket_count>
    struct logform_config {
        static constexpr auto bound_is_pow_2 = _first_bucket_bound >= 1
                                               && (_first_bucket_bound
                                                   & (_first_bucket_bound - 1))
                                                    == 0;
        static_assert(
          bound_is_pow_2, "_first_bucket_bound must be a power of 2");

        static constexpr auto scale = _scale;
        static constexpr auto first_bucket_bound = _first_bucket_bound;
        static constexpr auto bucket_count = _bucket_count;
    };

    template<typename cfg>
    seastar::metrics::histogram seastar_histogram_logform() const;
    /*
     * Generates a Prometheus histogram with 18 buckets. The first bucket has an
     * upper bound of 256 - 1 and subsequent buckets have an upper bound of 2
     * times the upper bound of the previous bucket.
     *
     * This is the histogram type used in the `/public_metrics` endpoint
     */
    seastar::metrics::histogram public_histogram_logform() const;
    /*
     * Generates a Prometheus histogram with 26 buckets. The first bucket has an
     * upper bound of 8 - 1 and subsequent buckets have an upper bound of 2
     * times the upper bound of the previous bucket.
     *
     * This is the histogram type used in the `/metrics` endpoint
     */
    seastar::metrics::histogram internal_histogram_logform() const;

    /*
     * Generates a Prometheus histogram with 16 buckets. The first bucket has an
     * upper bound of 4 - 1 and subsequent buckets have an upper bound of 2
     * times the upper bound of the previous bucket.
     *
     * This is the histogram type used for the Kafka read distribution.
     */
    seastar::metrics::histogram read_dist_histogram_logform() const;

    /*
     * Generates a Prometheus histogram with 15 buckets. The first bucket has an
     * upper bound of 1 - 1 and subsequent buckets have an upper bound of 2
     * times the upper bound of the previous bucket.
     *
     * This is the histogram type used for the Kafka client quota distribution.
     */
    seastar::metrics::histogram client_quota_histogram_logform() const;

private:
    std::array<uint64_t, number_of_buckets> _counts;
    uint64_t _sample_sum{0};
};

template<
  class duration_t,
  int number_of_buckets,
  uint64_t first_bucket_upper_bound>
requires detail::is_duration_v<duration_t>
class latency_log_hist {
    using base_histo_t = log_hist<number_of_buckets, first_bucket_upper_bound>;
    using duration_type = duration_t;
    using measurement_canary_t = seastar::lw_shared_ptr<bool>;

public:
    using clock_type = std::chrono::high_resolution_clock;
    static constexpr int first_bucket_exp = base_histo_t::first_bucket_exp;
    latency_log_hist()
      : _canary(seastar::make_lw_shared(true)) {}
    latency_log_hist(const latency_log_hist& o) = delete;
    latency_log_hist& operator=(const latency_log_hist&) = delete;
    latency_log_hist(latency_log_hist&& o) = delete;
    latency_log_hist& operator=(latency_log_hist&& o) = delete;
    ~latency_log_hist() {
        // Notify any active measurements that this object no longer exists.
        *_canary = false;
    }

    /// \brief move-only type to tracking durations
    /// if the log_hist ptr goes out of scope, it will detach itself
    /// and the recording will simply be ignored.
    class measurement {
    public:
        explicit measurement(latency_log_hist& h)
          : _canary(h._canary)
          , _h(std::ref(h))
          , _begin_t(latency_log_hist::clock_type::now())
          , _total_latency(duration_t(0)) {}
        measurement(const measurement&) = delete;
        measurement& operator=(const measurement&) = delete;
        measurement(measurement&& o) noexcept
          : _canary(o._canary)
          , _h(o._h)
          , _begin_t(o._begin_t)
          , _total_latency(o._total_latency) {
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
                _h.get().record(compute_total_latency());
            }
        }

        // Cancels this measurements and prevents any values from
        // being recorded to the underlying histogram.
        void cancel() { _canary = nullptr; }

        // Temporarily stops measuring latency.
        void stop() {
            _total_latency = compute_total_latency();
            _begin_t = std::nullopt;
        }

        // Resumes measuring latency.
        void start() {
            if (!_begin_t.has_value()) {
                _begin_t = latency_log_hist::clock_type::now();
            }
        }

        // Returns the total latency that has been measured so far.
        duration_t compute_total_latency() const {
            if (_begin_t) {
                return _total_latency
                       + std::chrono::duration_cast<duration_t>(
                         latency_log_hist::clock_type::now() - *_begin_t);
            } else {
                return _total_latency;
            }
        }

    private:
        measurement_canary_t _canary;
        std::reference_wrapper<latency_log_hist> _h;
        std::optional<latency_log_hist::clock_type::time_point> _begin_t;
        duration_t _total_latency;
    };

    template<
      typename dur_t,
      typename = std::enable_if_t<detail::is_duration_v<dur_t>, dur_t>>
    void record(dur_t dur) {
        _histo.record(static_cast<uint64_t>(
          std::chrono::duration_cast<duration_t>(dur).count()));
    }

    void record(duration_t::rep dur) {
        _histo.record(static_cast<uint64_t>(dur));
    }

    std::unique_ptr<measurement> auto_measure() {
        return std::make_unique<measurement>(*this);
    }

    seastar::metrics::histogram public_histogram_logform() const;

    seastar::metrics::histogram internal_histogram_logform() const;

    seastar::metrics::histogram read_dist_histogram_logform() const;

    seastar::metrics::histogram client_quota_histogram_logform() const;

private:
    friend measurement;

    log_hist<number_of_buckets, first_bucket_upper_bound> _histo;
    // Used to inform measurements whether `latency_log_hist` has been destroyed
    measurement_canary_t _canary;
};

/*
 * This histogram produces indentical results as the public metric's `hdr_hist`.
 * So if this histogram and `hdr_hist` are create and have the same values
 * recorded to them then `log_hist_public::seastar_histogram_logform(1000000)`
 * will produce the same seastar histogram as
 * `metrics::report_default_histogram(hdr_hist)`.
 */
using log_hist_public = latency_log_hist<std::chrono::microseconds, 18, 256ul>;

/*
 * This histogram produces results that are similar, but not indentical to the
 * internal metric's `hdr_hist`. Some of the first buckets will have the
 * following bounds; [log_hist_internal upper bounds, internal hdr_hist upper
 * bounds] [8, 10], [16, 20], [32, 41], [64, 83], [128, 167], [256, 335]
 */
using log_hist_internal = latency_log_hist<std::chrono::microseconds, 26, 8ul>;

/*
 * This histogram has units of minutes instead of microseconds, and is used for
 * measuring the Kafka read distribution on the scale of less than 4 minutes in
 * the first bucket to greater than 91 days in the last bucket.
 */
using log_hist_read_dist = latency_log_hist<std::chrono::minutes, 16, 4ul>;

/*
 * This histogram has units of milliseconds instead of microseconds, and is
 * used for measuring the Kafka client quota delays on the scale of less than
 * 1 milliseconds in the first bucket to greater than 32 seconds in the last
 * bucket.
 */
using log_hist_client_quota
  = latency_log_hist<std::chrono::milliseconds, 15, 1ul>;
