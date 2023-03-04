/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

// Must include first for a seastar transitive type only on GCC
// where metrics_types needs cstdint as an include header
#include <cstdint>

// vectorized types. needed comment to allow clang-format
// header sorting to not resort cstdint
#include "seastarx.h"
#include "static_deleter_fn.h"

#include <seastar/core/metrics_types.hh>
#include <seastar/core/temporary_buffer.hh>

#include <boost/intrusive/list.hpp>
#include <hdr/hdr_histogram.h>

#include <chrono>
#include <memory>
#include <utility>

namespace hist_internal {
using hdr_histogram_ptr = std::
  unique_ptr<hdr_histogram, static_deleter_fn<hdr_histogram, &::hdr_close>>;

inline hdr_histogram_ptr make_unique_hdr_histogram(
  int64_t max_value, int64_t min, int32_t significant_figures) {
    hdr_histogram* hist = nullptr;
    ::hdr_init(min, max_value, significant_figures, &hist);
    return hdr_histogram_ptr(hist);
}
} // namespace hist_internal

// Potentially VERY expensive object. At default granularity is about 4k bytes
class hdr_hist {
public:
    static constexpr int64_t us_per_hour = 3600000000;

    using clock_type = std::chrono::high_resolution_clock;
    /// \brief move-only type to tracking durations
    /// if hdr_hist ptr goes out of scope, it will detach itself
    /// and the recording will simply be ignored.
    class measurement : public boost::intrusive::list_base_hook<> {
    public:
        explicit measurement(hdr_hist& h)
          : _h(std::ref(h))
          , _begin_t(hdr_hist::clock_type::now()) {
            _h.get()._probes.push_back(*this);
        }
        measurement(const measurement&) = delete;
        measurement& operator=(const measurement&) = delete;
        measurement(measurement&& o) noexcept
          : _detached(o._detached)
          , _trace(o._trace)
          , _h(o._h)
          , _begin_t(o._begin_t) {
            o.detach_hdr_hist();
        }
        measurement& operator=(measurement&& o) noexcept {
            if (this != &o) {
                this->~measurement();
                new (this) measurement(std::move(o));
            }
            return *this;
        }
        ~measurement() noexcept {
            if (!_detached) {
                _h.get()._probes.erase(_h.get()._probes.iterator_to(*this));
                // !detached && trace
                // do not move outside of this nested if
                if (_trace) {
                    _h.get().record(compute_duration_micros());
                }
            }
        }

        void set_trace(bool b) { _trace = b; }

    private:
        friend hdr_hist;

        int64_t compute_duration_micros() const {
            return std::chrono::duration_cast<std::chrono::microseconds>(
                     hdr_hist::clock_type::now() - _begin_t)
              .count();
        }

        void detach_hdr_hist() { _detached = true; }

        bool _detached = false;
        bool _trace = true;
        std::reference_wrapper<hdr_hist> _h;
        hdr_hist::clock_type::time_point _begin_t;

        friend std::ostream& operator<<(std::ostream& o, const measurement&);
    };

    explicit hdr_hist(
      int64_t max_value = us_per_hour,
      int64_t min = 1,
      int32_t significant_figures = 1)
      : _hist(hist_internal::make_unique_hdr_histogram(
        max_value, min, significant_figures))
      , _first_discernible_value(min) {}
    hdr_hist(
      std::chrono::microseconds max_value, std::chrono::microseconds min_value)
      : hdr_hist(max_value.count(), min_value.count()) {}
    hdr_hist(hdr_hist&& o) noexcept
      : _probes(std::move(o._probes))
      , _hist(std::move(o._hist))
      , _sample_count(o._sample_count)
      , _sample_sum(o._sample_sum) {}
    hdr_hist& operator=(hdr_hist&& o) noexcept {
        if (this != &o) {
            this->~hdr_hist();
            new (this) hdr_hist(std::move(o));
        }
        return *this;
    }
    hdr_hist(const hdr_hist&) = delete;
    hdr_hist& operator=(const hdr_hist&) = delete;
    ~hdr_hist() noexcept;

    hdr_hist& operator+=(const hdr_hist& o);
    ss::temporary_buffer<char> print_classic() const;
    void record(uint64_t value);
    void record_multiple_times(uint64_t value, uint32_t times);
    void record_corrected(uint64_t value, uint64_t interval);
    // getters
    int64_t get_value_at(double percentile) const;
    double stddev() const;
    double mean() const;
    size_t memory_size() const;
    ss::metrics::histogram seastar_histogram_logform(
      size_t num_buckets = 26,
      int64_t first_value = 10,
      double log_base = 2.0,
      int64_t scale = 1) const;

    std::unique_ptr<measurement> auto_measure();

    void record(std::unique_ptr<measurement> m) {
        record(m->compute_duration_micros());
    }

private:
    friend measurement;
    friend std::ostream& operator<<(std::ostream& o, const hdr_hist& h);

    boost::intrusive::list<measurement> _probes;
    hist_internal::hdr_histogram_ptr _hist;
    uint64_t _sample_count{0};
    uint64_t _sample_sum{0};
    int64_t _first_discernible_value{1};

    friend std::ostream& operator<<(std::ostream& o, const hdr_hist& h);
};

inline std::ostream&
operator<<(std::ostream& o, const std::unique_ptr<hdr_hist::measurement>& m) {
    if (m) {
        return o << *m;
    }
    return o << "{nullptr}";
}
