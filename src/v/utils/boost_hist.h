// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "seastarx.h"

#include <seastar/core/metrics.hh>

#include <boost/histogram.hpp>
#include <boost/intrusive/list.hpp>

// Wrap a boost::histogram::histogram with the extra state
// and  methods that redpanda needs.
class boost_hist {
    using inner = boost::histogram::histogram<
      std::tuple<boost::histogram::axis::
                   regular<double, boost::histogram::axis::transform::log>>>;

public:
    using clock_type = std::chrono::high_resolution_clock;
    /// \brief move-only type to tracking durations
    /// if boost_hist ptr goes out of scope, it will detach itself
    /// and the recording will simply be ignored.
    class measurement : public boost::intrusive::list_base_hook<> {
    public:
        explicit measurement(boost_hist& h)
          : _h(std::ref(h))
          , _begin_t(boost_hist::clock_type::now()) {
            _h.get()._measurements.push_back(*this);
        }
        measurement(const measurement&) = delete;
        measurement& operator=(const measurement&) = delete;
        measurement(measurement&& o) noexcept
          : _detached(o._detached)
          , _trace(o._trace)
          , _h(o._h)
          , _begin_t(o._begin_t) {
            o.detach_boost_hist();
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
                _h.get()._measurements.erase(
                  _h.get()._measurements.iterator_to(*this));
                // !detached && trace
                // do not move outside of this nested if
                if (_trace) {
                    _h.get().record(compute_duration_micros());
                }
            }
        }

        void set_trace(bool b) { _trace = b; }

    private:
        friend boost_hist;

        int64_t compute_duration_micros() const {
            return std::chrono::duration_cast<std::chrono::microseconds>(
                     boost_hist::clock_type::now() - _begin_t)
              .count();
        }

        void detach_boost_hist() { _detached = true; }

        bool _detached = false;
        bool _trace = true;
        std::reference_wrapper<boost_hist> _h;
        boost_hist::clock_type::time_point _begin_t;

        friend std::ostream& operator<<(std::ostream& o, const measurement&);
    };

    boost_hist();
    boost_hist(size_t nbuckets, double lower, double upper);
    ss::metrics::histogram to_seastar();

    inline void record(uint64_t value) {
        _hist(value);
        _sum += value;
    }
    inner& hist() { return _hist; }

    std::unique_ptr<measurement> auto_measure();

private:
    inner _hist;
    uint64_t _sum{0};

    boost::intrusive::list<measurement> _measurements;
};