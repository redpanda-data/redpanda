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

#include "cluster/self_test_rpc_types.h"
#include "utils/hdr_hist.h"

#include <seastar/core/lowres_clock.hh>

namespace cluster::self_test {

class omit_metrics_measurement_exception : public std::exception {};

class omit_measure_timed_out_exception : public std::exception {};

template<typename Timepoint>
uint64_t time_since_epoch(Timepoint tp) {
    return std::chrono::duration_cast<std::chrono::seconds>(
             tp.time_since_epoch())
      .count();
}

class metrics {
public:
    explicit metrics(int64_t max_value_hist)
      : _hist(max_value_hist) {}

    template<typename Fn>
    requires requires(Fn fn) {
        { fn() } -> std::same_as<ss::future<size_t>>;
    }
    ss::future<> measure(Fn fn) {
        auto measurement = _hist.auto_measure();
        try {
            auto bytes = co_await fn();
            _bytes_operated += bytes;
            _num_requests++;
        } catch (const omit_metrics_measurement_exception&) {
            measurement->set_trace(false);
        } catch (const omit_measure_timed_out_exception&) {
            _number_of_timeouts++;
            measurement->set_trace(false);
        } catch (...) {
            measurement->set_trace(false);
            throw;
        }
    }

    void set_start_end_time(
      ss::lowres_system_clock::time_point start,
      ss::lowres_system_clock::time_point end) {
        _start_time = start;
        _end_time = end;
    }

    void set_total_time(ss::lowres_clock::duration t) { _total_time = t; }

    size_t iops() const {
        const auto secs = std::chrono::duration_cast<std::chrono::seconds>(
                            _total_time)
                            .count();
        return secs == 0 ? 0 : _num_requests / secs;
    }

    size_t throughput_bytes_sec() const {
        const auto secs = std::chrono::duration_cast<std::chrono::seconds>(
                            _total_time)
                            .count();
        return secs == 0 ? 0 : (_bytes_operated / secs);
    }

    const hdr_hist& get_hist() const { return _hist; }

    size_t get_number_of_timeouts() const { return _number_of_timeouts; }

    self_test_result to_st_result() const {
        return self_test_result{
          .p50 = static_cast<double>(_hist.get_value_at(50.0)),
          .p90 = static_cast<double>(_hist.get_value_at(90.0)),
          .p99 = static_cast<double>(_hist.get_value_at(99.0)),
          .p999 = static_cast<double>(_hist.get_value_at(99.9)),
          .max = static_cast<double>(_hist.get_value_at(100.0)),
          .rps = iops(),
          .bps = throughput_bytes_sec(),
          .timeouts = static_cast<uint32_t>(_number_of_timeouts),
          .start_time = get_start_time_since_epoch(),
          .end_time = get_end_time_since_epoch(),
          .duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            _total_time)};
    }

    uint64_t get_start_time_since_epoch() const {
        return time_since_epoch(_start_time);
    }

    uint64_t get_end_time_since_epoch() const {
        return time_since_epoch(_end_time);
    }

private:
    ss::lowres_clock::duration _total_time{};
    ss::lowres_system_clock::time_point _start_time{};
    ss::lowres_system_clock::time_point _end_time{};
    size_t _number_of_timeouts{0};
    size_t _bytes_operated{0};
    uint64_t _num_requests{0};
    hdr_hist _hist;
};

} // namespace cluster::self_test
