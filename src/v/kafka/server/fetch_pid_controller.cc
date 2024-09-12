/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/server/fetch_pid_controller.h"

#include "metrics/prometheus_sanitize.h"

#include <seastar/core/reactor.hh>

namespace kafka {

fetch_pid_controller::fetch_pid_controller(const ss::scheduling_group& sg)
  : _sg(sg)
  , _p_coeff{config::shard_local_cfg().fetch_pid_p_coeff()}
  , _i_coeff{config::shard_local_cfg().fetch_pid_i_coeff()}
  , _d_coeff{config::shard_local_cfg().fetch_pid_d_coeff()}
  , _target_fetch_sg_util{config::shard_local_cfg()
                            .fetch_pid_target_utilization_fraction()}
  , _max_delay{config::shard_local_cfg().fetch_pid_max_debounce_ms()}
  , _p_coeff_binding{config::shard_local_cfg().fetch_pid_p_coeff.bind()}
  , _i_coeff_binding{config::shard_local_cfg().fetch_pid_i_coeff.bind()}
  , _d_coeff_binding{config::shard_local_cfg().fetch_pid_d_coeff.bind()}
  , _target_fetch_sg_util_binding{config::shard_local_cfg()
                                    .fetch_pid_target_utilization_fraction
                                    .bind()}
  , _max_delay_binding{
      config::shard_local_cfg().fetch_pid_max_debounce_ms.bind()} {
    _p_coeff_binding.watch([this] {
        reset();
        _p_coeff = config::shard_local_cfg().fetch_pid_p_coeff();
    });
    _i_coeff_binding.watch([this] {
        reset();
        _i_coeff = config::shard_local_cfg().fetch_pid_i_coeff();
    });
    _d_coeff_binding.watch([this] {
        reset();
        _d_coeff = config::shard_local_cfg().fetch_pid_d_coeff();
    });
    _target_fetch_sg_util_binding.watch([this] {
        reset();
        _target_fetch_sg_util
          = config::shard_local_cfg().fetch_pid_target_utilization_fraction();
    });
    _max_delay_binding.watch([this] {
        reset();
        _max_delay = config::shard_local_cfg().fetch_pid_max_debounce_ms();
    });
    setup_metrics();
}

std::chrono::milliseconds fetch_pid_controller::current_delay() {
    auto sampled_time = ss::sched_clock::now();
    auto sample = _sg.get_stats().runtime;
    auto total_busy_time = ss::engine().total_busy_time();

    auto dt = sampled_time - _last_sampled_time;
    if (dt < sample_rate) {
        return _last_delay;
    }

    auto db = total_busy_time - _last_total_busy_time;
    if (db < 0s) {
        return _last_delay;
    }
    auto busy = (long double)(db / duration_unit) / (dt / duration_unit);
    if (busy > 1.0) {
        return _last_delay;
    }

    auto ds = sample - _last_sample;
    if (ds < 0 * duration_unit) {
        return _last_delay;
    }
    auto current_runtime = (long double)(ds / duration_unit)
                           / (dt / duration_unit);
    if (current_runtime > 1.0) {
        return _last_delay;
    }

    // If reactor utilization ~ 100% then ensure the fetch runtime is
    // less than or equal to _target_fetch_sg_util.
    //
    // Otherwise ensure that reactor utilization is 100% even if the
    // fetch runtime is above _target_fetch_sg_util.
    constexpr auto target_reactor_util = 0.999;
    auto error = (busy < target_reactor_util)
                   ? (busy - target_reactor_util)
                   : (current_runtime - _target_fetch_sg_util);

    // calculate the proportional
    auto p = _p_coeff * error;

    // calculate the integral
    _error_int += error * (dt / duration_unit);
    // Letting the integral fall below 0 only reduces the responsiveness
    // in cases of sudden load spikes. Like-wise allowing the integral to go
    // above the max delay in cases where the pid controller can't meet the
    // utilization target will reduce responsiveness as well.
    _error_int = std::clamp(
      _error_int, 0.0L, (long double)(_max_delay / duration_unit) / _i_coeff);
    auto i = _i_coeff * _error_int;

    // calculate the derivative
    auto d = (error - _last_error) / (dt / duration_unit);
    d *= _d_coeff;

    auto pid = p + i + d;
    pid = std::max(0.0L, pid);

    auto delay = std::chrono::duration_cast<std::chrono::milliseconds>(
      duration_unit * pid);
    delay = std::min(delay, _max_delay);

    _total_delay += _last_delay * (dt / 1ms);
    _last_delay = delay;
    _last_error = error;
    _last_sample = sample;
    _last_total_busy_time = total_busy_time;
    _last_sampled_time = sampled_time;

    return delay;
}

void fetch_pid_controller::reset() {
    _last_delay = 0ms;
    _error_int = 0.0;
    _last_error = 0.0;
    _last_sample = _sg.get_stats().runtime;
    _last_total_busy_time = ss::engine().total_busy_time();
    _last_sampled_time = ss::sched_clock::now();
}

void fetch_pid_controller::setup_metrics() {
    namespace sm = ss::metrics;
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    _metrics.add_group(
      prometheus_sanitize::metrics_name("kafka_fetch_pid"),
      {
        sm::make_counter(
          "delay_seconds_total",
          [this] { return (_total_delay / 1s) / 1000; },
          sm::description(
            "A running total of fetch delay set by the pid controller.")),
        sm::make_counter(
          "error_total",
          [this] { return (_error_int * 1us) / 1s; },
          sm::description(
            "A running total of error in the fetch PID controller.")),
      },
      {},
      {sm::shard_label});
}

} // namespace kafka
