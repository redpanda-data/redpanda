// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "storage/backlog_controller.h"

#include "base/vlog.h"
#include "config/configuration.h"
#include "metrics/prometheus_sanitize.h"
#include "ssx/future-util.h"
#include "ssx/sformat.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/reactor.hh>

#include <limits>

namespace storage {

backlog_controller_config::backlog_controller_config(
  double kp,
  double ki,
  double kd,
  int64_t normalization_factor,
  int64_t sp,
  int initial_shares,
  std::chrono::milliseconds interval,
  ss::scheduling_group sg,
  ss::io_priority_class iop,
  int min,
  int max)
  : proportional_coeff(kp)
  , integral_coeff(ki)
  , derivative_coeff(kd)
  , normalization_factor(normalization_factor)
  , setpoint(sp)
  , initial_shares(initial_shares)
  , sampling_interval(interval)
  , scheduling_group(sg)
  , io_priority(iop)
  , min_shares(min)
  , max_shares(max) {}

backlog_controller::backlog_controller(
  std::unique_ptr<sampler> sampler,
  ss::logger& logger,
  backlog_controller_config cfg)
  : _sampler(std::move(sampler))
  , _log(logger)
  , _proportional_coeff(cfg.proportional_coeff)
  , _integral_coeff(cfg.integral_coeff)
  , _derivative_coeff(cfg.derivative_coeff)
  , _norm(cfg.normalization_factor)
  , _sampling_interval(cfg.sampling_interval)
  , _scheduling_group(cfg.scheduling_group)
  , _io_priority(cfg.io_priority)
  , _setpoint(cfg.setpoint / _norm)
  , _current_shares(cfg.initial_shares)
  , _min_shares(cfg.min_shares)
  , _max_shares(cfg.max_shares) {}

ss::future<> backlog_controller::start() {
    _current_backlog = co_await _sampler->sample_backlog() / _norm;
    _prev_error = _setpoint - _current_backlog;
    _sampling_timer.set_callback([this] {
        ssx::spawn_with_gate(_gate, [this] {
            return update().then([this] {
                if (!_gate.is_closed()) {
                    _sampling_timer.arm(_sampling_interval);
                }
            });
        });
    });
    _sampling_timer.arm(_sampling_interval);
}

void backlog_controller::update_setpoint(int64_t v) {
    vlog(_log.info, "new setpoint value: {}", v);
    _setpoint = v;
}

ss::future<> backlog_controller::stop() {
    _sampling_timer.cancel();
    return _gate.close();
}

ss::future<> backlog_controller::update() {
    _current_backlog = co_await _sampler->sample_backlog() / _norm;
    auto current_err = _setpoint - _current_backlog;

    static constexpr int64_t max_error = std::numeric_limits<int32_t>::max();
    _error_integral = std::clamp(
      _error_integral + current_err, -max_error, max_error);

    auto update = static_cast<double>(current_err);
    if (_integral_coeff != 0.00) {
        update += _integral_coeff * static_cast<double>(_error_integral);
    }
    if (_derivative_coeff != 0.00) {
        update += _derivative_coeff
                  * static_cast<double>(current_err - _prev_error);
    }
    update *= _proportional_coeff;
    _current_shares = std::clamp(
      static_cast<int>(update), _min_shares, _max_shares);
    vlog(
      _log.trace,
      "state update: {{setpoint: {}, current_backlog: {}, current_error: {}, "
      "prev_error: {}, shares_update: {}, current_share: {} }}",
      _setpoint,
      _current_backlog,
      current_err,
      _prev_error,
      update,
      _current_shares);

    // update error sample
    _prev_error = current_err;

    co_await set();
}

ss::future<> backlog_controller::set() {
    vlog(_log.debug, "updating shares {}", _current_shares);
    _scheduling_group.set_shares(static_cast<float>(_current_shares));
    return _io_priority.update_shares(_current_shares);
}

void backlog_controller::setup_metrics(const ss::sstring& controller_label) {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }
    namespace sm = ss::metrics;
    _metrics.add_group(
      prometheus_sanitize::metrics_name(
        ssx::sformat("{}:backlog:controller", controller_label)),
      {
        sm::make_gauge(
          "error",
          [this] { return _prev_error; },
          sm::description("current controller error, i.e difference between "
                          "set point and backlog size")),
        sm::make_gauge(
          "shares",
          [this] { return _current_shares; },
          sm::description("controller output, i.e. number of shares")),
        sm::make_gauge(
          "backlog_size",
          [this] { return _current_backlog; },
          sm::description("controller backlog")),

      });
}

} // namespace storage
