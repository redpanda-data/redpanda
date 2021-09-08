// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "qd_tracker.h"

#include "prometheus/prometheus_sanitize.h"
#include "ssx/sformat.h"

namespace bh = boost::histogram;

qd_stats::qd_stats()
  : _q_depth_hist(10, 1, 512)
  , _d_depth_hist(10, 1, 512) {}

qd_tracker qd_stats::track() noexcept { return qd_tracker(*this); }

void qd_stats::setup_metrics(
  ss::metrics::metric_groups& mgs,
  const std::string& group_name,
  const std::string& description,
  std::vector<ss::metrics::label_instance>& labels) {
    namespace sm = ss::metrics;
    mgs.add_group(
      prometheus_sanitize::metrics_name(group_name),
      {
        sm::make_histogram(
          "q_t",
          [this] { return _q_hist.to_seastar(); },
          sm::description(ssx::sformat("Queuing latency of {}", description)),
          labels),
        sm::make_histogram(
          "d_t",
          [this] { return _d_hist.to_seastar(); },
          sm::description(ssx::sformat("Dispatch latency of {}", description)),
          labels),
        sm::make_histogram(
          "q_d_hist",
          [this] { return _q_depth_hist.to_seastar(); },
          sm::description(
            ssx::sformat("Queueing depth distribution of {}", description)),
          labels),
        sm::make_histogram(
          "d_d_hist",
          [this] { return _d_depth_hist.to_seastar(); },
          sm::description(
            ssx::sformat("Dispatch depth distribution of {}", description)),
          labels),
        sm::make_gauge(
          "q_d",
          [this] { return _q_depth; },
          sm::description(ssx::sformat("Queuing depth of {}", description)),
          labels),
        sm::make_gauge(
          "d_d",
          [this] { return _d_depth; },
          sm::description(ssx::sformat("Dispatch depth of {}", description)),
          labels),
      });
}

qd_tracker::~qd_tracker() noexcept {
    if (_dispatched) {
        _stats._d_depth -= 1;
    }
    if (_queued) {
        _stats._q_depth -= 1;
    }
}

qd_tracker::qd_tracker(qd_stats& stats) noexcept
  : _stats(stats)
  , _queued(true) {
    _hist_measurement = _stats._q_hist.auto_measure();
    _stats._q_depth += 1;
    _stats._q_depth_hist.record(_stats._q_depth);
}

qd_tracker::qd_tracker(qd_tracker&& rhs) noexcept
  : _stats(rhs._stats)
  , _queued(rhs._queued)
  , _dispatched(rhs._dispatched)
  , _hist_measurement(std::move(rhs._hist_measurement)) {
    rhs._dispatched = false;
    rhs._queued = false;
}

void qd_tracker::dispatch() {
    if (_dispatched || !_queued) {
        // In case someone e.g. retries requests and calls
        // dispatched more than once: just ignore it if
        // we're in an unexpected state.
        return;
    }
    _dispatched = true;
    _queued = false;

    _stats._q_depth -= 1;
    _stats._d_depth += 1;
    _stats._d_depth_hist.record(_stats._d_depth);

    // Implicitly record the queue stage timing by destroying
    // the `measurement` for it, and start measuring dispatch.
    _hist_measurement = _stats._d_hist.auto_measure();
}
