// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "boost_hist.h"
#include "utils/hdr_hist.h"
#include "vassert.h"

#include <seastar/core/metrics.hh>

#include <boost/histogram.hpp>

class qd_tracker;

/// A qd_stats tracks *Q*ueing and *D*ispatch latencies
/// separately, and provides a guard object for tracking both.
class qd_stats {
public:
    void setup_metrics(
      ss::metrics::metric_groups& mgs,
      std::string const& metric_name,
      std::string const& description);

    qd_tracker track() noexcept;

    qd_stats();

private:
    boost_hist _q_hist;
    boost_hist _q_depth_hist;
    int _q_depth{0};

    boost_hist _d_hist;
    boost_hist _d_depth_hist;
    int _d_depth{0};

    friend class qd_tracker;
};

class qd_tracker {
public:
    qd_tracker(qd_stats& stats) noexcept;
    qd_tracker(qd_tracker&& rhs) noexcept;
    qd_tracker(qd_tracker&) noexcept = delete;
    ~qd_tracker() noexcept;

    void dispatch();

private:
    qd_stats& _stats;
    bool _queued{false};
    bool _dispatched{false};

    std::unique_ptr<boost_hist::measurement> _hist_measurement;
};
