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

#pragma once

#include "config/configuration.h"
#include "metrics/metrics.h"

using namespace std::chrono_literals;

namespace kafka {

class fetch_pid_controller {
    static constexpr auto duration_unit = 1us;
    static constexpr auto sample_rate = 250us;

public:
    explicit fetch_pid_controller(const ss::scheduling_group& sg);

    /*
     * Returns the delay that should be applied to a given fetch in order to
     * limit the fetch scheduling group to the target reactor utilization.
     */
    std::chrono::milliseconds current_delay();

private:
    void reset();
    void setup_metrics();

    ss::scheduling_group _sg;

    std::chrono::milliseconds _total_delay{0ms};
    std::chrono::milliseconds _last_delay{0ms};
    long double _error_int{0.0};
    long double _last_error{0.0};
    ss::sched_clock::duration _last_sample{0ms};
    ss::sched_clock::duration _last_total_busy_time{0ms};
    ss::sched_clock::time_point _last_sampled_time{ss::sched_clock::now()};

    double _p_coeff;
    double _i_coeff;
    double _d_coeff;
    double _target_fetch_sg_util;
    std::chrono::milliseconds _max_delay;

    config::binding<double> _p_coeff_binding;
    config::binding<double> _i_coeff_binding;
    config::binding<double> _d_coeff_binding;
    config::binding<double> _target_fetch_sg_util_binding;
    config::binding<std::chrono::milliseconds> _max_delay_binding;

    metrics::internal_metric_groups _metrics;
};

} // namespace kafka
