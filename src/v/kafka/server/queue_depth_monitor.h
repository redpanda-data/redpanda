/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "base/vlog.h"
#include "kafka/server/logger.h"
#include "utils/queue_depth_control.h"

namespace kafka {

struct qdc_monitor {
    struct config {
        double latency_alpha;
        std::chrono::milliseconds max_latency;
        size_t window_count;
        std::chrono::milliseconds window_size;
        double depth_alpha;
        size_t idle_depth;
        size_t min_depth;
        size_t max_depth;
        std::chrono::milliseconds depth_update_freq;
    };

    exponential_moving_average<std::chrono::steady_clock::duration> ema;
    queue_depth_control qdc;
    ss::timer<ss::lowres_clock> timer;
    ss::lowres_clock::time_point last_update;

    /*
     * ema is initialized with an average of half the max latency. there
     * isn't really a perfect value here. its purpose is to bootstrap the
     * algorithm and is irrelevant after a full time window has elapsed.
     */
    explicit qdc_monitor(const config& cfg)
      : ema(cfg.latency_alpha, cfg.max_latency / 2, cfg.window_count)
      , qdc(
          cfg.max_latency,
          cfg.depth_alpha,
          cfg.idle_depth,
          cfg.min_depth,
          cfg.max_depth) {
        /*
         * start the queue depth control algorithm. on each timer invocation we
         * advance the latency tracker and optionally update the queue depth.
         */
        timer.set_callback([this, update_freq = cfg.depth_update_freq] {
            auto now = ss::lowres_clock::now();
            if ((now - last_update) < update_freq) {
                ema.tick();
                return;
            }
            auto sample = ema.sample();
            ema.tick();
            last_update = now;
            qdc.update(sample);
            vlog(
              klog.debug,
              "Updating queue depth to {} at latency {}",
              qdc.depth(),
              sample);
        });

        timer.arm_periodic(cfg.window_size);
        last_update = ss::lowres_clock::now();
    }
};

} // namespace kafka
