/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/types.h"
#include "config/property.h"
#include "random/simple_time_jitter.h"

#include <chrono>

namespace archival {

/*
 * Utility class that schedules a scrubbing action given the interval and jitter
 * cluster configs. Updates to the cluster configs are also handled.
 *
 * The utility can be used by a fiber that wakes up periodically in order to
 * check whether it should perform an action. It should call the `should_scrub`
 * method of the scheduler in order to determine that. If it does decide to go
 * through with the action, it should call `schedule_next_scrub` once the action
 * is completed.
 *
 * The first scheduled action will happen after the jitter duration. Subsequent
 * actions will happen after the interval and jitter.
 *
 * This class was authored in terms of the cloud storage scrubber, but it's
 * actually generic. If one wishes to reuse in the future, they should rename
 * the class and its methods.
 */
template<typename Clock = std::chrono::system_clock>
class scrubber_scheduler {
public:
    using last_scrub_type = std::function<
      std::tuple<model::timestamp, cloud_storage::scrub_status>()>;

    scrubber_scheduler(
      last_scrub_type get_last_scrub_time,
      config::binding<std::chrono::milliseconds> partial_scrub_interval,
      config::binding<std::chrono::milliseconds> full_scrub_interval,
      config::binding<std::chrono::milliseconds> jitter)
      : _get_last_scrub_time(std::move(get_last_scrub_time))
      , _partial_scrub_interval(std::move(partial_scrub_interval))
      , _full_scrub_interval(std::move(full_scrub_interval))
      , _jitter(std::move(jitter))
      , _jittery_partial_scrub_timer(_partial_scrub_interval(), _jitter())
      , _jittery_full_scrub_timer(_full_scrub_interval(), _jitter()) {
        _partial_scrub_interval.watch([this]() {
            _jittery_partial_scrub_timer
              = simple_time_jitter<model::timestamp_clock>(
                _partial_scrub_interval(), _jitter());
            pick_next_scrub_time();
        });

        _full_scrub_interval.watch([this]() {
            _jittery_full_scrub_timer
              = simple_time_jitter<model::timestamp_clock>(
                _full_scrub_interval(), _jitter());
            pick_next_scrub_time();
        });

        _jitter.watch([this]() {
            _jittery_partial_scrub_timer
              = simple_time_jitter<model::timestamp_clock>(
                _partial_scrub_interval(), _jitter());
            _jittery_full_scrub_timer
              = simple_time_jitter<model::timestamp_clock>(
                _full_scrub_interval(), _jitter());
            pick_next_scrub_time();
        });
    }

    bool should_scrub() const {
        if (_next_scrub_at == model::timestamp::missing()) {
            return false;
        }

        const auto now_ts = model::to_timestamp(Clock::now());
        return now_ts >= _next_scrub_at;
    }

    void pick_next_scrub_time() {
        const auto [at, status] = _get_last_scrub_time();
        const auto first_scrub = at == model::timestamp::missing();

        auto& timer = status == cloud_storage::scrub_status::full
                        ? _jittery_full_scrub_timer
                        : _jittery_partial_scrub_timer;

        if (first_scrub) {
            const auto now = Clock::now();
            _next_scrub_at = model::to_timestamp(
              now + timer.next_jitter_duration());
        } else {
            _next_scrub_at = model::timestamp{
              at()
              + std::chrono::duration_cast<std::chrono::milliseconds>(
                  timer.next_duration())
                  .count()};
        }
    }

    std::optional<std::chrono::milliseconds> until_next_scrub() const {
        if (_next_scrub_at == model::timestamp::missing()) {
            return std::nullopt;
        }

        const auto now_ts = model::to_timestamp(Clock::now());
        if (now_ts >= _next_scrub_at) {
            return std::chrono::milliseconds(0);
        }

        return std::chrono::milliseconds{(_next_scrub_at - now_ts).value()};
    }

private:
    last_scrub_type _get_last_scrub_time;
    config::binding<std::chrono::milliseconds> _partial_scrub_interval;
    config::binding<std::chrono::milliseconds> _full_scrub_interval;
    config::binding<std::chrono::milliseconds> _jitter;

    simple_time_jitter<model::timestamp_clock> _jittery_partial_scrub_timer;
    simple_time_jitter<model::timestamp_clock> _jittery_full_scrub_timer;
    model::timestamp _next_scrub_at;
};

} // namespace archival
