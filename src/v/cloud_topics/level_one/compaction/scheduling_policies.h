/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/level_one/compaction/meta.h"
#include "container/chunked_circular_buffer.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"

#include <iterator>

namespace cloud_topics::l1 {

class compaction_executor;

class scheduling_policy {
public:
    scheduling_policy() = default;
    scheduling_policy(const scheduling_policy&) = delete;
    scheduling_policy& operator=(const scheduling_policy&) = delete;
    scheduling_policy(scheduling_policy&& other) noexcept = default;
    scheduling_policy& operator=(scheduling_policy&&) noexcept = default;
    virtual ~scheduling_policy() = default;

    // Sorts the input `chunked_vector` of compaction jobs per a desired
    // heuristic and returns a `chunked_circular_buffer` for easy iteration
    // within `schedule_compactions()`.
    virtual chunked_circular_buffer<log_info_and_meta>
    sort_log_infos(chunked_vector<log_info_and_meta>&&) const noexcept = 0;
};

// Compacts partitions from highest dirty ratio (the ratio of unclean bytes in
// the log to the total log size) to lowest.
class dirty_ratio_scheduling_policy : public scheduling_policy {
public:
    chunked_circular_buffer<log_info_and_meta>
    sort_log_infos(chunked_vector<log_info_and_meta>&&) const noexcept final;

private:
    struct sort_policy {
        bool operator()(const log_info_and_meta& a, const log_info_and_meta& b)
          const noexcept {
            return a.info.dirty_ratio > b.info.dirty_ratio;
        }
    };
};

// Compacts partitions from highest compaction lag (the oldest timestamp of
// the first uncompacted record) to lowest.
class compaction_lag_scheduling_policy : public scheduling_policy {
public:
    chunked_circular_buffer<log_info_and_meta>
    sort_log_infos(chunked_vector<log_info_and_meta>&&) const noexcept final;

private:
    struct sort_policy {
        bool operator()(const log_info_and_meta& a, const log_info_and_meta& b)
          const noexcept {
            return a.info.earliest_dirty_ts < b.info.earliest_dirty_ts;
        }
    };
};

// Shuffles all partitions eligible for compaction.
class random_scheduling_policy : public scheduling_policy {
public:
    chunked_circular_buffer<log_info_and_meta>
    sort_log_infos(chunked_vector<log_info_and_meta>&&) const noexcept final;
};

std::unique_ptr<scheduling_policy> make_default_scheduling_policy();

} // namespace cloud_topics::l1
