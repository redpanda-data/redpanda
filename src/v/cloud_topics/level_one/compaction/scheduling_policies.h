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
#include "model/fundamental.h"

#include <iterator>

namespace cloud_topics::l1 {

class scheduling_policy {
public:
    scheduling_policy() = default;
    scheduling_policy(const scheduling_policy&) = delete;
    scheduling_policy& operator=(const scheduling_policy&) = delete;
    scheduling_policy(scheduling_policy&& other) noexcept = default;
    scheduling_policy& operator=(scheduling_policy&&) noexcept = default;
    virtual ~scheduling_policy() = default;

    virtual cmp_t get_comparator() const noexcept = 0;
};

// Compacts partitions from highest dirty ratio (the ratio of unclean bytes in
// the log to the total log size) to lowest.
class dirty_ratio_scheduling_policy : public scheduling_policy {
public:
    cmp_t get_comparator() const noexcept final;

private:
    struct sort_policy {
        static bool operator()(
          const log_compaction_meta_ptr& a,
          const log_compaction_meta_ptr& b) noexcept {
            vassert(
              a->info_and_ts.has_value() && b->info_and_ts.has_value(),
              "Sorting policy applied to logs without info_and_ts assigned- "
              "concurrency issue?");
            return a->info_and_ts->info.dirty_ratio
                   > b->info_and_ts->info.dirty_ratio;
        }
    };
};

// Compacts partitions from highest compaction lag (the oldest timestamp of
// the first uncompacted record) to lowest.
class compaction_lag_scheduling_policy : public scheduling_policy {
public:
    cmp_t get_comparator() const noexcept final;

private:
    struct sort_policy {
        static bool operator()(
          const log_compaction_meta_ptr& a,
          const log_compaction_meta_ptr& b) noexcept {
            vassert(
              a->info_and_ts.has_value() && b->info_and_ts.has_value(),
              "Sorting policy applied to logs without info_and_ts assigned- "
              "concurrency issue?");
            return a->info_and_ts->info.earliest_dirty_ts
                   < b->info_and_ts->info.earliest_dirty_ts;
        }
    };
};

std::unique_ptr<scheduling_policy> make_default_scheduling_policy();

} // namespace cloud_topics::l1
