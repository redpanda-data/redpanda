/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/level_one/maintenance/meta.h"
#include "model/fundamental.h"

namespace cloud_topics::l1 {

class scheduling_policy {
public:
    scheduling_policy() = default;
    scheduling_policy(const scheduling_policy&) = delete;
    scheduling_policy& operator=(const scheduling_policy&) = delete;
    scheduling_policy(scheduling_policy&& other) noexcept = default;
    scheduling_policy& operator=(scheduling_policy&&) noexcept = default;
    virtual ~scheduling_policy() = default;

    virtual cmp_t get_compaction_comparator() const noexcept = 0;
    virtual cmp_t get_leveling_comparator() const noexcept = 0;
};

// Schedules compaction jobs from highest dirty ratio to lowest, and leveling
// jobs from highest levelable ratio to lowest.
class maintenance_ratio_scheduling_policy : public scheduling_policy {
public:
    cmp_t get_compaction_comparator() const noexcept final;
    cmp_t get_leveling_comparator() const noexcept final;

private:
    struct compaction_sort_policy {
        static bool operator()(
          const log_maintenance_meta_ptr& a,
          const log_maintenance_meta_ptr& b) noexcept {
            vassert(
              a->compaction_info_and_ts.has_value()
                && b->compaction_info_and_ts.has_value(),
              "Sorting policy applied to logs without "
              "compaction_info_and_ts assigned- concurrency issue?");
            return a->compaction_info_and_ts->info.dirty_ratio
                   > b->compaction_info_and_ts->info.dirty_ratio;
        }
    };

    struct leveling_sort_policy {
        static bool operator()(
          const log_maintenance_meta_ptr& a,
          const log_maintenance_meta_ptr& b) noexcept {
            vassert(
              a->leveling_info_and_ts.has_value()
                && b->leveling_info_and_ts.has_value(),
              "Sorting policy applied to logs without "
              "leveling_info_and_ts assigned- concurrency issue?");
            return a->leveling_info_and_ts->info.levelable_ratio
                   > b->leveling_info_and_ts->info.levelable_ratio;
        }
    };
};

std::unique_ptr<scheduling_policy> make_default_scheduling_policy();

} // namespace cloud_topics::l1
