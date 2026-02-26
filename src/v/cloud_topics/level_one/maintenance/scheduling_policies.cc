/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/maintenance/scheduling_policies.h"

namespace cloud_topics::l1 {

cmp_t maintenance_ratio_scheduling_policy::get_compaction_comparator()
  const noexcept {
    return compaction_sort_policy{};
}

cmp_t maintenance_ratio_scheduling_policy::get_leveling_comparator()
  const noexcept {
    return leveling_sort_policy{};
}

std::unique_ptr<scheduling_policy> make_default_scheduling_policy() {
    return std::make_unique<maintenance_ratio_scheduling_policy>();
}

} // namespace cloud_topics::l1
