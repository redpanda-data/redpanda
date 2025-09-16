/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/scheduling_policies.h"

#include "cloud_topics/level_one/compaction/executor.h"
#include "model/fundamental.h"
#include "random/generators.h"

#include <seastar/core/coroutine.hh>

#include <iterator>

namespace cloud_topics::l1 {

chunked_circular_buffer<log_info_and_meta>
dirty_ratio_scheduling_policy::sort_log_infos(
  chunked_vector<log_info_and_meta>&& log_infos) const noexcept {
    std::sort(log_infos.begin(), log_infos.end(), sort_policy{});
    chunked_circular_buffer<log_info_and_meta> logs;
    std::move(log_infos.begin(), log_infos.end(), std::back_inserter(logs));
    return logs;
}

chunked_circular_buffer<log_info_and_meta>
compaction_lag_scheduling_policy::sort_log_infos(
  chunked_vector<log_info_and_meta>&& log_infos) const noexcept {
    std::sort(log_infos.begin(), log_infos.end(), sort_policy{});
    chunked_circular_buffer<log_info_and_meta> logs;
    std::move(log_infos.begin(), log_infos.end(), std::back_inserter(logs));
    return logs;
}

chunked_circular_buffer<log_info_and_meta>
random_scheduling_policy::sort_log_infos(
  chunked_vector<log_info_and_meta>&& log_infos) const noexcept {
    std::shuffle(
      log_infos.begin(), log_infos.end(), random_generators::internal::gen);
    chunked_circular_buffer<log_info_and_meta> logs;
    std::move(log_infos.begin(), log_infos.end(), std::back_inserter(logs));
    return logs;
}

std::unique_ptr<scheduling_policy> make_default_scheduling_policy() {
    return std::make_unique<dirty_ratio_scheduling_policy>();
}

} // namespace cloud_topics::l1
