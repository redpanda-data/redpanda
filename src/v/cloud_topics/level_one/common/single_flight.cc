/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/single_flight.h"

#include "base/vassert.h"

namespace cloud_topics::l1 {

single_flight::join_result single_flight::join_or_lead(
  const std::filesystem::path& key, ss::abort_source& as) {
    // NOTE: Keep this function synchronous to ensure entries map integrity.
    if (auto it = _entries.find(key); it != _entries.end()) {
        return {
          .kind = join_kind::merger,
          .merge_future = it->second.get_shared_future(as)};
    }

    if (_entries.size() >= _max_entries) {
        return {.kind = join_kind::at_capacity, .merge_future = std::nullopt};
    }

    auto [it, inserted] = _entries.emplace(key, ss::shared_promise<outcome>{});
    vassert(inserted, "single_flight: concurrent insert for {}", key.native());
    return {.kind = join_kind::leader, .merge_future = std::nullopt};
}

void single_flight::release_leader(
  const std::filesystem::path& key, outcome o) noexcept {
    auto it = _entries.find(key);
    vassert(
      it != _entries.end(),
      "single_flight: entry for {} erased outside release_leader",
      key.native());
    it->second.set_value(o);
    _entries.erase(it);
}

} // namespace cloud_topics::l1
