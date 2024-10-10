/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/dl_stm/overlay_collection.h"

#include "container/fragmented_vector.h"
#include "model/fundamental.h"

namespace experimental::cloud_topics {

void overlay_collection::append(const dl_overlay& o) noexcept {
    do_append(o);

    // If too many runs are accumulated we need to compact the storage
    // by k-way merging them.
    if (_runs.size() > max_runs) {
        compact();
    }
}

std::optional<dl_overlay> overlay_collection::lower_bound(
  kafka::offset o) const noexcept { // TODO: add overload for the timequery
    // This is guaranteed to be small because max_runs is small.
    std::vector<dl_overlay> results;
    results.reserve(max_runs);
    for (const auto& run : _runs) {
        auto it = std::lower_bound(
          run.values.begin(),
          run.values.end(),
          o,
          [](const dl_overlay& ov, kafka::offset key) {
              return ov.last_offset < key;
          });
        if (it != run.values.end()) {
            results.push_back(*it);
        }
    }
    if (results.empty()) {
        return std::nullopt;
    }
    // Select single value
    // NOTE: this is based on heuristic but it should
    // probably be avoided in the production code.

    // Prefer exact matches
    auto part = std::partition(
      results.begin(), results.end(), [o](const dl_overlay& ov) {
          return ov.base_offset <= o && o <= ov.last_offset;
      });
    if (part != results.begin()) {
        // This indicates that we found at least one run that contains
        // the key.
        results.resize(part - results.begin());
    }
    // Prefer exclusive ownership above anything else
    part = std::partition(
      results.begin(), results.end(), [](const dl_overlay& ov) {
          return ov.ownership == dl_stm_object_ownership::exclusive;
      });
    if (part != results.begin()) {
        // This indicates that we found at least one exclusively owned
        // sorted run. In this case no need to look at anything else.
        results.resize(part - results.begin());
    }

    // Chose the longest run (partial sort). Note that we can't
    // have more than max_runs results. This should work fast.
    std::nth_element(
      results.begin(),
      std::next(results.begin()),
      results.end(),
      [](const dl_overlay& lhs, const dl_overlay& rhs) {
          return (lhs.last_offset - lhs.base_offset)
                 > (rhs.last_offset - rhs.base_offset);
      });
    return results.front();
}

void overlay_collection::compact() noexcept {
    using priority_queue_t = std::
      priority_queue<dl_overlay, chunked_vector<dl_overlay>, std::greater<>>;

    std::deque<sorted_run_t> tmp;
    std::swap(tmp, _runs);

    // Initialize pq
    priority_queue_t pq;
    for (auto& run : tmp) {
        for (const auto& val : run.values) {
            pq.emplace(val);
        }
        run.values.clear();
    }

    // Main loop, pull data from pq and move it into the object
    while (!pq.empty()) {
        auto overlay = pq.top();
        pq.pop();
        do_append(overlay);
    }
    vassert(_runs.size() < max_runs, "Too many runs - {}", _runs.size());
}

void overlay_collection::do_append(const dl_overlay& o) noexcept {
    // Try to accommodate the overlay into existing
    // run.
    bool done = false;
    for (auto& run : _runs) {
        done = run.maybe_append(o); // TODO: clear terms before inserting
        if (done) {
            return;
        }
    }

    // If no run could accommodate the overlay create a new run.
    // This path is also taken if we're inserting the first overlay.
    _runs.emplace_back(o);
}
} // namespace experimental::cloud_topics
