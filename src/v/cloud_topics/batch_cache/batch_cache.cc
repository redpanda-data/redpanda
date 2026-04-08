/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/batch_cache/batch_cache.h"

#include "cloud_topics/logger.h"
#include "config/configuration.h"
#include "ssx/future-util.h"
#include "storage/batch_cache.h"
#include "storage/log_manager.h"
#include "storage/ntp_config.h"

#include <seastar/core/preempt.hh>

#include <chrono>

namespace cloud_topics {

batch_cache::batch_cache(
  storage::log_manager* log_manager, std::chrono::milliseconds gc_interval)
  : _gc_interval(gc_interval)
  , _lm(log_manager)
  , _probe(config::shard_local_cfg().disable_metrics()) {}

batch_cache::batch_cache(
  ss::sharded<storage::api>& log_manager, std::chrono::milliseconds gc_interval)
  : batch_cache(&log_manager.local().log_mgr(), gc_interval) {}

ss::future<> batch_cache::start() {
    _cleanup_timer.set_callback([this] {
        auto gh = _gate.hold();
        ssx::spawn_with_gate(_gate, [this] { return cleanup_index_entries(); });
    });
    _cleanup_timer.arm(_gc_interval);
    return ss::now();
}

ss::future<> batch_cache::stop() {
    _cleanup_timer.cancel();
    for (auto& [_, entry] : _entries) {
        if (entry.monitor) {
            entry.monitor->stop();
        }
    }
    co_await _gate.close();
}

void batch_cache::put(
  const model::topic_id_partition& tidp, const model::record_batch& b) {
    vassert(
      b.term() > model::term_id{-1},
      "Batch without term in the cache: {}",
      b.header());
    if (_lm == nullptr) {
        return;
    }
    _gate.check();
    auto& entry = _entries[tidp];
    if (!entry.index) {
        auto cache_ix = _lm->create_cache(storage::with_cache::yes);
        if (!cache_ix.has_value()) {
            return;
        }
        entry.index = std::make_unique<storage::batch_cache_index>(
          std::move(*cache_ix));
    }
    entry.index->put(b, storage::batch_cache::is_dirty_entry::no);
    _probe.register_put(b.size_bytes());
    if (entry.monitor) {
        entry.monitor->notify(b.last_offset());
    }
}

void batch_cache::notify(
  const model::topic_id_partition& tidp, model::offset last_offset) {
    auto it = _entries.find(tidp);
    if (it != _entries.end() && it->second.monitor) {
        it->second.monitor->notify(last_offset);
    }
}

std::optional<model::record_batch>
batch_cache::get(const model::topic_id_partition& tidp, model::offset o) {
    if (_lm == nullptr) {
        return std::nullopt;
    }
    _gate.check();
    if (
      auto it = _entries.find(tidp); it != _entries.end() && it->second.index) {
        auto& index = *it->second.index;
        auto rb = index.get(o);
        if (rb.has_value()) {
            vassert(
              rb->term() > model::term_id{-1},
              "Batch without term in the cache: {}",
              rb->header());
            vassert(
              rb->base_offset() <= o && o <= rb->last_offset(),
              "Unexpected batch for {}, got range: [{},{}] for offset {}",
              tidp,
              rb->base_offset(),
              rb->last_offset(),
              o);
            _probe.register_get(rb->size_bytes());
        } else {
            // Offset was within the cached range but got evicted.
            _probe.register_miss();
        }
        return rb;
    }
    return std::nullopt;
}

ss::future<> batch_cache::wait_for_offset(
  const model::topic_id_partition& tidp,
  model::offset offset,
  model::offset last_known,
  model::timeout_clock::time_point deadline,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    auto& entry = _entries[tidp];
    if (!entry.monitor) {
        entry.monitor = std::make_unique<offset_monitor<model::offset>>();
        entry.monitor->notify(last_known);
    }
    return entry.monitor->wait(offset, deadline, as);
}

void batch_cache::put_ordered(
  const model::topic_id_partition& tidp,
  chunked_vector<model::record_batch> batches) {
    if (batches.empty() || _lm == nullptr) {
        return;
    }
    _gate.check();

    auto first_base = batches.front().base_offset();
    auto last = batches.back().last_offset();

    // Insert all batches into the cache without notifying the monitor.
    auto& entry = _entries[tidp];
    if (!entry.index) {
        auto cache_ix = _lm->create_cache(storage::with_cache::yes);
        if (!cache_ix.has_value()) {
            return;
        }
        entry.index = std::make_unique<storage::batch_cache_index>(
          std::move(*cache_ix));
    }
    for (const auto& b : batches) {
        vassert(
          b.term() > model::term_id{-1},
          "Batch without term in the cache: {}",
          b.header());
        entry.index->put(b, storage::batch_cache::is_dirty_entry::no);
        _probe.register_put(b.size_bytes());
    }

    if (!entry.monitor) {
        return;
    }

    auto prev = model::prev_offset(first_base);
    if (prev <= entry.monitor->last_applied()) {
        // The batch is contiguous with what the monitor has already seen.
        // Scan forward past `last` in case earlier out-of-order puts left
        // batches beyond this range that are now reachable.
        auto end = entry.index->contiguous_end(model::next_offset(last));
        entry.monitor->notify(std::max(last, end));
        return;
    }

    // Check whether the index covers the gap between the monitor's position
    // and this batch.
    auto gap_start = model::next_offset(entry.monitor->last_applied());
    if (entry.index->has_contiguous_coverage(gap_start, prev)) {
        // The gap is closed. Scan forward past the inserted range to
        // pick up any batches that arrived out of order earlier.
        auto end = entry.index->contiguous_end(model::next_offset(last));
        entry.monitor->notify(std::max(last, end));
    }
}

ss::future<> batch_cache::cleanup_index_entries() {
    // NOTE: the memory is reclaimed asynchronously.  In some cases
    // the index may no longer reference any live entries.  If this
    // is the case we need to delete the batch_cache_index from the
    // '_entries' collection to avoid accumulating orphaned entries.
    auto it = _entries.begin();
    while (it != _entries.end()) {
        auto& entry = it->second;
        // Release empty index
        if (entry.index && entry.index->empty()) {
            entry.index.reset();
        }
        // Erase the entry only when both index and monitor are gone.
        // Don't stop a monitor with active waiters — that would throw
        // abort_requested_exception to readers.
        bool monitor_idle = !entry.monitor || entry.monitor->empty();
        if (!entry.index && monitor_idle) {
            if (entry.monitor) {
                entry.monitor->stop();
            }
            it = _entries.erase(it);
        } else {
            ++it;
        }
        if (ss::need_preempt() && it != _entries.end()) {
            model::topic_id_partition next = it->first;
            co_await ss::yield();
            it = _entries.lower_bound(next);
        }
    }
    _cleanup_timer.arm(_gc_interval);
}

} // namespace cloud_topics
