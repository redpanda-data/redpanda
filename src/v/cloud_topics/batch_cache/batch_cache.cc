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

#include "ssx/future-util.h"
#include "storage/batch_cache.h"
#include "storage/log_manager.h"
#include "storage/ntp_config.h"

#include <seastar/core/preempt.hh>

#include <chrono>

namespace experimental::cloud_topics {

batch_cache::batch_cache(
  storage::log_manager* log_manager, std::chrono::milliseconds gc_interval)
  : _gc_interval(gc_interval)
  , _lm(log_manager) {}

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
    co_await _gate.close();
}

void batch_cache::put(const model::ntp& ntp, const model::record_batch& b) {
    if (_lm == nullptr) {
        return;
    }
    _gate.check();
    auto it = _index.find(ntp);
    if (it == _index.end()) {
        auto cache_ix = _lm->create_cache(storage::with_cache::yes);
        if (!cache_ix.has_value()) {
            return;
        }
        auto [new_it, ok] = _index.insert(std::make_pair(
          ntp,
          std::make_unique<storage::batch_cache_index>(std::move(*cache_ix))));
        if (ok) {
            it = new_it;
        } else {
            return;
        }
    }
    it->second->put(b, storage::batch_cache::is_dirty_entry::no);
}

std::optional<model::record_batch>
batch_cache::get(const model::ntp& ntp, model::offset o) {
    if (_lm == nullptr) {
        return std::nullopt;
    }
    _gate.check();
    if (auto it = _index.find(ntp); it != _index.end()) {
        return it->second->get(o);
    }
    return std::nullopt;
}

ss::future<> batch_cache::cleanup_index_entries() {
    // NOTE: the memory is reclaimed asynchronously.  In some cases
    // the index may no longer reference any live entries.  If this
    // is the case we need to delete the batch_cache_index from the
    // '_index'  collection to avoid accumulating orphaned entries.
    auto it = _index.begin();
    while (it != _index.end()) {
        if (!it->second->empty()) {
            it = _index.erase(it);
        } else {
            ++it;
        }
        if (ss::need_preempt() && it != _index.end()) {
            model::ntp next = it->first;
            co_await ss::yield();
            it = _index.lower_bound(next);
        }
    }
    _cleanup_timer.arm(_gc_interval);
}

} // namespace experimental::cloud_topics
