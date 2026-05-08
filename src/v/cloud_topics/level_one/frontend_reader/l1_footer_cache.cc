/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/frontend_reader/l1_footer_cache.h"

#include "cloud_topics/level_one/frontend_reader/level_one_reader_probe.h"
#include "random/simple_time_jitter.h"

#include <seastar/core/coroutine.hh>

namespace cloud_topics::l1 {

l1_footer_cache::l1_footer_cache(
  config::binding<std::chrono::milliseconds> eviction_timeout,
  config::binding<size_t> target_max_size,
  level_one_reader_probe* probe)
  : _eviction_timeout(std::move(eviction_timeout))
  , _target_max_size(std::move(target_max_size))
  , _probe(probe) {
    if (_probe != nullptr) {
        _probe->set_footer_cache_size_fn([this] { return _index.size(); });
    }
    // Drain on any shrink (including to 0); without this, entries
    // inserted before a live config change to a smaller target would
    // sit until the idle timer reaped them.
    _target_max_size.watch([this] { maybe_evict_size(); });
    // Re-arm under the new interval on a live timeout change; otherwise
    // the timer keeps firing at the previous cadence until it expires.
    _eviction_timeout.watch([this] {
        if (_eviction_timer.armed()) {
            _eviction_timer.cancel();
        }
        arm_eviction_timer();
    });
    _eviction_timer.set_callback([this] {
        maybe_evict();
        arm_eviction_timer();
    });
    arm_eviction_timer();
}

l1_footer_cache::~l1_footer_cache() {
    if (_probe != nullptr) {
        // Reset the probe's getter before our storage goes away so that any
        // late metric scrape returns 0 rather than reading freed memory.
        _probe->set_footer_cache_size_fn([]() -> size_t { return 0; });
    }
    vassert(
      _index.empty(), "l1_footer_cache must be stopped before destruction");
}

std::optional<l1_footer_cache::footer_ptr>
l1_footer_cache::get(const object_id& oid) {
    if (_gate.is_closed()) {
        return std::nullopt;
    }
    auto it = _index.find(oid);
    if (it == _index.end()) {
        return std::nullopt;
    }
    auto& e = *it->second;
    e.last_used = ss::lowres_clock::now();
    // Move to back: most-recently-used end of the LRU list.
    // `iterator_to` is UB on an unlinked node; the invariant is that any
    // entry in `_index` is linked in `_lru`, assert it before erasing.
    vassert(
      e._hook.is_linked(),
      "entry in _index but not linked in _lru: oid={}",
      oid);
    _lru.erase(_lru.iterator_to(e));
    _lru.push_back(e);
    return e.cached_footer;
}

l1_footer_cache::footer_ptr
l1_footer_cache::put(object_id oid, footer footer_value) {
    auto ptr = ss::make_lw_shared<const footer>(std::move(footer_value));
    // Skip the insert when shutting down or when the cache is sized to zero
    // (disabled); in both cases just hand back the wrapped pointer without
    // storing. Guarding the insert here, rather than evicting afterwards,
    // avoids inserting an entry only to immediately evict it. Enabledness is
    // implicit in the size limit, mirroring l1_reader_cache.
    if (_gate.is_closed() || _target_max_size() == 0) {
        return ptr;
    }
    _index.erase(oid);
    auto e = std::make_unique<entry>(oid, ptr);
    _lru.push_back(*e);
    _index.emplace(std::move(oid), std::move(e));
    maybe_evict_size();
    return ptr;
}

l1_footer_cache::stats l1_footer_cache::get_stats() const {
    return stats{.cached_footers = _index.size()};
}

ss::future<> l1_footer_cache::stop() {
    if (_eviction_timer.armed()) {
        _eviction_timer.cancel();
    }
    co_await _gate.close();
    _index.clear();
}

void l1_footer_cache::arm_eviction_timer() {
    if (_gate.is_closed()) {
        return;
    }
    auto timeout = _eviction_timeout();
    if (timeout > std::chrono::milliseconds::zero()) {
        _eviction_timer.arm(
          simple_time_jitter<ss::lowres_clock>(timeout).next_duration());
    }
}

void l1_footer_cache::maybe_evict() {
    if (_gate.is_closed()) {
        return;
    }
    auto cutoff = ss::lowres_clock::now() - _eviction_timeout();
    while (!_lru.empty() && _lru.front().last_used < cutoff) {
        _index.erase(_lru.front().oid);
    }
}

void l1_footer_cache::maybe_evict_size() {
    if (_gate.is_closed()) {
        return;
    }
    while (over_size_limit() && !_lru.empty()) {
        _index.erase(_lru.front().oid);
    }
}

bool l1_footer_cache::over_size_limit() const {
    return _index.size() > _target_max_size();
}

} // namespace cloud_topics::l1
