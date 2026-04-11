/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/frontend_reader/l1_reader_cache.h"

#include "cloud_topics/logger.h"

#include <seastar/core/loop.hh>

#include <algorithm>
#include <exception>

namespace cloud_topics {

std::optional<cached_l1_reader> l1_reader_cache::take_reader(
  const model::topic_id_partition& tidp, kafka::offset start_offset) {
    if (_gate.is_closed()) {
        return std::nullopt;
    }
    auto it = std::find_if(
      _entries.rbegin(), _entries.rend(), [&](const cache_entry& e) {
          return e.tidp == tidp && e.reader.next_offset == start_offset;
      });
    if (it == _entries.rend()) {
        return std::nullopt;
    }
    auto result = std::move(it->reader);
    _entries.erase_and_dispose(
      _entries.iterator_to(*it), [](cache_entry* e) { delete e; }); // NOLINT
    return result;
}

ss::future<> l1_reader_cache::return_reader(
  const model::topic_id_partition& tidp, cached_l1_reader entry) {
    if (_gate.is_closed() || entry.next_offset > entry.last_object_offset) {
        auto reader = std::move(entry.reader);
        co_return co_await close_reader_safe(std::move(reader));
    }
    std::unique_ptr<l1::object_reader> evicted;
    if (_entries.size() >= _max_cached_readers) {
        _entries.pop_front_and_dispose([&evicted](cache_entry* e) {
            evicted = std::move(e->reader.reader);
            vlog(cd_log.debug, "LRU evicted cached L1 reader for {}", e->tidp);
            delete e; // NOLINT
        });
    }
    // NOLINTNEXTLINE
    auto* e = new cache_entry{
      .reader = std::move(entry),
      .tidp = tidp,
      .atime = ss::lowres_clock::now(),
    };
    _entries.push_back(*e);
    arm_timer();
    if (evicted) {
        co_await close_reader_safe(std::move(evicted));
    }
}

ss::future<> l1_reader_cache::stop() {
    _ttl_timer.cancel();
    co_await _gate.close();

    // Collect all readers for parallel close.
    std::vector<std::unique_ptr<l1::object_reader>> to_close;
    to_close.reserve(_entries.size());
    for (auto& e : _entries) {
        to_close.push_back(std::move(e.reader.reader));
    }
    _entries.clear_and_dispose([](cache_entry* e) { delete e; }); // NOLINT

    co_await ss::max_concurrent_for_each(
      to_close, 16, [](std::unique_ptr<l1::object_reader>& r) {
          return close_reader_safe(std::move(r));
      });
}

ss::future<> l1_reader_cache::evict_stale() {
    auto now = ss::lowres_clock::now();
    // Collect all stale readers without yielding. Yielding mid-iteration
    // would let take_reader() or return_reader() erase entries from the
    // list, invalidating our iterator (use-after-free).
    std::vector<std::unique_ptr<l1::object_reader>> to_close;
    auto it = _entries.begin();
    while (it != _entries.end()) {
        if (now - it->atime > ttl) {
            to_close.push_back(std::move(it->reader.reader));
            vlog(cd_log.debug, "TTL evicted cached L1 reader for {}", it->tidp);
            it = _entries.erase_and_dispose(
              it, [](cache_entry* e) { delete e; }); // NOLINT
        } else {
            ++it;
        }
    }
    arm_timer();
    co_await ss::max_concurrent_for_each(
      to_close, 16, [](std::unique_ptr<l1::object_reader>& r) {
          return close_reader_safe(std::move(r));
      });
}

void l1_reader_cache::arm_timer() {
    if (!_entries.empty() && !_ttl_timer.armed()) {
        _ttl_timer.arm(eviction_interval);
    }
}

ss::future<>
l1_reader_cache::close_reader_safe(std::unique_ptr<l1::object_reader> reader) {
    try {
        co_await reader->close();
    } catch (const std::exception& e) {
        vlog(cd_log.warn, "Exception closing cached L1 reader: {}", e.what());
    }
}

} // namespace cloud_topics
