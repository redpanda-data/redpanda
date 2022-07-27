/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "random/generators.h"
#include "storage/log_reader.h"
#include "storage/readers_cache_probe.h"
#include "storage/types.h"
#include "utils/intrusive_list_helpers.h"
#include "vlog.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/coroutine.hh>

namespace storage {
/**
 * The cache holds reader instances and allows user to query for reader using
 * reader configuration. If any of the readers kept in a cache matches given
 * query its configuration is reset and it is returned to the caller. Caller is
 * responsible for adding readers to the cache using `put()` method. Since
 * readers keep read lock to underlying segments `readers_cache` exposes
 * interface to force readers eviction in face of truncation and segments
 * removal. Readers are evicted from the cache according to LRU policy and
 * automatically when they can not longer be reused (f.e. EOF).
 */
class readers_cache {
public:
    using offset_range = std::pair<model::offset, model::offset>;
    class range_lock_holder {
    public:
        range_lock_holder(offset_range rng, readers_cache* c)
          : _range(std::move(rng))
          , _cache(c) {}

        range_lock_holder(const range_lock_holder&) = delete;
        range_lock_holder(range_lock_holder&& other) noexcept
          : _range(std::move(other._range))
          , _cache(other._cache) {
            other._range.reset();
        }

        range_lock_holder& operator=(const range_lock_holder&) = delete;
        range_lock_holder& operator=(range_lock_holder&& other) noexcept {
            _range = std::move(other._range);
            _cache = other._cache;
            other._range.reset();

            return *this;
        }

        ~range_lock_holder() {
            if (_range) {
                std::erase(_cache->_locked_offset_ranges, _range.value());
            }
        }

    private:
        std::optional<offset_range> _range;
        readers_cache* _cache;
    };
    explicit readers_cache(model::ntp, std::chrono::milliseconds);
    std::optional<model::record_batch_reader>
    get_reader(const log_reader_config&);

    model::record_batch_reader put(std::unique_ptr<log_reader> reader);

    /**
     * Evict readers. No new readers holding log to given offset can be added to
     * the cache under range_lock_holder is destroyed
     */
    ss::future<range_lock_holder> evict_prefix_truncate(model::offset);
    ss::future<range_lock_holder>
    evict_segment_readers(ss::lw_shared_ptr<segment> s);
    ss::future<range_lock_holder> evict_truncate(model::offset);
    ss::future<range_lock_holder> evict_range(model::offset, model::offset);

    ss::future<> stop();
    readers_cache() noexcept = default;
    readers_cache(readers_cache&&) = delete;
    readers_cache(const readers_cache&) = delete;
    readers_cache& operator=(readers_cache&&) = delete;
    readers_cache& operator=(const readers_cache&) = delete;

    ~readers_cache();

private:
    friend struct readers_cache_test_fixture;
    struct entry;
    void touch(entry* e) {
        e->last_used = ss::lowres_clock::now();
        e->_hook.unlink();
        _readers.push_back(*e);
    };

    /**
     * Entry kept in lru readers list
     */
    struct entry {
        model::record_batch_reader make_cached_reader(readers_cache*);
        std::unique_ptr<log_reader> reader;
        ss::lowres_clock::time_point last_used = ss::lowres_clock::now();
        bool valid = true;
        intrusive_list_hook _hook;
    };
    /**
     * RAII based entry lock guard, it touches entry in a cache and handles
     * locking logic. Entry is unlocked when cached reader is destroyed
     */
    struct entry_guard {
        entry_guard(entry_guard&&) noexcept = default;
        entry_guard& operator=(entry_guard&&) noexcept = default;

        entry_guard(const entry_guard&) = delete;
        entry_guard& operator=(const entry_guard&) = delete;

        explicit entry_guard(entry* e, readers_cache* c)
          : _e(e)
          , _cache(c) {}

        ~entry_guard() noexcept {
            _e->_hook.unlink();
            /**
             * we only return reader to cache if it is reusable and wasn't
             * requested to be evicted
             */
            if (_e->reader->is_reusable() && _e->valid) {
                _cache->_readers.push_back(*_e);
            } else {
                _cache->dispose_in_background(_e);
            }
            _cache->_in_use_reader_destroyed.broadcast();
        }

    private:
        entry* _e;
        readers_cache* _cache;
    };

    ss::future<> maybe_evict();
    ss::future<> dispose_entries(intrusive_list<entry, &entry::_hook>);
    void dispose_in_background(intrusive_list<entry, &entry::_hook>);
    void dispose_in_background(entry* e);
    ss::future<> wait_for_no_inuse_readers();
    template<typename Predicate>
    ss::future<> evict_if(Predicate predicate) {
        intrusive_list<entry, &entry::_hook> to_evict;
        // lock reders to make sure no new readers will be added
        for (auto it = _readers.begin(); it != _readers.end();) {
            auto should_evict = predicate(*it);
            if (should_evict) {
                // marking reader as invlid to prevent any further use
                it->valid = false;
                it = _readers.erase_and_dispose(
                  it, [&to_evict](entry* e) { to_evict.push_back(*e); });
            } else {
                ++it;
            }
        }
        for (auto& r : _in_use) {
            if (predicate(r)) {
                // marking reader as invlid to prevent further reuse
                r.valid = false;
            }
        }
        co_await dispose_entries(std::move(to_evict));
    }

    bool intersects_with_locked_range(model::offset, model::offset) const;

    model::ntp _ntp;
    std::chrono::milliseconds _eviction_timeout;
    ss::gate _gate;
    ss::timer<> _eviction_timer;
    readers_cache_probe _probe;
    /**
     * when reader is in use we push it to _in_use intrusive list, otherwise it
     * is stored in _readers.
     */
    intrusive_list<entry, &entry::_hook> _readers;
    intrusive_list<entry, &entry::_hook> _in_use;
    /**
     * When offset range is locked any new readers for given offset will not be
     * added to cache.
     */
    std::vector<offset_range> _locked_offset_ranges;
    ss::condition_variable _in_use_reader_destroyed;
};
} // namespace storage
