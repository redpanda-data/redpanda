/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "model/record.h"
#include "utils/intrusive_list_helpers.h"
#include "vassert.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/weak_ptr.hh>

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>

#include <limits>
#include <type_traits>

namespace storage {

class batch_cache_index;

/**
 * The batch cache system consists of two components. The `batch_cache` is a
 * global (per-shard) LRU cache of batches stored in memory. The second
 * component is the `batch_cache_index` which presents an offset-based index
 * into the global cache.
 *
 *    ┌per-shard lru cache──────────────────────────────────────────────┐
 *    │           ┌─────┐    ┌─────┐         ┌─────┐                    │
 *    │           │Batch│    │Batch│   ...   │Batch│                    │
 *    │        ▲  └─────┘    └─────┘▲        └─────┘           ▲        │
 *    └────────┼────────────────────┼──────────────────────────┼────────┘
 *             │                    │                          │
 *    ┌─────────────────┐  ┌─────────────────┐        ┌─────────────────┐
 *    │{ off -> batch } │  │{ off -> batch } │  ...   │{ off -> batch } │
 *    └─────────────────┘  └─────────────────┘        └─────────────────┘
 *
 * The typical usage is to arrange for a single LRU cache to exist on each
 * shard, and create an index for each functional grouping of batches. For
 * example, a batch cache index is created for each log segment, all of which
 * share the same LRU cache.
 *
 * The LRU cache serves as an entry point for the Seastar memory reclaimer.
 * During a low-memory event Seastar may make an upcall to the LRU cache to free
 * memory. When memory is reclaimed cache entries are invalidated. Since this
 * occurs asynchronously, callers must check that their entries are valid before
 * dereferencing.
 *
 * Reclaim concurrency
 * ===================
 *
 * The synchronous reclaimer creates a challenge for the batch cache which
 * internally causes allocations to occur which may invoke the reclaimer.
 *
 * Batch cache operations that hold a reference to an entry in the cache need to
 * be careful that any heap allocation may cause the held reference to be
 * _concurrently_ freed/invalidated.
 *
 * For example, given a live entry in the cache:
 *
 *    auto e = _index.find(..);
 *    assert(e);
 *
 * doing something such as `e->batch.share()` internally causes allocations
 * which may trigger the reclaimer. if the reclaimer selects `e` for reclaiming,
 * then `e->batch.share()` will run concurrently with the destructor of the
 * batch (not technically concurently--but interleaved with share() in ways that
 * are not safe).
 *
 * If an operation may perform an allocation use entry::pin/unpin to guard the
 * reference which will force the reclaimer to skip the entry.
 *
 * IMPORTANT: this is a viral leaky abstraction solution. it relies on all code
 * paths whose call sites are inside the batch cache to have their allocation
 * behavior known. that is generally an aspect of interfaces that are never
 * guaranteed. so, good luck. if you find yourself with mysterious crashes in
 * the future, consider other solutions like blocking the reclaimer or only
 * allowing asynchronous reclaims while executing within the batch catch.
 *
 * TODO:
 *  - add probes to track statistics
 */
class batch_cache {
    /// Minimum size reclaimed in low-memory situations.
    static constexpr size_t min_reclaim_size = 128 << 10;

    using reclaimer = ss::memory::reclaimer;
    using reclaim_scope = ss::memory::reclaimer_scope;
    using reclaim_result = ss::memory::reclaiming_result;

public:
    struct reclaim_options {
        ss::lowres_clock::duration growth_window;
        ss::lowres_clock::duration stable_window;
        size_t min_size;
        size_t max_size;
    };

    /*
     * An entry manages the lifetime of a cached record batch, and always exists
     * in either the LRU or the free pool. Any batches stored in the free pool
     * are in an undefined state.
     */
    class entry : private ss::weakly_referencable<entry> {
    public:
        class lock_guard {
        public:
            explicit lock_guard(entry& e) noexcept
              : _e(e) {
                _e.pin();
            }
            lock_guard(lock_guard&&) = delete;
            lock_guard& operator=(lock_guard&&) = delete;
            lock_guard(const lock_guard&) = delete;
            lock_guard& operator=(const lock_guard&) = delete;
            ~lock_guard() noexcept { _e.unpin(); }

        private:
            entry& _e;
        };

        explicit entry(batch_cache_index& index, model::record_batch&& batch)
          : _batch(std::move(batch))
          , _index(index) {}

        ~entry() noexcept = default;
        entry(entry&&) noexcept = delete;
        entry& operator=(entry&&) noexcept = delete;
        entry(const entry&) = delete;
        entry& operator=(const entry&) = delete;

        // the entry initially contains a valid batch, but it may transition
        // into an invalid state where the batch data cannot be accessed.
        bool valid() const { return _valid; }
        void invalidate() { _valid = false; }

        model::record_batch& batch() {
            vassert(_valid, "cannot access invalided batch");
            return _batch;
        }

        void pin() { _pinned = true; }
        void unpin() { _pinned = false; }
        bool pinned() const { return _pinned; }

    private:
        friend class batch_cache;
        friend ss::weakly_referencable<entry>;

        // invalidation is logical. we still want the cache to be able to look
        // at its memory usage and base offset, but the cache index should never
        // interact with an invalid entry.
        bool _valid{true};
        model::record_batch _batch;

        bool _pinned{false};
        intrusive_list_hook _hook;
        batch_cache_index& _index;
    };

    using entry_ptr = ss::weak_ptr<entry>;

    batch_cache(const reclaim_options& opts)
      : _reclaimer(
        [this](reclaimer::request r) { return reclaim(r); },
        reclaim_scope::sync)
      , _reclaim_opts(opts) {}

    batch_cache(const batch_cache&) = delete;
    batch_cache& operator=(const batch_cache&) = delete;
    batch_cache& operator=(batch_cache&&) = delete;

    /*
     * the reclaimer cannot be moved or copied (even though it doesn't prevent
     * it). the reason is that the reclaimer registers/deregisters itself using
     * `this` as a key in its constructor/destructor which aren't properly
     * balanced for these cases. the reclaimer needs to be fully recreated here,
     * and the moved from reclaimer will deregister itself properly.
     */
    batch_cache(batch_cache&& o) noexcept
      : _lru(std::move(o._lru))
      , _reclaimer(
          [this](reclaimer::request r) { return reclaim(r); },
          reclaim_scope::sync)
      , _is_reclaiming(o._is_reclaiming)
      , _size_bytes(o._size_bytes)
      , _reclaim_opts(o._reclaim_opts) {
        o._size_bytes = 0;
        o._is_reclaiming = false;
    }

    ~batch_cache() noexcept;

    /// Returns true if the cache is empty, and false otherwise.
    bool empty() const { return _lru.empty(); }

    /// Removes all entries from the cache and entry pool.
    void clear() { reclaim(std::numeric_limits<size_t>::max()); }

    /**
     * Copies a batch into the LRU cache.
     * Copying is needed to release memory references of underlying tempbufs.
     *
     * The returned weak_ptr will be invalidated if its memory is reclaimed. To
     * evict the entry, move it into batch_cache::evict().
     */
    entry_ptr put(batch_cache_index&, const model::record_batch&);

    /**
     * \brief Remove a batch from the cache.
     *
     * Memory associated with the batch is released and the cache entry is
     * returned to the free pool.
     *
     * It is important that this interface act as a sink. Since we are moving
     * the cache entries into the free pool this means that the weak_ptr
     * invalidation does not trigger. This is still safe because the caching
     * interface forces an the caller to give up its entry reference, as well as
     * preventing multiple weak_ptr references to the same entry.  This is not
     * relevant for the reclaim interface. Reclaim fully deletes cache entries
     * which does invoke weak_ptr invalidation.
     */
    void evict(entry_ptr&& e);

    /**
     * Notify the cache that the specified entry was recently used.
     */
    void touch(entry_ptr& e) {
        if (e) {
            auto p = e.get();
            p->_hook.unlink();
            _lru.push_back(*p);
        }
    }

    /**
     * \brief Evict batches up to the accumulated size specified.
     *
     * Unlike `evict` which places the cache entry back into the free pool, this
     * method releases the entire entry because this interface is intended to be
     * used to deal with low-memory situations.
     */
    size_t reclaim(size_t size);

    /**
     * returns true if there is an active reclaim happening
     */
    bool is_memory_reclaiming() const { return _is_reclaiming; }

private:
    struct batch_reclaiming_lock {
        explicit batch_reclaiming_lock(batch_cache& b) noexcept
          : ref(b)
          , prev(ref._is_reclaiming) {
            ref._is_reclaiming = true;
        }
        ~batch_reclaiming_lock() noexcept { ref._is_reclaiming = prev; }
        batch_reclaiming_lock(const batch_reclaiming_lock&) = delete;
        batch_reclaiming_lock(batch_reclaiming_lock&&) = delete;
        batch_reclaiming_lock& operator=(const batch_reclaiming_lock&) = delete;
        batch_reclaiming_lock& operator=(batch_reclaiming_lock&&) = delete;

        batch_cache& ref;
        bool prev;
    };
    friend batch_reclaiming_lock;
    /*
     * The entry point for the Seastar upcall for relcaiming memory. The
     * reclaimer is configured to perform the upcall asynchronously in a new
     * fiber. A more advanced usage that is allowed to invoke reclaim
     * synchronously with memory allocation is also possible.
     */
    ss::memory::reclaiming_result reclaim(reclaimer::request r) {
        const size_t lower_bound = std::max(
          r.bytes_to_reclaim, min_reclaim_size);
        // _attempt_ to reclaim lower_bound. stop at greater than or equal to
        // lower_bound
        const size_t reclaimed = reclaim(lower_bound);
        return reclaimed != 0 ? reclaim_result::reclaimed_something
                              : reclaim_result::reclaimed_nothing;
    }

    intrusive_list<entry, &entry::_hook> _lru;
    reclaimer _reclaimer;
    bool _is_reclaiming{false};
    size_t _size_bytes{0};

    reclaim_options _reclaim_opts;
    ss::lowres_clock::time_point _last_reclaim;
    size_t _reclaim_size;

    friend std::ostream& operator<<(std::ostream&, const batch_cache&);
};

class batch_cache_index {
    using index_type = absl::btree_map<model::offset, batch_cache::entry_ptr>;

public:
    struct read_result {
        ss::circular_buffer<model::record_batch> batches;
        size_t memory_usage{0};
        model::offset next_batch;
        std::optional<model::offset> next_cached_batch;

        friend std::ostream& operator<<(std::ostream&, const read_result&);
    };

    explicit batch_cache_index(batch_cache& cache)
      : _cache(&cache) {}
    ~batch_cache_index() {
        lock_guard lk(*this);
        std::for_each(
          _index.begin(), _index.end(), [this](index_type::value_type& e) {
              _cache->evict(std::move(e.second));
          });
    }
    batch_cache_index(batch_cache_index&&) noexcept = default;
    batch_cache_index& operator=(batch_cache_index&&) noexcept = default;
    batch_cache_index(const batch_cache_index&) = delete;
    batch_cache_index& operator=(const batch_cache_index&) = delete;

    bool empty() const { return _index.empty(); }

    void put(const model::record_batch& batch) {
        lock_guard lk(*this);
        auto offset = batch.header().base_offset;
        if (likely(!_index.contains(offset))) {
            /*
             * do not allow initial cache entries to be dangling. if the index
             * is destroyed the cache will contain invalid index reference. once
             * entries are initialized in the cache and index, clean-up happens
             * correctly on either side.
             */
            auto p = _cache->put(*this, batch);
            _index.emplace(offset, std::move(p));
        }
    }

    /**
     * Return the batch containing the specified offset, if one exists.
     */
    std::optional<model::record_batch> get(model::offset offset);

    /**
     * \brief Return a contiguous range of cached batches.
     *
     * The set of batches returned will cover a contiguous range starting
     * from the specified offset up until the given max offset. A partial
     * result will be returned if any cache miss occurs or max bytes is
     * exceeded. If applicable, the result will include the base offset of the
     * next available batch in the cache (`.next_cached_batch`). This offset may
     * then be used to optimize for returning to the cache to satisfy reads.
     *
     * The `.next_batch` field in the result is defined to be the base offset of
     * the next batch in the log. This value should be used to initialize any
     * iteration over the log, even when no cached results are returned because
     * the type filter may cause cached batches to be skipped.
     *
     * When `skip_lru_promote` is true a cache hit doesn't change the position
     * of the batch in the lru list. This is useful when the read is known to
     * not be repeated in the near future.
     */
    read_result read(
      model::offset offset,
      model::offset max_offset,
      std::optional<model::record_batch_type> type_filter,
      std::optional<model::timestamp> first_ts,
      size_t max_bytes,
      bool skip_lru_promote);

    /**
     * Removes all batches that _may_ contain the specified offset.
     */
    void truncate(model::offset offset);

    /*
     * Testing interface used to evict a batch from the cache identified by
     * the specified offset. The index entry is not removed. The offset must
     * be specified in terms of the batch's base offset.
     */
    void testing_evict_from_cache(model::offset offset) {
        if (auto it = _index.find(offset); it != _index.end()) {
            _cache->evict(std::move(it->second));
        }
    }

    /*
     * Testing interface used to check if an index entry exists even if its
     * associated batch has been evicted from the cache.
     */
    bool testing_exists_in_index(model::offset offset) {
        return _index.find(offset) != _index.end();
    }

private:
    friend class batch_cache;

    class lock_guard {
    public:
        explicit lock_guard(batch_cache_index& index) noexcept
          : _index(index) {
            _index.lock();
        }

        lock_guard(lock_guard&&) = delete;
        lock_guard& operator=(lock_guard&&) = delete;
        lock_guard(const lock_guard&) = delete;
        lock_guard& operator=(const lock_guard&) = delete;
        ~lock_guard() noexcept { _index.unlock(); }

    private:
        batch_cache_index& _index;
    };

    bool locked() const { return _locked; }

    void lock() {
        vassert(!_locked, "batch cache index double lock");
        _locked = true;
    }

    void unlock() {
        vassert(_locked, "batch cache index double unlock");
        _locked = false;
    }

    /*
     * XXX: only safe when invoked by the batch cache reclaimer.
     */
    bool remove(model::offset offset) {
        vassert(!locked(), "attempt to erase from locked index");
        return _index.erase(offset) == 1;
    }

    /*
     * Return an iterator to the first batch that _may_ contain the specified
     * offset. Since the batch may have been evicted, and we only store the base
     * offset in the index, the caller deals with the missing upper bound.
     */
    index_type::iterator find_first(model::offset offset) {
        if (_index.empty()) {
            return _index.end();
        }
        auto it = _index.upper_bound(offset);
        if (it != _index.begin()) {
            --it;
        }
        return it;
    }

    /*
     * Return an iterator to the first batch known to contain the specified
     * offset, otherwise return the end iterator. Since a batch must be present
     * in memory to verify that it contains the offset, a non-end returned
     * iterator is guaranteed to point to a live batch.
     */
    index_type::iterator find_first_contains(model::offset offset) {
        if (auto it = find_first(offset);
            it != _index.end() && it->second && it->second->valid()
            && it->second->batch().contains(offset)) {
            return it;
        }
        return _index.end();
    }

    bool _locked{false};
    batch_cache* _cache;
    index_type _index;

    friend std::ostream& operator<<(std::ostream&, const batch_cache_index&);
};

using batch_cache_index_ptr = std::unique_ptr<batch_cache_index>;

} // namespace storage
