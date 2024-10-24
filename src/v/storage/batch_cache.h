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
#include "base/units.h"
#include "base/vassert.h"
#include "container/intrusive_list_helpers.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "resource_mgmt/available_memory.h"
#include "ssx/semaphore.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/weak_ptr.hh>

#include <absl/container/btree_map.h>

#include <limits>
#include <type_traits>

class batch_cache_test_fixture;
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
 * Batch cache operations that hold a reference to an range in the cache need to
 * be careful that any heap allocation may cause the held reference to be
 * _concurrently_ freed/invalidated.
 *
 * For example, given a live range in the cache:
 *
 *    auto r = _index.find(..);
 *    assert(r);
 *
 * doing something such as `e->batch.share()` internally causes allocations
 * which may trigger the reclaimer. if the reclaimer selects `e` for reclaiming,
 * then `e->batch.share()` will run concurrently with the destructor of the
 * batch (not technically concurently--but interleaved with share() in ways that
 * are not safe).
 *
 * If an operation may perform an allocation use range::pin/unpin to guard the
 * reference which will force the reclaimer to skip the range.
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
    static constexpr size_t min_reclaim_size = 128U << 10U;

    using reclaimer = ss::memory::reclaimer;
    using reclaim_scope = ss::memory::reclaimer_scope;
    using reclaim_result = ss::memory::reclaiming_result;

public:
    using is_dirty_entry
      = ss::bool_class<struct batch_cache_is_dirty_entry_tag>;

    struct reclaim_options {
        ss::lowres_clock::duration growth_window;
        ss::lowres_clock::duration stable_window;
        size_t min_size;
        size_t max_size;
        // background reclaimer settings
        ss::scheduling_group background_reclaimer_sg;
        size_t min_free_memory = 64_MiB;
    };

    /*
     * An range manages the lifetime of a multiple cached record batches.
     */
    class range : private ss::weakly_referencable<range> {
    public:
        static constexpr size_t range_size = 32_KiB;
        // max waste is parameter controlling max free space left in range after
        // it is not longer being appended to
        static constexpr size_t max_waste_bytes = 1_KiB;
        static constexpr size_t min_bytes_in_range = range_size
                                                     - max_waste_bytes;

        class lock_guard {
        public:
            explicit lock_guard(range& e) noexcept
              : _e(e) {
                _e.pin();
            }
            lock_guard(lock_guard&&) = delete;
            lock_guard& operator=(lock_guard&&) = delete;
            lock_guard(const lock_guard&) = delete;
            lock_guard& operator=(const lock_guard&) = delete;
            ~lock_guard() noexcept { _e.unpin(); }

        private:
            range& _e;
        };

        explicit range(batch_cache_index& index);
        explicit range(
          batch_cache_index& index,
          const model::record_batch& batch,
          is_dirty_entry dirty);

        ~range() noexcept = default;
        range(range&&) noexcept = delete;
        range& operator=(range&&) noexcept = delete;
        range(const range&) = delete;
        range& operator=(const range&) = delete;

        // the range initially contains a valid batches, but it may transition
        // into an invalid state where the batch data cannot be accessed.
        bool valid() const { return _valid; }
        void invalidate() { _valid = false; }
        static constexpr size_t serialized_header_size
          = model::packed_record_batch_header_size + sizeof(size_t);

        model::record_batch batch(size_t o);
        model::record_batch_header header(size_t o);

        void pin() { _pinned = true; }
        void unpin() { _pinned = false; }
        bool pinned() const { return _pinned; }

        void mark_clean(model::offset up_to) {
            if (_max_dirty_offset <= up_to) {
                _max_dirty_offset = model::offset{};
            }
        }
        bool clean() const { return _max_dirty_offset == model::offset{}; };

        size_t memory_size() const;
        size_t bytes_left() const;
        double waste() const;
        bool empty() const;
        // checks if record batch will fit into current range
        bool fits(const model::record_batch& b) const;
        uint32_t add(const model::record_batch&, is_dirty_entry);

    private:
        friend class batch_cache;
        friend ss::weakly_referencable<range>;

        // invalidation is logical. we still want the cache to be able to look
        // at its memory usage and base offset, but the cache index should never
        // interact with an invalid range.
        bool _valid{true};
        // buffer where batches are stored
        iobuf _arena;
        // list of offsets to update batch_cache_index
        std::vector<model::offset> _offsets;

        bool _pinned{false};

        // Maximum dirty batch offset that is stored in this range. If a range
        // contains any dirty batch offsets we prevent its eviction so that
        // readers get cache hits until the data is persisted to disk and we can
        // rehydrate on a miss.
        //
        // Dirty batches are inserted with monotonically increasing offsets and
        // marked as clean with an upper bound.
        //
        // You can imagine the put operation moving one cursor forward and
        // mark_clean operation moving another one from behind. Once they
        // overlap, the range is considered clean.
        //
        // Let's take the following example range with clean and dirty batches:
        //
        // +------------+------------|------------+----------+
        // |  c=10..15  |  d=16..18  |  d=19..25  |  c=4..6  |
        // +------------+------------|------------+----------+
        //                                        ^
        //              _max_dirty_offset=25 -----+
        //
        // Note 1: Dirty batches always are monotonically increasing but clean
        //   batches might not be as they might have been added by concurrent
        //   read operations.
        //
        // Note 2: We don't actually track whether a particular batch is clean
        //   or dirty. The `c` and `d` on the diagram are for illustration
        //   purposes only.
        //
        // Requesting to mark the range as clean up to offset 20 won't do
        // anything. The range would still be considered dirty.
        //
        // +------------+------------|------------+----------+
        // |  c=10..15  |  c=16..18  |  d=19..25  |  c=4..6  |
        // +------------+------------|------------+----------+
        //                                  ^     ^
        //           _max_dirty_offset=25 --+-----+
        //                                  |
        //           mark_clean(20) --------+
        //
        // However, requesting to mark clean up to offset 25 will effectively
        // mark the entire range as clean and reset the _max_dirty_offset to
        // it's default value.
        //
        // +------------+------------|------------+----------+
        // |  c=10..15  |  c=16..18  |  c=19..25  |  c=4..6  |
        // +------------+------------|------------+----------+
        //                                        ^
        //           _max_dirty_offset=25 --------+
        //                                        |
        //           mark_clean(25) --------------+
        model::offset _max_dirty_offset;

        size_t _size = 0;
        intrusive_list_hook _hook;
        batch_cache_index& _index;
    };

    using range_ptr = ss::weak_ptr<range>;
    /**
     * Entry represents single batch in given range, it contains range weak
     * pointer and batch offset in range _arena buffer.
     */
    class entry {
    public:
        entry(uint32_t o, range_ptr&& ptr)
          : _range_offset(o)
          , _range(std::move(ptr)) {}

        ~entry() noexcept = default;
        entry(entry&&) noexcept = default;
        entry& operator=(entry&&) noexcept = default;
        entry(const entry&) = delete;
        entry& operator=(const entry&) = delete;

        model::record_batch batch() { return _range->batch(_range_offset); }
        model::record_batch_header header() const {
            return _range->header(_range_offset);
        }

        range_ptr& range() { return _range; }
        const range_ptr& range() const { return _range; }
        bool valid() const { return _range->valid(); }

    private:
        uint32_t _range_offset;
        range_ptr _range;
    };

    explicit batch_cache(const reclaim_options& opts);

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
    batch_cache(batch_cache&& o) noexcept = delete;

    ~batch_cache() noexcept;

    ss::future<> stop() { return _background_reclaimer.stop(); }

    /// Returns true if the cache is empty, and false otherwise.
    bool empty() const { return _lru.empty(); }

    /// Removes all entries from the cache.
    void clear() { reclaim(std::numeric_limits<size_t>::max()); }

    /**
     * Copies a batch into the LRU cache.
     * Copying is needed to release memory references of underlying tempbufs.
     *
     * The returned weak_ptr will be invalidated if its memory is reclaimed. To
     * evict the range, move it into batch_cache::evict().
     */
    entry put(batch_cache_index&, const model::record_batch&, is_dirty_entry);

    /**
     * \brief Remove a batch from the cache.
     *
     * Memory associated with the whole range is released.
     *
     * It is important that this interface act as a sink. Since we are moving
     * the cache entries into the free pool this means that the weak_ptr
     * invalidation does not trigger. This is still safe because the caching
     * interface forces an the caller to give up its range reference, as well as
     * preventing multiple weak_ptr references to the same range.  This is not
     * relevant for the reclaim interface. Reclaim fully deletes cache entries
     * which does invoke weak_ptr invalidation.
     */
    void evict(range_ptr&& e);

    /**
     * Notify the cache that the specified range was recently used.
     */
    void touch(range_ptr& e) {
        if (e) {
            auto p = e.get();
            p->_hook.unlink();
            _lru.push_back(*p);
        }
    }

    /**
     * \brief Evict batches up to the accumulated size specified.
     *
     * Unlike `evict` which places the cache range back into the free pool, this
     * method releases the entire range because this interface is intended to be
     * used to deal with low-memory situations.
     */
    size_t reclaim(size_t size);

    /**
     * returns true if there is an active reclaim happening
     */
    bool is_memory_reclaiming() const { return _is_reclaiming; }

    /**
     * @brief The estimated size of the cache in bytes.
     *
     * This is a slight underestimate of the true size of the memory used by the
     * cache since it only counts the size of each underlying iobuf (arena) but
     * not other overhead such as the iobuf struct itself, index structures,
     * etc.
     *
     * @return size_t estimate of the in-memory size of the cache
     */
    size_t size_bytes() const { return _size_bytes; }

private:
    friend batch_cache_test_fixture;
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
    class background_reclaimer {
    public:
        explicit background_reclaimer(
          batch_cache& c, size_t min_free_memory, ss::scheduling_group sg)
          : _cache(c)
          , _min_free_memory(min_free_memory)
          , _sg(sg) {}

        void notify() { _change.signal(); }

        void start();
        ss::future<> stop();

        ss::future<> reclaim_loop();

    private:
        bool have_to_reclaim() const {
            return ss::memory::stats().free_memory() < _min_free_memory;
        }
        bool _stopped = false;
        ssx::semaphore _change{0, "s/batch-reclaim"};
        batch_cache& _cache;
        size_t _min_free_memory;
        ss::scheduling_group _sg;
        ss::gate _gate;
    };

    friend background_reclaimer;
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

    intrusive_list<range, &range::_hook> _lru;
    reclaimer _reclaimer;
    bool _is_reclaiming{false};
    size_t _size_bytes{0};

    reclaim_options _reclaim_opts;
    ss::lowres_clock::time_point _last_reclaim;
    size_t _reclaim_size;
    background_reclaimer _background_reclaimer;
    resources::available_memory::deregister_holder _available_mem_deregister;

    friend std::ostream& operator<<(std::ostream&, const reclaim_options&);
    friend std::ostream& operator<<(std::ostream&, const batch_cache&);
};

class batch_cache_index {
    /// A data structure for tracking the dirty state of a batch cache index and
    /// minimum and maximum dirty offsets.
    ///
    /// We use it to optimize the \ref batch_cache_index::mark_clean routine
    /// where we narrow the set of offsets and ranges we have to iterate
    /// through when marking them as clean and to facilitate invariants:
    /// 1. Dirty offsets are monotonic (an assumption we also leverage for
    /// tracking the dirty state of \ref batch_cache::range).
    /// 2. Truncation is requested only with a clean index (we can evict only at
    /// range level).
    /// 3. At the end of batch cache / batch cache index lifetime there are no
    /// dirty batches left in the index (should help catch bugs in other
    /// components).
    class dirty_tracker {
    public:
        /// Marks a range of offsets as dirty.
        ///
        /// \param range The range of offsets to mark as dirty.
        ///              The first element of the pair represents the starting
        ///              offset, and the second element represents the ending
        ///              offset.
        void mark_dirty(const std::pair<model::offset, model::offset> range);

        /// Marks the offsets as clean up to and including the specified offset.
        ///
        /// \param up_to_inclusive The offset, inclusive, up to which the cache
        ///                        should be marked as clean.
        ///
        void mark_clean(const model::offset up_to_inclusive);

        auto min() const { return _min; };

        bool clean() const { return _min == model::offset{}; }

        friend std::ostream&
        operator<<(std::ostream& o, const dirty_tracker& t) {
            o << "{min: " << t._min << ", max: " << t._max << "}";
            return o;
        }

    private:
        model::offset _min;
        model::offset _max;
    };

    using index_type = absl::btree_map<model::offset, batch_cache::entry>;

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
        vassert(
          _dirty_tracker.clean(),
          "Destroying batch_cache_index ({}) tracking dirty batches.",
          *this);
        std::for_each(
          _index.begin(), _index.end(), [this](index_type::value_type& e) {
              _cache->evict(std::move(e.second.range()));
          });
    }
    batch_cache_index(batch_cache_index&&) noexcept = default;
    batch_cache_index& operator=(batch_cache_index&&) noexcept = default;
    batch_cache_index(const batch_cache_index&) = delete;
    batch_cache_index& operator=(const batch_cache_index&) = delete;

    ss::future<> clear_async();
    bool empty() const { return _index.empty(); }

    void
    put(const model::record_batch& batch, batch_cache::is_dirty_entry dirty) {
        lock_guard lk(*this);
        auto offset = batch.header().base_offset;
        if (likely(!_index.contains(offset))) {
            /*
             * do not allow initial cache entries to be dangling. if the index
             * is destroyed the cache will contain invalid index reference. once
             * entries are initialized in the cache and index, clean-up happens
             * correctly on either side.
             */
            auto p = _cache->put(*this, batch, dirty);
            if (dirty) {
                _dirty_tracker.mark_dirty(
                  {batch.base_offset(), batch.last_offset()});
            }
            _index.emplace(offset, std::move(p));
        } else {
            vassert(
              dirty == batch_cache::is_dirty_entry::no,
              "Dirty entry already present in batch cache index. This is a "
              "bug.");
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

    /**
     * Marks the offsets as clean up to and including the specified offset.
     * \param up_to_inclusive The offset, inclusive, up to which the cache
     *                        should be marked as clean.
     */
    void mark_clean(model::offset up_to_inclusive);

    /*
     * Testing interface used to evict a batch from the cache identified by
     * the specified offset. The index range is not removed. The offset must
     * be specified in terms of the batch's base offset.
     */
    void testing_evict_from_cache(model::offset offset) {
        if (auto it = _index.find(offset); it != _index.end()) {
            _cache->evict(std::move(it->second.range()));
        }
    }

    /*
     * Testing interface used to check if an index range exists even if its
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
            it != _index.end() && it->second.range()
            && it->second.range()->valid()
            && it->second.header().contains(offset)) {
            return it;
        }
        return _index.end();
    }

    bool _locked{false};
    batch_cache* _cache;
    index_type _index;
    batch_cache::range_ptr _small_batches_range = nullptr;

    dirty_tracker _dirty_tracker;

    friend std::ostream& operator<<(std::ostream&, const batch_cache_index&);
};

using batch_cache_index_ptr = std::unique_ptr<batch_cache_index>;

} // namespace storage
