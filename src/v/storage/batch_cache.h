#pragma once
#include "model/record.h"
#include "seastar/core/memory.hh"
#include "seastar/core/weak_ptr.hh"
#include "utils/intrusive_list_helpers.h"
#include "vassert.h"

#include <seastar/core/circular_buffer.hh>

#include <boost/container/flat_map.hpp>

#include <type_traits>

namespace storage {

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
    /*
     * An entry manages the lifetime of a cached record batch, and always exists
     * in either the LRU or the free pool. Any batches stored in the free pool
     * are in an undefined state.
     */
    class entry : private ss::weakly_referencable<entry> {
    public:
        explicit entry(model::record_batch&& batch)
          : batch(std::move(batch)) {}
        entry(const entry&) = delete;
        entry& operator=(const entry&) = delete;

        model::record_batch batch;

    private:
        friend class batch_cache;
        friend ss::weakly_referencable<entry>;

        intrusive_list_hook _hook;
    };

    using entry_ptr = ss::weak_ptr<entry>;

    batch_cache()
      : _reclaimer(
        [this](reclaimer::request r) { return reclaim(r); },
        reclaim_scope::async) {}

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
    batch_cache(batch_cache&& o)
      : _lru(std::move(o._lru))
      , _pool(std::move(o._pool))
      , _reclaimer(
          [this](reclaimer::request r) { return reclaim(r); },
          reclaim_scope::async) {}

    ~batch_cache() { clear(); }

    /// Returns true if the cache is empty, and false otherwise.
    bool empty() const { return _lru.empty(); }

    /// Removes all entries from the cache and entry pool.
    void clear() {
        _lru.clear_and_dispose([](entry* e) { delete e; });
        _pool.clear_and_dispose([](entry* e) { delete e; });
    }

    /**
     * Insert a batch into the LRU cache.
     *
     * The returned weak_ptr will be invalidated if its memory is reclaimed. To
     * evict the entry, move it into batch_cache::evict().
     */
    entry_ptr put(model::record_batch batch) {
        entry* e;
        if (_pool.empty()) {
            e = new entry(std::move(batch));
        } else {
            e = &_pool.front();
            _pool.pop_front();
            e->batch = std::move(batch);
        }
        _lru.push_back(*e);
        return e->weak_from_this();
    }

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
    void evict(entry_ptr&& e) {
        if (e) {
            auto p = std::exchange(e, {});
            p->_hook.unlink();
            p->batch.clear();
            _pool.push_back(*p);
        }
    }

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
    size_t reclaim(size_t size) {
        size_t reclaimed = 0;
        while (reclaimed < size && !_lru.empty()) {
            reclaimed += _lru.front().batch.memory_usage();
            _lru.pop_front_and_dispose([](entry* e) { delete e; });
        }
        while (reclaimed < size && !_pool.empty()) {
            reclaimed += _pool.front().batch.memory_usage();
            _pool.pop_front_and_dispose([](entry* e) { delete e; });
        }
        return reclaimed;
    }

private:
    /*
     * The entry point for the Seastar upcall for relcaiming memory. The
     * reclaimer is configured to perform the upcall asynchronously in a new
     * fiber. A more advanced usage that is allowed to invoke reclaim
     * synchronously with memory allocation is also possible.
     */
    ss::memory::reclaiming_result reclaim(reclaimer::request r) {
        auto size = std::max(r.bytes_to_reclaim, min_reclaim_size);
        auto reclaimed = reclaim(size);
        return reclaimed ? reclaim_result::reclaimed_something
                         : reclaim_result::reclaimed_nothing;
    }

    intrusive_list<entry, &entry::_hook> _lru;
    intrusive_list<entry, &entry::_hook> _pool;
    reclaimer _reclaimer;
};

class batch_cache_index {
    using index_type
      = boost::container::flat_map<model::offset, batch_cache::entry_ptr>;

public:
    struct read_result {
        ss::circular_buffer<model::record_batch> batches;
        size_t memory_usage{0};
        std::optional<model::offset> next_batch;
    };

    batch_cache_index(batch_cache& cache)
      : _cache(cache) {}

    ~batch_cache_index() {
        std::for_each(
          _index.begin(), _index.end(), [this](index_type::value_type& e) {
              _cache.evict(std::move(e.second));
          });
    }

    bool empty() const { return _index.empty(); }

    void put(model::record_batch&& batch) {
        auto offset = batch.base_offset();
        auto p = _cache.put(std::move(batch));
        _index.emplace(offset, std::move(p));
    }

    template<typename Range>
    void put(Range&& batches) {
        static_assert(
          std::is_rvalue_reference_v<decltype(batches)>,
          "ensure you move your types to the cache");
        for (auto& batch : batches) {
            put(std::move(batch));
        }
    }

    /**
     * Return the batch containing the specified offset, if one exists.
     */
    std::optional<model::record_batch> get(model::offset offset) {
        if (auto it = find_first_contains(offset); it != _index.end()) {
            _cache.touch(it->second);
            return it->second->batch.share();
        }
        return std::nullopt;
    }

    /**
     * \brief Return a contiguous range of cached batches.
     *
     * The set of batches returned will cover a contiguous range starting
     * from the specified offset up until the given max offset. A partial
     * result will be returned if any cache miss occurs or max bytes is
     * exceeded. If applicable, the result will include the base offset of the
     * next available batch in the cache. This offset may then be used to
     * optimize for returning to the cache to satisfy reads.
     */
    read_result
    read(model::offset offset, model::offset max_offset, size_t max_bytes) {
        if (unlikely(offset > max_offset)) {
            return {};
        }

        read_result ret;

        for (auto it = find_first_contains(offset); it != _index.end();) {
            ret.batches.emplace_back(it->second->batch.share());
            auto& batch = ret.batches.back();
            ret.memory_usage += batch.memory_usage();
            _cache.touch(it->second);

            offset = batch.last_offset() + model::offset(1);

            /*
             * we're done in any of the following cases:
             *
             * 1. end of index
             */
            if (++it == _index.end()) {
                break;
            }

            /*
             * 2. cache miss
             * 3. hole in range
             */
            if (!it->second || it->first != offset) {
                // compute the base offset of the next cached batch
                auto next_batch = std::find_if(
                  it, _index.end(), [](const index_type::value_type& e) {
                      return bool(e.second);
                  });
                if (next_batch != _index.end()) {
                    ret.next_batch = next_batch->second->batch.base_offset();
                }
                break;
            }

            /*
             * 4. exceed max offset
             * 5. exceed max bytes
             */
            if (offset > max_offset || ret.memory_usage >= max_bytes) {
                break;
            }
        }

        return ret;
    }

    /**
     * Removes all batches that _may_ contain the specified offset.
     */
    void truncate(model::offset offset) {
        if (auto it = find_first(offset); it != _index.end()) {
            // rule out if possible, otherwise always be pessimistic
            if (it->second && !it->second->batch.contains(offset)) {
                ++it;
            }
            std::for_each(it, _index.end(), [this](index_type::value_type& e) {
                _cache.evict(std::move(e.second));
            });
            _index.erase(it, _index.end());
        }
    }

    /*
     * Testing interface used to evict a batch from the cache identified by
     * the specified offset. The index entry is not removed. The offset must
     * be specified in terms of the batch's base offset.
     */
    void testing_evict_from_cache(model::offset offset) {
        if (auto it = _index.find(offset); it != _index.end()) {
            _cache.evict(std::move(it->second));
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
            it != _index.end() && it->second
            && it->second->batch.contains(offset)) {
            return it;
        }
        return _index.end();
    }

    batch_cache& _cache;
    index_type _index;
};

using batch_cache_index_ptr = std::unique_ptr<batch_cache_index>;

} // namespace storage
