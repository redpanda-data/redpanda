// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "batch_cache.h"

#include "utils/to_string.h"
#include "vassert.h"

#include <fmt/ostream.h>

namespace storage {

batch_cache::entry_ptr
batch_cache::put(batch_cache_index& index, const model::record_batch& input) {
#ifdef SEASTAR_DEFAULT_ALLOCATOR
    static const size_t threshold = ss::memory::stats().total_memory() * .2;
    while (_size_bytes > threshold) {
        reclaim(1);
    }
#endif
    // we must copy memory to prevent holding onto bigger memory from
    // temporary buffers
    auto batch = input.copy();
    _size_bytes += batch.memory_usage();
    auto e = new entry(index, std::move(batch));

    // if weak_from_this were to cause an allocation--which it shouldn't--`e`
    // wouldn't be visible to the reclaimer since it isn't on a lru/pool list.
    auto p = e->weak_from_this();
    _lru.push_back(*e);
    return p;
}

batch_cache::~batch_cache() noexcept {
    clear();
    vassert(
      _size_bytes == 0 && _lru.empty(),
      "Detected incorrect batch_cache accounting. {}",
      *this);
}

void batch_cache::evict(entry_ptr&& e) {
    if (e) {
        // it's necessary to cause `e` to be sinked so the move constructor
        // invalidates the caller's entry_ptr. simply interacting with the
        // r-value reference `e` wouldn't do that.
        auto p = std::exchange(e, {});
        _size_bytes -= p->_batch.memory_usage();
        _lru.erase_and_dispose(
          _lru.iterator_to(*p), [](entry* e) { delete e; });
    }
}

size_t batch_cache::reclaim(size_t size) {
    if (is_memory_reclaiming()) {
        return 0;
    }
    batch_reclaiming_lock lock(*this);

    /*
     * if the time since the last reclaim is < `reclaim_growth_window` --
     * typically a small value such as 3 seconds, then increase the reclaim size
     * by around 50%. this generally handles the the memory pressure and tight
     * reclaim loops. otherwise, use the last guess if it has been less than
     * `reclaim_stable_window` and reset the process if it has been longer.
     */
    auto elapsed = ss::lowres_clock::now() - _last_reclaim;
    if (elapsed < _reclaim_opts.growth_window) {
        _reclaim_size = (((_reclaim_size * 3) + 1) / 2);
    } else if (elapsed > _reclaim_opts.stable_window) {
        _reclaim_size = _reclaim_opts.min_size;
    }

    _reclaim_size = std::min(_reclaim_size, _reclaim_opts.max_size);
    _reclaim_size = std::max(size, _reclaim_size);

    /*
     * reclaiming is a two pass process. given that the entry isn't pinned (in
     * which case it is skipped), the first step is to reclaim the batch's
     * record data. at this point, if the entry's owning index is not locked the
     * entry is added to a temporary list of entries which will be removed.
     * otherwise if the index is locked, removal is deferred but the entry is
     * invalidated. invalidation is important because the batch reference in the
     * index still exists even though the batch data was removed.
     */
    size_t reclaimed = 0;
    intrusive_list<entry, &entry::_hook> reclaimed_entries;

    for (auto it = _lru.begin(); it != _lru.end();) {
        if (reclaimed >= _reclaim_size) {
            break;
        }

        // skip any entry that has a live reference.
        if (unlikely(it->pinned())) {
            ++it;
            continue;
        }

        // reclaim the batch's record data
        reclaimed += it->_batch.memory_usage();
        it->_batch.clear_data();

        /*
         * if the owning index is locked invalidate the entry but leave it on
         * the lru list for deferred deletion so as to not invalidate any open
         * iterators on the index.
         */
        if (unlikely(it->_index.locked())) {
            reclaimed -= it->_batch.memory_usage();
            it->invalidate();
            ++it;
            continue;
        }

        // collect the entries that will be fully removed
        it = _lru.erase_and_dispose(it, [&reclaimed_entries](entry* e) {
            reclaimed_entries.push_back(*e);
        });
    }

    /*
     * final removal from the index is deferred because there is some chance
     * that removal allocates, so waiting until the bulk of the reclaims have
     * occurred reduces the probability of an allocation failure.
     */
    reclaimed_entries.clear_and_dispose([](entry* e) {
        auto offset = e->_batch.base_offset();
        auto* index = &e->_index;
        delete e; // NOLINT

        /*
         * since reclaim may be invoked at any moment and removals may be
         * deferred if an index is locked, one can imagine races in which a
         * batch is removed by offset here which is not the same batch that was
         * reclaimed in a prior pass. at worst this would raise the miss ratio,
         * but is still generally safe since all batch cache users are prepared
         * to handle a miss.
         */
        index->remove(offset);
    });

    _last_reclaim = ss::lowres_clock::now();
    _size_bytes -= reclaimed;
    return reclaimed;
}

std::optional<model::record_batch>
batch_cache_index::get(model::offset offset) {
    lock_guard lk(*this);
    if (auto it = find_first_contains(offset); it != _index.end()) {
        batch_cache::entry::lock_guard g(*it->second);
        _cache->touch(it->second);
        auto ret = it->second->batch().share();
        return ret;
    }
    return std::nullopt;
}

batch_cache_index::read_result batch_cache_index::read(
  model::offset offset,
  model::offset max_offset,
  std::optional<model::record_batch_type> type_filter,
  std::optional<model::timestamp> first_ts,
  size_t max_bytes,
  bool skip_lru_promote) {
    lock_guard lk(*this);
    read_result ret;
    ret.next_batch = offset;
    if (unlikely(offset > max_offset)) {
        return ret;
    }
    for (auto it = find_first_contains(offset); it != _index.end();) {
        auto& batch = it->second->batch();

        auto take = !type_filter || type_filter == batch.header().type;
        take &= !first_ts || batch.header().first_timestamp >= *first_ts;

        if (take) {
            batch_cache::entry::lock_guard g(*it->second);
            ret.batches.emplace_back(batch.share());
            ret.memory_usage += batch.memory_usage();
            if (!skip_lru_promote) {
                _cache->touch(it->second);
            }
        }

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
        if (!it->second || !it->second->valid() || it->first != offset) {
            // compute the base offset of the next cached batch
            auto next_batch = std::find_if(
              it, _index.end(), [](const index_type::value_type& e) {
                  return e.second && e.second->valid();
              });
            if (next_batch != _index.end()) {
                ret.next_cached_batch
                  = next_batch->second->batch().base_offset();
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
    ret.next_batch = offset;
    return ret;
}

void batch_cache_index::truncate(model::offset offset) {
    lock_guard lk(*this);
    if (auto it = find_first(offset); it != _index.end()) {
        // rule out if possible, otherwise always be pessimistic
        if (
          it->second && it->second->valid()
          && !it->second->batch().contains(offset)) {
            ++it;
        }
        std::for_each(it, _index.end(), [this](index_type::value_type& e) {
            _cache->evict(std::move(e.second));
        });
        _index.erase(it, _index.end());
    }
}

std::ostream&
operator<<(std::ostream& os, const batch_cache::reclaim_options& opts) {
    fmt::print(
      os,
      "growth window {} stable window {} min_size {} max_size {}",
      opts.growth_window,
      opts.stable_window,
      opts.min_size,
      opts.max_size);
    return os;
}

std::ostream& operator<<(std::ostream& o, const batch_cache& b) {
    // NOTE: intrusive list have a O(N) for size.
    // Do _not_ print size of _lru
    return o << "{is_reclaiming:" << b.is_memory_reclaiming()
             << ", size_bytes: " << b._size_bytes
             << ", lru_empty:" << b._lru.empty() << "}";
}
std::ostream&
operator<<(std::ostream& o, const batch_cache_index::read_result& c) {
    o << "{batches:" << c.batches.size() << ", memory_usage:" << c.memory_usage
      << ", next_batch:" << c.next_batch << ", next_cache_batch:";
    if (c.next_cached_batch) {
        o << *c.next_cached_batch;
    } else {
        o << "nullopt";
    }
    return o << "}";
}
std::ostream& operator<<(std::ostream& o, const batch_cache_index& c) {
    return o << "{cache_size=" << c._index.size() << "}";
}

} // namespace storage
