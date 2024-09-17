// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "batch_cache.h"

#include "base/vassert.h"
#include "bytes/iobuf_parser.h"
#include "model/adl_serde.h"
#include "model/fundamental.h"
#include "resource_mgmt/available_memory.h"
#include "ssx/async_algorithm.h"
#include "ssx/future-util.h"
#include "utils/to_string.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/util/defer.hh>

#include <fmt/ostream.h>

namespace storage {

batch_cache::range::range(batch_cache_index& index)
  : _index(index) {
    auto f = std::make_unique<details::io_fragment>(range_size);
    _arena.append(std::move(f));
}

batch_cache::range::range(
  batch_cache_index& index,
  const model::record_batch& batch,
  is_dirty_entry dirty)
  : _index(index) {
    add(batch, dirty);
}

model::record_batch batch_cache::range::batch(size_t o) {
    vassert(_valid, "cannot access invalided batch");
    iobuf_const_parser parser(_arena);
    parser.skip(o);
    auto hdr = reflection::adl<model::record_batch_header>{}.from(parser);
    auto buffer = _arena.share(
      o + serialized_header_size,
      hdr.size_bytes - model::packed_record_batch_header_size);

    return model::record_batch(
      hdr, std::move(buffer), model::record_batch::tag_ctor_ng{});
}

model::record_batch_header batch_cache::range::header(size_t o) {
    vassert(_valid, "cannot access invalided batch");
    iobuf_const_parser parser(_arena);
    parser.skip(o);
    return reflection::adl<model::record_batch_header>{}.from(parser);
}

size_t batch_cache::range::memory_size() const {
    // actual memory allocated by arena iobuf must be calculated
    // taking capacity into account.
    return std::accumulate(
      _arena.begin(),
      _arena.end(),
      (size_t)0,
      [](size_t acc, const details::io_fragment& f) {
          return acc + f.capacity();
      });
}

double batch_cache::range::waste() const {
    return (1.0 - ((double)_size / memory_size())) * 100.0;
}

bool batch_cache::range::empty() const { return _size == 0; }

bool batch_cache::range::fits(const model::record_batch& b) const {
    const size_t to_add = serialized_header_size + b.data().size_bytes();
    // if there are not enough bytes in current range return true even
    // though batch doesn't fit into arena. This way we can control maximum
    // waste
    if (_size <= min_bytes_in_range) {
        return true;
    }
    // if size is already larger than the max range do not append
    // batch to current range
    if (_size >= range_size) {
        return false;
    }
    // check if batch fits in into left space
    auto space_left = range_size - _size;
    return space_left >= to_add;
}

uint32_t
batch_cache::range::add(const model::record_batch& b, is_dirty_entry dirty) {
    auto offset = _arena.size_bytes();
    reflection::adl<model::record_batch_header>{}.to(_arena, b.header().copy());
    _size += serialized_header_size;
    // if there is not enough space left in last arena fragment we
    // trim it and append existing fragments directly to the arena
    // iobuf
    if (_arena.rbegin()->available_bytes() < b.data().size_bytes()) {
        _size += b.data().size_bytes();
        _arena.append_fragments(b.data().copy());
    } else {
        // if there is enough space in arena just copy data into
        // existing fragment
        for (auto& f : b.data()) {
            _arena.append(f.get(), f.size());
            _size += f.size();
        }
    }

    _offsets.push_back(b.base_offset());
    if (dirty) {
        vassert(
          _max_dirty_offset < b.last_offset(),
          "Dirty batch base offsets must be monotonically increasing. "
          "Adding: {}, Prev: {}",
          b.last_offset(),
          _max_dirty_offset);
        _max_dirty_offset = b.last_offset();
    }

    return offset;
}

static resources::available_memory::deregister_holder
register_memory_reporter(const batch_cache& bc) {
    auto& ab = resources::available_memory::local();
    return ab.register_reporter(
      "batch_cache", [&bc] { return bc.size_bytes(); });
}

batch_cache::batch_cache(const reclaim_options& opts)
  : _reclaimer(
      [this](reclaimer::request r) { return reclaim(r); }, reclaim_scope::sync)
  , _reclaim_opts(opts)
  , _reclaim_size(_reclaim_opts.min_size)
  , _background_reclaimer(
      *this, opts.min_free_memory, opts.background_reclaimer_sg)
  , _available_mem_deregister(register_memory_reporter(*this)) {
    _background_reclaimer.start();
}

batch_cache::entry batch_cache::put(
  batch_cache_index& index,
  const model::record_batch& input,
  is_dirty_entry dirty) {
    // notify no matter what the exit path
    auto notify_guard = ss::defer([this] { _background_reclaimer.notify(); });

#ifdef SEASTAR_DEFAULT_ALLOCATOR
    static const size_t threshold = ss::memory::stats().total_memory() * .2;
    while (_size_bytes > threshold) {
        reclaim(1);
    }
#endif

    // we must copy memory to prevent holding onto bigger memory from
    // temporary buffers

    // if weak_from_this were to cause an allocation--which it
    // shouldn't--`e` wouldn't be visible to the reclaimer since it
    // isn't on a lru/pool list.

    if (static_cast<size_t>(input.size_bytes()) > range::range_size) {
        auto r = new range(index, input, dirty);
        _lru.push_back(*r);
        _size_bytes += r->memory_size();
        return entry(0, r->weak_from_this());
    }

    if (
      !index._small_batches_range || !index._small_batches_range->valid()
      || !index._small_batches_range->fits(input)) {
        auto r = new range(index);
        _lru.push_back(*r);
        _size_bytes += r->memory_size();
        index._small_batches_range = r->weak_from_this();
    }

    auto initial_sz = index._small_batches_range->memory_size();
    auto offset = index._small_batches_range->add(input, dirty);
    // calculate size difference to update batch cache size
    int64_t diff = (int64_t)index._small_batches_range->memory_size()
                   - initial_sz;
    _size_bytes += diff;
    return entry(offset, index._small_batches_range->weak_from_this());
}

batch_cache::~batch_cache() noexcept {
    clear();
    vassert(
      _size_bytes == 0 && _lru.empty(),
      "Detected incorrect batch_cache accounting. {}",
      *this);
}

void batch_cache::evict(range_ptr&& e) {
    if (e) {
        vassert(
          e->clean(),
          "Requested to evict a range with dirty data: Max dirty offset: {}",
          e->_max_dirty_offset);

        // it's necessary to cause `e` to be sinked so the move constructor
        // invalidates the caller's range_ptr. simply interacting with the
        // r-value reference `e` wouldn't do that.
        auto p = std::exchange(e, {});
        _size_bytes -= p->memory_size();
        _lru.erase_and_dispose(
          _lru.iterator_to(*p), [](range* e) { delete e; });
    }
}

size_t batch_cache::reclaim(size_t size) {
    // update the available_memory low-water mark: this is a good place to do
    // this because under memory pressure the reclaimer will be called
    // frequently so we expect the LWM to track closely the true LWM if we
    // update it here
    resources::available_memory().update_low_water_mark();

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
     * reclaiming is a two pass process. given that the range isn't pinned (in
     * which case it is skipped), the first step is to reclaim the batch's
     * record data. at this point, if the range's owning index is not locked the
     * range is added to a temporary list of entries which will be removed.
     * otherwise if the index is locked, removal is deferred but the range is
     * invalidated. invalidation is important because the batch reference in the
     * index still exists even though the batch data was removed.
     */
    size_t reclaimed = 0;
    intrusive_list<range, &range::_hook> reclaimed_ranges;

    for (auto it = _lru.begin(); it != _lru.end();) {
        if (reclaimed >= _reclaim_size) {
            break;
        }

        // skip any range that has a live reference.
        if (unlikely(it->pinned() || !it->clean())) {
            ++it;
            continue;
        }
        // if entry is empty it will be disposed by other reclaim caller
        if (unlikely(it->empty())) {
            continue;
        }
        // reclaim the batch's record data
        reclaimed += it->memory_size();
        it->_arena.clear();

        /*
         * if the owning index is locked invalidate the range but leave it on
         * the lru list for deferred deletion so as to not invalidate any open
         * iterators on the index.
         */
        if (unlikely(it->_index.locked())) {
            it->invalidate();
            ++it;
            continue;
        }

        // collect the entries that will be fully removed
        it = _lru.erase_and_dispose(it, [&reclaimed_ranges](range* e) {
            reclaimed_ranges.push_back(*e);
        });
    }

    /*
     * final removal from the index is deferred because there is some chance
     * that removal allocates, so waiting until the bulk of the reclaims have
     * occurred reduces the probability of an allocation failure.
     */

    reclaimed_ranges.clear_and_dispose([](range* e) {
        auto* index = &e->_index;
        auto offsets = std::move(e->_offsets);
        delete e; // NOLINT

        /*
         * since reclaim may be invoked at any moment and removals may be
         * deferred if an index is locked, one can imagine races in which a
         * batch is removed by offset here which is not the same batch that was
         * reclaimed in a prior pass. at worst this would raise the miss ratio,
         * but is still generally safe since all batch cache users are prepared
         * to handle a miss.
         */
        for (auto& o : offsets) {
            index->remove(o);
        }
    });

    _last_reclaim = ss::lowres_clock::now();
    _size_bytes -= reclaimed;
    return reclaimed;
}

void batch_cache_index::dirty_tracker::mark_dirty(
  const std::pair<model::offset, model::offset> range) {
    if (_min == model::offset{}) {
        _min = range.first;
        _max = range.second;
    } else {
        vassert(
          _max <= range.first && _max < range.second,
          "newly tracked offset must be above any previously seen offsets "
          "(inserting: [{}, {}], state: {})",
          range.first,
          range.second,
          *this);
        _max = range.second;
    }
}

void batch_cache_index::dirty_tracker::mark_clean(
  const model::offset up_to_inclusive) {
    if (up_to_inclusive >= _max) {
        _min = model::offset{};
        _max = model::offset{};
    } else {
        _min = std::max(_min, up_to_inclusive);
    }
}

std::optional<model::record_batch>
batch_cache_index::get(model::offset offset) {
    lock_guard lk(*this);
    if (auto it = find_first_contains(offset); it != _index.end()) {
        batch_cache::range::lock_guard g(*it->second.range());
        _cache->touch(it->second.range());
        return it->second.batch();
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
        auto batch = it->second.batch();

        auto take = !type_filter || type_filter == batch.header().type;
        take &= !first_ts || batch.header().max_timestamp >= *first_ts;
        offset = batch.last_offset() + model::offset(1);
        if (take) {
            batch_cache::range::lock_guard g(*it->second.range());
            ret.memory_usage += batch.memory_usage();
            ret.batches.emplace_back(std::move(batch));
            if (!skip_lru_promote) {
                _cache->touch(it->second.range());
            }
        }

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
        if (
          !it->second.range() || !it->second.range()->valid()
          || it->first != offset) {
            // compute the base offset of the next cached batch
            auto next_batch = std::find_if(
              it, _index.end(), [](const index_type::value_type& e) {
                  return e.second.range() && e.second.range()->valid();
              });
            if (next_batch != _index.end()) {
                ret.next_cached_batch = next_batch->second.header().base_offset;
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

    vassert(
      _dirty_tracker.clean(),
      "truncate() with dirty data in the index ({}).",
      *this);

    if (auto it = find_first(offset); it != _index.end()) {
        // rule out if possible, otherwise always be pessimistic
        if (
          it->second.range() && it->second.valid()
          && !it->second.header().contains(offset)) {
            ++it;
        }
        std::for_each(it, _index.end(), [this](index_type::value_type& e) {
            _cache->evict(std::move(e.second.range()));
        });
        _index.erase(it, _index.end());
    }
}

void batch_cache_index::mark_clean(model::offset up_to_inclusive) {
    lock_guard lk(*this);

    if (_dirty_tracker.clean() || up_to_inclusive < _dirty_tracker.min()) {
        // No dirty data in the cache.
        return;
    }

    auto first = find_first(_dirty_tracker.min());
    vassert(
      first != _index.end(),
      "Iterator must exist if dirty tracker isn't clean.");

    auto last = std::next(find_first(up_to_inclusive));

    std::for_each(first, last, [up_to_inclusive](index_type::value_type& e) {
        e.second.range()->mark_clean(up_to_inclusive);
    });

    _dirty_tracker.mark_clean(up_to_inclusive);
}
ss::future<> batch_cache_index::clear_async() {
    lock_guard lk(*this);
    vassert(
      _dirty_tracker.clean(),
      "Destroying batch_cache_index ({}) tracking dirty batches.",
      *this);
    co_await ssx::async_for_each(
      _index.begin(), _index.end(), [this](index_type::value_type& value) {
          _cache->evict(std::move(value.second.range()));
      });
    _index.clear();
}

void batch_cache::background_reclaimer::start() {
    ssx::spawn_with_gate(_gate, [this] {
        return ss::with_scheduling_group(
          _sg, [this] { return reclaim_loop(); });
    });
}
ss::future<> batch_cache::background_reclaimer::stop() {
    _stopped = true;
    _change.signal();
    return _gate.close();
}
ss::future<> batch_cache::background_reclaimer::reclaim_loop() {
    while (!_stopped) {
        auto units = std::max(_change.current(), size_t(1));
        co_await _change.wait(units);

        if (unlikely(_stopped)) {
            co_return;
        }

        if (!have_to_reclaim()) {
            continue;
        }

        auto free = ss::memory::stats().free_memory();

        if (free < _min_free_memory) {
            auto to_reclaim = _min_free_memory - free;
            _cache.reclaim(to_reclaim);
        }
    }
    co_return;
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
    return o << "{cache_size=" << c._index.size()
             << ", dirty tracker: " << c._dirty_tracker << "}";
}

} // namespace storage
