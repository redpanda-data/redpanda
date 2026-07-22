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
#include "model/fundamental.h"
#include "resource_mgmt/available_memory.h"
#include "ssx/future-util.h"
#include "utils/to_string.h" // NOLINT(misc-include-cleaner) fmt::formatter for optionals

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/defer.hh>

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

model::record_batch batch_cache::range::batch(
  size_t data_offset, const model::record_batch_header& hdr) {
    vassert(_valid, "cannot access invalided batch");
    auto buffer = _arena.share(
      data_offset, hdr.size_bytes - model::packed_record_batch_header_size);

    return model::record_batch(
      hdr.copy(), std::move(buffer), model::record_batch::tag_ctor_ng{});
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

bool batch_cache::range::empty() const { return _offsets.empty(); }

bool batch_cache::range::fits(const model::record_batch& b) const {
    const size_t to_add = b.data().size_bytes();
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
    auto data_offset = _arena.size_bytes();
    // if there is not enough space left in last arena fragment we
    // trim it and append existing fragments directly to the arena
    // iobuf. the arena is empty when the range was constructed for a
    // single large batch.
    if (
      _arena.begin() == _arena.end()
      || _arena.rbegin()->available_bytes() < b.data().size_bytes()) {
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

    return data_offset;
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
        return entry(0, r->weak_from_this(), input.header());
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
    return entry(
      offset, index._small_batches_range->weak_from_this(), input.header());
}

batch_cache::~batch_cache() noexcept {
    clear();
    vassert(
      _size_bytes == 0 && _lru.empty() && _pending_index_removal.empty(),
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
        // the range is linked on either the lru list or, if its memory was
        // already reclaimed, the pending index removal list. the auto-unlink
        // hook removes it from whichever list holds it on destruction.
        delete p.get(); // NOLINT
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
     * given that the range isn't pinned (in which case it is skipped), the
     * batch's record data is reclaimed and the range is invalidated and moved
     * to the pending index removal list. invalidation is important because the
     * batch reference in the index still exists even though the batch data was
     * removed. removal of the index entries is completed by the background
     * reclaimer so that the potentially large number of index erases stays off
     * the memory allocation path (this reclaimer runs synchronously within
     * allocation).
     */
    size_t reclaimed = 0;

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
        it->invalidate();

        it = _lru.erase_and_dispose(
          it, [this](range* e) { _pending_index_removal.push_back(*e); });
    }

    if (reclaimed != 0) {
        _background_reclaimer.notify();
    }

    _last_reclaim = ss::lowres_clock::now();
    _size_bytes -= reclaimed;
    return reclaimed;
}

void batch_cache::dispose_pending(range* r) {
    auto* index = &r->_index;
    auto offsets = std::move(r->_offsets);
    delete r; // NOLINT

    /*
     * since reclaim may be invoked at any moment and removals are
     * deferred, one can imagine races in which a batch is removed by
     * offset here which is not the same batch that was reclaimed in a
     * prior pass. at worst this would raise the miss ratio, but is still
     * generally safe since all batch cache users are prepared to handle a
     * miss.
     */
    for (auto& o : offsets) {
        index->remove(o);
    }
}

ss::future<> batch_cache::do_pending_index_removals() {
    /*
     * detach the current pending set so that list mutations during scheduling
     * points (new arrivals from reclaim, ranges evicted by their index) don't
     * interfere with iteration.
     */
    intrusive_range_list work;
    work.splice(work.begin(), _pending_index_removal);
    while (!work.empty()) {
        dispose_pending(&work.front());
        co_await ss::coroutine::maybe_yield();
    }
}

void batch_cache::drain_pending_index_removals() {
    _pending_index_removal.clear_and_dispose(dispose_pending);
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
        auto& e = it->second;

        auto take = !type_filter || type_filter == e.type();
        take &= !first_ts || e.max_timestamp() >= *first_ts;
        offset = e.last_offset() + model::offset(1);
        if (take) {
            batch_cache::range::lock_guard g(*e.range());
            auto batch = e.batch();
            ret.memory_usage += batch.memory_usage();
            ret.batches.emplace_back(std::move(batch));
            if (!skip_lru_promote) {
                _cache->touch(e.range());
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
                ret.next_cached_batch = next_batch->first;
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

bool batch_cache_index::has_contiguous_coverage(
  model::offset from, model::offset to) const {
    if (from > to) {
        return true;
    }
    // Find the first entry whose base_offset may contain 'from'.
    auto it = _index.upper_bound(from);
    if (it != _index.begin()) {
        --it;
    }
    model::offset expected = from;
    while (it != _index.end() && expected <= to) {
        const auto& range = it->second.range();
        if (!range || !range->valid()) {
            return false;
        }
        if (it->first > expected) {
            return false;
        }
        auto next = model::next_offset(it->second.last_offset());
        if (next <= expected) {
            ++it;
            continue;
        }
        expected = next;
        ++it;
    }
    return expected > to;
}

model::offset batch_cache_index::contiguous_end(model::offset from) const {
    auto it = _index.upper_bound(from);
    if (it != _index.begin()) {
        --it;
    }
    model::offset expected = from;
    while (it != _index.end()) {
        const auto& range = it->second.range();
        if (!range || !range->valid()) {
            break;
        }
        if (it->first > expected) {
            break;
        }
        auto next = model::next_offset(it->second.last_offset());
        if (next <= expected) {
            ++it;
            continue;
        }
        expected = next;
        ++it;
    }
    return model::prev_offset(expected);
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
          && !(it->first <= offset && offset <= it->second.last_offset())) {
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
    vassert(
      _dirty_tracker.clean(),
      "Destroying batch_cache_index ({}) tracking dirty batches.",
      *this);
    /*
     * clear in bounded chunks, restarting from the beginning of the index at
     * each scheduling point, so that no btree iterator lives across a yield.
     */
    while (!_index.empty()) {
        auto it = _index.begin();
        do {
            _cache->evict(std::move(it->second.range()));
            it = _index.erase(it);
        } while (it != _index.end() && !ss::need_preempt());
        co_await ss::coroutine::maybe_yield();
    }
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

        co_await _cache.do_pending_index_removals();

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

} // namespace storage
