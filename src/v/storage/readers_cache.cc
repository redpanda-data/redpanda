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
#include "storage/readers_cache.h"

#include "base/vlog.h"
#include "container/intrusive_list_helpers.h"
#include "model/fundamental.h"
#include "ssx/future-util.h"
#include "storage/logger.h"
#include "storage/types.h"
#include "utils/mutex.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>

#include <algorithm>
#include <chrono>

namespace storage {

readers_cache::readers_cache(
  model::ntp ntp,
  std::chrono::milliseconds eviction_timeout,
  config::binding<size_t> target_max_size)
  : _ntp(std::move(ntp))
  , _eviction_timeout(eviction_timeout)
  , _target_max_size(std::move(target_max_size))
  , _eviction_jitter(eviction_timeout) {
    _probe.setup_metrics(_ntp);
    // setup eviction timer
    _eviction_timer.set_callback([this] {
        ssx::spawn_with_gate(_gate, [this] {
            return maybe_evict().finally([this] {
                if (!_gate.is_closed()) {
                    _eviction_timer.arm(
                      _eviction_jitter.next_jitter_duration());
                }
            });
        });
    });

    _eviction_timer.arm(_eviction_jitter.next_jitter_duration());
}

model::record_batch_reader
readers_cache::put(std::unique_ptr<log_reader> reader) {
    if (
      _gate.is_closed()
      || reader->lease_range_base_offset() < model::offset{0}) {
        // do not cache reader with empty lease
        return model::record_batch_reader(std::move(reader));
    }
    // check if requested reader belongs to one of the locked range
    // range locked, do not insert into the cache
    if (intersects_with_locked_range(
          reader->lease_range_base_offset(),
          reader->lease_range_end_offset())) {
        vlog(
          stlog.trace,
          "{} - range is locked, not adding reader with lease [{},{}]",
          _ntp,
          reader->lease_range_base_offset(),
          reader->lease_range_end_offset());
        return model::record_batch_reader(std::move(reader));
    }

    vlog(
      stlog.trace,
      "{} - adding reader [{},{}]",
      _ntp,
      reader->lease_range_base_offset(),
      reader->lease_range_end_offset());

    auto ptr = new entry{.reader = std::move(reader)}; // NOLINT
    _in_use.push_back(*ptr);
    _probe.reader_added();
    maybe_evict_size();
    return ptr->make_cached_reader(this);
}

bool readers_cache::intersects_with_locked_range(
  model::offset reader_base_offset, model::offset reader_end_offset) const {
    auto lock_it = std::find_if(
      _locked_offset_ranges.begin(),
      _locked_offset_ranges.end(),
      [reader_base_offset, reader_end_offset](const offset_range& range) {
          return reader_base_offset <= range.second
                 && reader_end_offset >= range.first;
      });

    return lock_it != _locked_offset_ranges.end();
}

std::optional<model::record_batch_reader>
readers_cache::get_reader(const log_reader_config& cfg) {
    if (_gate.is_closed()) {
        return std::nullopt;
    }
    vassert(
      cfg.skip_readers_cache == false,
      "{} - invalid readers_cache request {}",
      _ntp,
      cfg);
    vlog(stlog.trace, "{} - trying to get reader for: {}", _ntp, cfg);
    uncounted_intrusive_list<entry, &entry::_hook> to_evict;
    /**
     * We use linear search since _readers intrusive list is small.
     */
    auto it = _readers.begin();
    while (it != _readers.end()) {
        const auto is_valid = it->reader->is_reusable() && it->valid;
        // TODO: think about relaxing this equality condition
        const auto offset_matches = it->reader->next_read_lower_bound()
                                    == cfg.start_offset;
        // if invalid we will dispose this entry in background
        if (!is_valid) {
            it = _readers.erase_and_dispose(
              it, [&to_evict](entry* e) { to_evict.push_back(*e); });
            continue;
        }
        if (offset_matches && is_valid) {
            // found matching reader
            break;
        }
        ++it;
    }
    /**
     * dispose unused readers in background
     */
    dispose_in_background(std::move(to_evict));
    if (it == _readers.end()) {
        _probe.cache_miss();
        vlog(stlog.trace, "{} - reader cache miss for: {}", _ntp, cfg);
        return std::nullopt;
    }
    auto& e = *it;
    vlog(stlog.trace, "{} - reader cache hit for: {}", _ntp, cfg);
    it->reader->reset_config(cfg);
    _probe.cache_hit();

    // we use cached_reader wrapper to track reader usage, when cached_reader is
    // destroyed we unlock reader and trigger eviction
    _readers.erase(_readers.iterator_to(e));
    _in_use.push_back(e);
    return e.make_cached_reader(this);
}

ss::future<> readers_cache::wait_for_no_inuse_readers() {
    return _in_use_reader_destroyed.wait([this] { return _in_use.empty(); });
}

ss::future<> readers_cache::stop() {
    if (_eviction_timer.armed()) {
        _eviction_timer.cancel();
    }
    /**
     * First we close the gate, this will prevent new readers from being
     * inserted to the cache
     */
    co_await _gate.close();
    /**
     * Next we wait for all active readers to finish
     */
    co_await wait_for_no_inuse_readers();
    /**
     * At this poit we are sure that all cached readers are not used anymore and
     * no new readers will be added to _readers list.
     *
     * Close and dispose cached readers
     */
    for (auto& r : _readers) {
        co_await r.reader->finally();
    }
    _readers.clear_and_dispose([](entry* e) {
        delete e; // NOLINT
    });
    /**
     * Stop and clear metrics as well or risk a double registrion on partition
     * movements. For details see
     * https://github.com/redpanda-data/redpanda/issues/5938
     */
    _probe.clear();
}

ss::future<readers_cache::range_lock_holder>
readers_cache::evict_segment_readers(ss::lw_shared_ptr<segment> s) {
    vlog(
      stlog.debug,
      "{} - evicting reader from cache, segment [{},{}] removal",
      _ntp,
      s->offsets().get_base_offset(),
      s->offsets().get_dirty_offset());
    return evict_range(
      s->offsets().get_base_offset(), s->offsets().get_dirty_offset());
}

ss::future<readers_cache::range_lock_holder>
readers_cache::evict_prefix_truncate(model::offset o) {
    vlog(stlog.debug, "{} - evicting reader prefix truncate {}", _ntp, o);
    offset_range range{model::offset::min(), o};
    _locked_offset_ranges.push_back(range);
    return evict_if(
             [o](entry& e) { return e.reader->next_read_lower_bound() <= o; })
      .then([this, range = std::move(range)]() mutable {
          return range_lock_holder(std::move(range), this);
      });
    ;
}

ss::future<readers_cache::range_lock_holder>
readers_cache::evict_truncate(model::offset o) {
    vlog(stlog.debug, "{} - evicting reader truncate {}", _ntp, o);
    offset_range range{o, model::offset::max()};
    _locked_offset_ranges.push_back(range);
    return evict_if(
             [o](entry& e) { return e.reader->lease_range_end_offset() >= o; })
      .then([this, range = std::move(range)]() mutable {
          return range_lock_holder(std::move(range), this);
      });
}

ss::future<readers_cache::range_lock_holder>
readers_cache::evict_range(model::offset base, model::offset end) {
    vlog(
      stlog.debug,
      "{} - evicting reader from cache, range [{},{}]",
      _ntp,
      base,
      end);
    offset_range range{base, end};
    _locked_offset_ranges.push_back(range);
    return evict_if([base, end](entry& e) {
               return !(
                 e.reader->lease_range_base_offset() > end
                 || e.reader->lease_range_end_offset() < base);
           })
      .then([this, range = std::move(range)]() mutable {
          return range_lock_holder(std::move(range), this);
      });
}

model::record_batch_reader
readers_cache::entry::make_cached_reader(readers_cache* cache) {
    class cached_reader_impl final : public model::record_batch_reader::impl {
    public:
        explicit cached_reader_impl(entry* e, readers_cache* c)
          : _underlying(e->reader.get())
          , _guard(e, c) {}
        cached_reader_impl(cached_reader_impl&&) noexcept = default;
        cached_reader_impl& operator=(cached_reader_impl&&) noexcept = default;

        cached_reader_impl(const cached_reader_impl&) noexcept = delete;
        cached_reader_impl& operator=(const cached_reader_impl&) noexcept
          = delete;

        bool is_end_of_stream() const final {
            return _underlying->is_end_of_stream();
        };

        ss::future<model::record_batch_reader::storage_t>
        do_load_slice(model::timeout_clock::time_point tout) final {
            return _underlying->do_load_slice(tout);
        }

        ss::future<> finally() noexcept final { return ss::now(); }

        void print(std::ostream& o) final { return _underlying->print(o); };
        ~cached_reader_impl() final = default;

    private:
        log_reader* _underlying;
        entry_guard _guard;
    };

    return model::make_record_batch_reader<cached_reader_impl>(this, cache);
}

readers_cache::~readers_cache() {
    vassert(
      _readers.empty() && _in_use.empty(),
      "readers cache have to be closed before destorying");
}

ss::future<> readers_cache::dispose_entries(
  uncounted_intrusive_list<entry, &entry::_hook> entries) {
    for (auto& e : entries) {
        co_await e.reader->finally();
    }

    entries.clear_and_dispose([this](entry* e) {
        vlog(
          stlog.trace,
          "{} - removing reader: [{},{}] lower_bound: {}",
          _ntp,
          e->reader->lease_range_base_offset(),
          e->reader->lease_range_end_offset(),
          e->reader->next_read_lower_bound());
        _probe.reader_evicted();
        delete e; // NOLINT
    });
}

void readers_cache::dispose_in_background(
  uncounted_intrusive_list<entry, &entry::_hook> entries) {
    ssx::spawn_with_gate(_gate, [this, entries = std::move(entries)]() mutable {
        return dispose_entries(std::move(entries));
    });
}

void readers_cache::dispose_in_background(entry* e) {
    if (_gate.is_closed()) {
        /**
         * since gate is closed and we failed to call finally on the reader we
         * add it back to _readers list, it will be gracefully closed in
         * `readers_cache::close`.
         * NOTE:
         * _readers intrusive list is supposed to keep resusable readers but
         * when gate closed exception is thrwon we are certain that no more
         * oprations will be executed on the cache so we can reuse the _readers
         * list to gracefully shutdown readers.
         */
        _readers.push_back(*e);
    } else {
        ssx::spawn_with_gate(_gate, [this, e] {
            return e->reader->finally().finally([this, e] {
                vlog(
                  stlog.trace,
                  "{} - removing reader: [{},{}] lower_bound: {}",
                  _ntp,
                  e->reader->lease_range_base_offset(),
                  e->reader->lease_range_end_offset(),
                  e->reader->next_read_lower_bound());
                _probe.reader_evicted();
                delete e; // NOLINT
            });
        });
    }
}

ss::future<> readers_cache::maybe_evict() {
    auto now = ss::lowres_clock::now();
    co_await evict_if([this, now](entry& e) {
        const auto invalid = !e.reader->is_reusable() || !e.valid;
        const auto outdated = e.last_used + _eviction_timeout < now;
        return invalid || outdated;
    });
}

inline bool readers_cache::over_size_limit() const {
    return !_readers.empty()
           && _readers.size() + _in_use.size() > _target_max_size();
}

void readers_cache::maybe_evict_size() {
    /**
     * exit early if there is nothing to clean
     */
    if (!over_size_limit()) [[likely]] {
        return;
    }

    uncounted_intrusive_list<entry, &entry::_hook> to_evict;
    _readers.pop_front_and_dispose(
      [&to_evict](entry* e) { to_evict.push_back(*e); });

    dispose_in_background(std::move(to_evict));
}

readers_cache::stats readers_cache::get_stats() const {
    return readers_cache::stats{
      .in_use_readers = _in_use.size(), .cached_readers = _readers.size()};
}

} // namespace storage
