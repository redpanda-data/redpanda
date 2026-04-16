// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "lsm/db/table_cache.h"

#include "base/vassert.h"
#include "base/vlog.h"
#include "container/chunked_hash_map.h"
#include "lsm/core/exceptions.h"
#include "lsm/core/internal/files.h"
#include "lsm/core/internal/iterator.h"
#include "lsm/core/internal/logger.h"
#include "lsm/sst/reader.h"
#include "ssx/mutex.h"
#include "ssx/work_queue.h"
#include "utils/s3_fifo.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/weak_ptr.hh>

#include <exception>
#include <utility>

namespace lsm::db {

namespace {
/**
 * A RAII scoped lock that ensures reader locks are deleted when there are no
 * waiters.
 */
class reader_lock_guard {
public:
    reader_lock_guard(const reader_lock_guard&) = delete;
    reader_lock_guard& operator=(const reader_lock_guard&) = delete;
    reader_lock_guard(reader_lock_guard&&) noexcept = default;
    reader_lock_guard& operator=(reader_lock_guard&&) noexcept = default;

    static ss::future<reader_lock_guard> acquire(
      chunked_hash_map<internal::file_handle, std::unique_ptr<ssx::mutex>>*
        mu_map,
      internal::file_handle h) {
        auto it = mu_map->find(h);
        ssx::mutex* mu = nullptr;
        if (it == mu_map->end()) {
            auto inserted = mu_map->emplace(
              h, std::make_unique<ssx::mutex>("lsm::db::reader_lock_guard"));
            vassert(inserted.second, "expected mutex to be inserted");
            mu = inserted.first->second.get();
        } else {
            mu = it->second.get();
        }
        ssx::mutex::units units = co_await mu->get_units();
        co_return reader_lock_guard(h, mu_map, mu, std::move(units));
    }

    ~reader_lock_guard() {
        _underlying.return_all();
        // If nothing is waiting on or holding the mutex, we can remove the lock
        // from the map.
        if (_mu->ready()) {
            _mu_map->erase(_handle);
        }
    }

private:
    reader_lock_guard(
      internal::file_handle h,
      chunked_hash_map<internal::file_handle, std::unique_ptr<ssx::mutex>>*
        mu_map,
      ssx::mutex* mu,
      ssx::mutex::units underlying)
      : _handle(h)
      , _mu_map(mu_map)
      , _mu(mu)
      , _underlying(std::move(underlying)) {}
    internal::file_handle _handle;
    chunked_hash_map<internal::file_handle, std::unique_ptr<ssx::mutex>>*
      _mu_map;
    ssx::mutex* _mu;
    ssx::mutex::units _underlying;
};
} // namespace

class table_cache::impl {
    // To ensure that we don't close/evict readers while there are pending
    // iterators for them, we let them live longer and enqueue them for cleanup
    // when destructed.
    class wrapped_iterator : public internal::iterator {
    public:
        wrapped_iterator(
          table_cache::impl* cache,
          ss::lw_shared_ptr<sst::reader> reader,
          internal::iterator_options opts)
          : _cache(cache)
          , _reader(std::move(reader))
          , _underlying(_reader->create_iterator(opts)) {}
        wrapped_iterator(const wrapped_iterator&) = delete;
        wrapped_iterator(wrapped_iterator&&) = delete;
        wrapped_iterator& operator=(const wrapped_iterator&) = delete;
        wrapped_iterator& operator=(wrapped_iterator&&) = delete;
        ~wrapped_iterator() override {
            _cache->maybe_enqueue_cleanup(std::exchange(_reader, {}));
        }
        bool valid() const override { return _underlying->valid(); }
        ss::future<> seek_to_first() override {
            return _underlying->seek_to_first();
        }
        ss::future<> seek_to_last() override {
            return _underlying->seek_to_last();
        }
        ss::future<> seek(internal::key_view target) override {
            return _underlying->seek(target);
        }
        ss::future<> next() override { return _underlying->next(); }
        ss::future<> prev() override { return _underlying->prev(); }
        internal::key_view key() override { return _underlying->key(); }
        iobuf value() override { return _underlying->value(); }

    private:
        table_cache::impl* _cache;
        ss::lw_shared_ptr<sst::reader> _reader;
        std::unique_ptr<internal::iterator> _underlying;
    };

public:
    impl(
      io::data_persistence* p,
      size_t max_entries,
      ss::lw_shared_ptr<probe> probe,
      ss::lw_shared_ptr<sst::block_cache> block_cache)
      : _persistence(p)
      , _probe(std::move(probe))
      , _cache(compute_cache_config(max_entries), eviction(this))
      , _block_cache(std::move(block_cache))
      , _cleanup_queue([](const std::exception_ptr& ex) {
          vlog(log.error, "table_cache_cleanup_error error=\"{}\"", ex);
      }) {}

    ss::future<std::unique_ptr<internal::iterator>> create_iterator(
      internal::file_handle h,
      uint64_t file_size,
      internal::iterator_options opts) {
        auto table = co_await find_reader(h, file_size);
        co_return std::make_unique<wrapped_iterator>(this, table, opts);
    }

    ss::future<> get(
      internal::file_handle h,
      uint64_t file_size,
      internal::key_view key,
      absl::FunctionRef<ss::future<>(internal::key_view, iobuf)> fn) {
        auto table = co_await find_reader(h, file_size);
        co_await table->internal_get(key, fn);
        // It's possible (although unlikely), that while `internal_get` was
        // going on the table was evicted from the cache, if that's the case
        // we need to enqueue the cleanup, as the ghost fifo gc would have not
        // done it.
        maybe_enqueue_cleanup(std::move(table));
    }

    ss::future<> evict(internal::file_handle h) {
        gc_ghost_fifo();
        auto guard = co_await reader_lock_guard::acquire(&_mu_map, h);
        auto it = _map.find(h);
        if (it == _map.end()) {
            co_return;
        }
        _cache.remove(*it->second);
        if (it->second->ghost_hook.is_linked()) {
            _ghost_fifo.erase(_ghost_fifo.iterator_to(*it->second));
        }
        auto reader = std::exchange(it->second->value, {});
        _map.erase(it);
        vassert(
          reader.use_count() == 1,
          "expected only a single reference for {} when evicting, was: {}",
          h,
          reader.use_count());
        co_await reader->close();
    }

    ss::future<> close() {
        for (auto& [h, entry] : _map) {
            _cache.remove(*entry);
            auto reader = std::exchange(entry->value, {});
            vassert(
              reader.use_count() == 1,
              "expected only a single reference for {} when closing, was: {}",
              h,
              reader.use_count());
            co_await reader->close();
        }
        _ghost_fifo.clear();
        _map.clear();
        ss::promise<void> p;
        _cleanup_queue.submit([&p] {
            p.set_value();
            return ss::now();
        });
        co_await p.get_future();
        co_await _cleanup_queue.shutdown();
    }

    // Take an r-value so that we don't accidently make a copy and preserving
    // the ref count.
    void maybe_enqueue_cleanup(ss::lw_shared_ptr<sst::reader>&& reader) {
        auto r = std::move(reader);
        if (r.use_count() == 1) {
            _cleanup_queue.submit([this, r = std::move(r)] {
                return r->close().finally(
                  [this] { --_handles_pending_cleanup; });
            });
        }
    }

    table_cache::stat stats() const {
        auto cache_stats = _cache.stat();
        return {
          .open_file_handles = _map.size() + _handles_pending_cleanup,
          .small_queue_size = cache_stats.small_queue_size,
          .main_queue_size = cache_stats.main_queue_size,
        };
    }

private:
    using ghost_hook_t = boost::intrusive::list_member_hook<
      boost::intrusive::link_mode<boost::intrusive::safe_link>>;

    struct cached_value {
        internal::file_handle handle;
        ss::lw_shared_ptr<sst::reader> value;
        utils::s3_fifo::cache_hook hook;
        ghost_hook_t ghost_hook;
    };

    using entry_t = std::unique_ptr<cached_value>;
    using ghost_fifo_t = boost::intrusive::list<
      cached_value,
      boost::intrusive::
        member_hook<cached_value, ghost_hook_t, &cached_value::ghost_hook>>;

    struct eviction {
        table_cache::impl* impl;
        bool operator()(cached_value& e) noexcept {
            impl->_ghost_fifo.push_back(e);
            return true;
        }
    };

    using cache_t = utils::s3_fifo::cache<
      cached_value,
      &cached_value::hook,
      eviction,
      utils::s3_fifo::default_cache_cost>;

    static cache_t::config compute_cache_config(size_t max_entries) {
        // In s3_fifo they recommend the small queue to be ~10% of the main
        // queue. The ghost queue and main queue are the same size. So we split
        // up our queue into 3 chunks:
        // 45% -> main queue
        // 45% -> ghost queue
        // 10% -> small queue
        auto main_cache_size = static_cast<size_t>(
          static_cast<double>(max_entries) * 0.45);
        return cache_t::config{
          .cache_size = main_cache_size,
          .small_size = max_entries - (2 * main_cache_size),
        };
    }

    ss::future<ss::lw_shared_ptr<sst::reader>>
    find_reader(internal::file_handle handle, uint64_t file_size) {
        gc_ghost_fifo();
        auto it = _map.find(handle);
        if (it == _map.end()) {
            // Use fine grained locking to prevent opening unrelated tables from
            // being a bottleneck.
            auto guard = co_await reader_lock_guard::acquire(&_mu_map, handle);
            // Make sure since we had a scheduling point that something else
            // didn't come along and insert what we were looking for into the
            // map, if it did, we can continue as usual, as the units will be
            // released after the `if` statement.
            it = _map.find(handle);
            if (it == _map.end()) {
                auto reader = co_await open_reader(handle, file_size);
                auto [it, succ] = _map.try_emplace(
                  handle,
                  std::make_unique<cached_value>(handle, std::move(reader)));
                vassert(succ, "lock is held, who mutated _map?");
                _cache.insert(*it->second);
                _probe->table_cache_miss += 1;
                co_return it->second->value;
            }
        }
        _probe->table_cache_hit += 1;
        auto& entry = *it->second;
        if (entry.hook.evicted()) {
            // If this was evicted, but on the ghost queue, then we can reinsert
            // it into the cache and keep the existing entry alive.
            _ghost_fifo.erase(_ghost_fifo.iterator_to(entry));
            _cache.insert(entry);
        }
        entry.hook.touch();
        co_return entry.value;
    }

    ss::future<ss::lw_shared_ptr<sst::reader>>
    open_reader(internal::file_handle h, uint64_t file_size) {
        auto file = co_await _persistence->open_random_access_reader(h);
        if (!file) {
            throw invalid_argument_exception("file for ID {} is not found", h);
        }
        auto reader = co_await sst::reader::open(
          std::move(*file), h.id, file_size, _block_cache);
        co_return ss::make_lw_shared(std::move(reader));
    }

    void gc_ghost_fifo() {
        for (auto it = _ghost_fifo.begin(); it != _ghost_fifo.end();) {
            auto& entry = *it;
            if (_cache.ghost_queue_contains(entry)) {
                // The ghost queue is in fifo-order so any entry that comes
                // after an entry that hasn't been evicted will also not be
                // evicted.
                return;
            }
            ++_handles_pending_cleanup;
            maybe_enqueue_cleanup(std::exchange(entry.value, {}));
            it = _ghost_fifo.erase(it);
            _map.erase(entry.handle);
        }
    }

    io::data_persistence* _persistence;
    ss::lw_shared_ptr<probe> _probe;
    chunked_hash_map<internal::file_handle, std::unique_ptr<ssx::mutex>>
      _mu_map;
    chunked_hash_map<internal::file_handle, entry_t> _map;
    cache_t _cache;
    // Entries that have been "soft evicted" from the cache. We keep them around
    // just in case and GC them after some period of time.
    ghost_fifo_t _ghost_fifo;
    ss::lw_shared_ptr<sst::block_cache> _block_cache;
    // Once an item is evicted, we need to also check that outstanding iterators
    // are closed. If they are not, then we wait until they are, then we insert
    // this onto this queue (since we are in a destructor, we can't await the
    // close there).
    ssx::work_queue _cleanup_queue;
    size_t _handles_pending_cleanup = 0;
};

table_cache::table_cache(
  io::data_persistence* persistence,
  size_t max_entries,
  ss::lw_shared_ptr<probe> probe,
  ss::lw_shared_ptr<sst::block_cache> block_cache)
  : _impl(
      std::make_unique<impl>(
        persistence, max_entries, std::move(probe), std::move(block_cache))) {}

table_cache::~table_cache() = default;

ss::future<std::unique_ptr<internal::iterator>> table_cache::create_iterator(
  internal::file_handle h,
  uint64_t file_size,
  internal::iterator_options opts) {
    return _impl->create_iterator(h, file_size, opts);
}

ss::future<> table_cache::get(
  internal::file_handle h,
  uint64_t file_size,
  internal::key_view key,
  absl::FunctionRef<ss::future<>(internal::key_view, iobuf)> fn) {
    return _impl->get(h, file_size, key, fn);
}

ss::future<> table_cache::evict(internal::file_handle h) {
    return _impl->evict(h);
}

ss::future<> table_cache::close() { return _impl->close(); }

table_cache::stat table_cache::statistics() const { return _impl->stats(); }

fmt::iterator table_cache::stat::format_to(fmt::iterator it) const {
    auto total_queue_size = main_queue_size + small_queue_size;
    auto ghost_queue_size = open_file_handles - total_queue_size;
    return fmt::format_to(
      it,
      "{{open_handles:{},main_queue:{},small_queue:{},ghost_queue:{}}}",
      open_file_handles,
      main_queue_size,
      small_queue_size,
      ghost_queue_size);
}

} // namespace lsm::db
