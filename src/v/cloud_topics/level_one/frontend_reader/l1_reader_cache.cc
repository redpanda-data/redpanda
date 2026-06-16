/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/frontend_reader/l1_reader_cache.h"

#include "cloud_topics/logger.h"
#include "config/configuration.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"
#include "random/simple_time_jitter.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/lowres_clock.hh>

namespace cloud_topics {

l1_reader_cache::l1_reader_cache(
  config::binding<std::chrono::milliseconds> eviction_timeout,
  config::binding<size_t> target_max_size)
  : _eviction_timeout(std::move(eviction_timeout))
  , _target_max_size(std::move(target_max_size)) {
    _eviction_timer.set_callback([this] {
        ssx::spawn_with_gate(_gate, [this] {
            return maybe_evict().finally([this] { arm_eviction_timer(); });
        });
    });
    arm_eviction_timer();
    setup_metrics();
}

l1_reader_cache::~l1_reader_cache() {
    vassert(
      _readers.empty() && _in_use.empty(),
      "l1_reader_cache must be stopped before destruction");
}

std::optional<model::record_batch_reader> l1_reader_cache::get_reader(
  const model::topic_id_partition& tidp,
  const cloud_topic_log_reader_config& cfg) {
    if (_gate.is_closed()) {
        return std::nullopt;
    }

    auto it = _readers.begin();
    bool saw_tidp = false;
    while (it != _readers.end()) {
        if (it->reader->tidp() == tidp) {
            // CORE-15812: a reader for this partition is cached. If it also
            // matches the requested offset it's a hit; otherwise the reader
            // is positioned at an offset the fetch isn't asking for, i.e. it
            // read past what the consumer re-requests (over-read).
            saw_tidp = true;
            if (
              it->reader->is_reusable()
              && it->reader->next_read_lower_bound() == cfg.start_offset) {
                break;
            }
        }
        ++it;
    }

    if (it == _readers.end()) {
        ++_cache_misses;
        if (saw_tidp) {
            ++_offset_mismatch;
        }
        return std::nullopt;
    }

    auto& e = *it;
    e.reader->reset_config(cfg);
    ++_cache_hits;

    _readers.erase(_readers.iterator_to(e));
    _in_use.push_back(e);
    return e.make_cached_reader(this);
}

model::record_batch_reader
l1_reader_cache::put(std::unique_ptr<level_one_log_reader_impl> reader) {
    if (_gate.is_closed()) {
        return model::record_batch_reader(std::move(reader));
    }

    auto* ptr = new entry{.reader = std::move(reader)}; // NOLINT
    _in_use.push_back(*ptr);
    ++_readers_added;
    maybe_evict_size();
    return ptr->make_cached_reader(this);
}

l1_reader_cache::stats l1_reader_cache::get_stats() const {
    return stats{
      .in_use_readers = _in_use.size(),
      .cached_readers = _readers.size(),
    };
}

l1_reader_cache::entry_guard::~entry_guard() noexcept {
    _cache->_in_use.erase(_cache->_in_use.iterator_to(*_e));
    if (_e->reader->is_reusable()) {
        _e->last_used = ss::lowres_clock::now();
        _cache->_readers.push_back(*_e);
    } else {
        // CORE-15812: a reader returned non-reusable can't be cached. Record
        // whether it ended at end-of-stream (read to the frontier / true end
        // -> B) vs lost its stream some other way (abort/error).
        if (_e->reader->is_end_of_stream()) {
            ++_cache->_evicted_eos;
        } else {
            ++_cache->_evicted_not_reusable;
        }
        _cache->dispose_in_background(_e);
    }
    _cache->_in_use_reader_destroyed.broadcast();
}

model::record_batch_reader
l1_reader_cache::entry::make_cached_reader(l1_reader_cache* cache) {
    class cached_reader_impl final : public model::record_batch_reader::impl {
    public:
        explicit cached_reader_impl(entry* e, l1_reader_cache* c)
          : _underlying(e->reader.get())
          , _guard(e, c) {}
        cached_reader_impl(cached_reader_impl&&) noexcept = default;
        cached_reader_impl& operator=(cached_reader_impl&&) noexcept = default;
        cached_reader_impl(const cached_reader_impl&) = delete;
        cached_reader_impl& operator=(const cached_reader_impl&) = delete;

        bool is_end_of_stream() const final {
            return _underlying->is_end_of_stream();
        }

        ss::future<model::record_batch_reader::storage_t>
        do_load_slice(model::timeout_clock::time_point tout) final {
            return _underlying->do_load_slice(tout);
        }

        std::optional<private_flags> get_flags() const final {
            return _underlying->get_flags();
        }

        ss::future<> finally() noexcept final { return ss::now(); }

        fmt::iterator format_to(fmt::iterator it) const final {
            return _underlying->format_to(it);
        }
        ~cached_reader_impl() final = default;

    private:
        level_one_log_reader_impl* _underlying;
        entry_guard _guard;
    };

    return model::make_record_batch_reader<cached_reader_impl>(this, cache);
}

ss::future<> l1_reader_cache::wait_for_no_inuse_readers() {
    return _in_use_reader_destroyed.wait([this] { return _in_use.empty(); });
}

ss::future<> l1_reader_cache::stop() {
    if (_eviction_timer.armed()) {
        _eviction_timer.cancel();
    }
    co_await _gate.close();
    co_await wait_for_no_inuse_readers();

    // Close all cached readers' streams.
    for (auto& r : _readers) {
        co_await r.reader->finally();
    }
    _readers.clear_and_dispose([](entry* e) {
        delete e; // NOLINT
    });
}

void l1_reader_cache::dispose_in_background(entry* e) {
    if (_gate.is_closed()) {
        _readers.push_back(*e);
    } else {
        ++_readers_evicted;
        ssx::spawn_with_gate(_gate, [e] {
            return e->reader->finally().finally([e] {
                delete e; // NOLINT
            });
        });
    }
}

void l1_reader_cache::arm_eviction_timer() {
    if (_gate.is_closed()) {
        return;
    }
    auto timeout = _eviction_timeout();
    if (timeout > std::chrono::milliseconds::zero()) {
        _eviction_timer.arm(
          simple_time_jitter<ss::lowres_clock>(timeout).next_duration());
    }
}

ss::future<> l1_reader_cache::maybe_evict() {
    auto now = ss::lowres_clock::now();
    uncounted_intrusive_list<entry, &entry::_hook> to_evict;

    for (auto it = _readers.begin(); it != _readers.end();) {
        const auto invalid = !it->reader->is_reusable();
        const auto outdated = it->last_used + _eviction_timeout() < now;
        if (invalid || outdated) {
            it = _readers.erase_and_dispose(
              it, [&to_evict](entry* e) { to_evict.push_back(*e); });
        } else {
            ++it;
        }
    }

    for (auto it = to_evict.begin(); it != to_evict.end();) {
        auto* e = &*it;
        ++it;
        to_evict.erase(to_evict.iterator_to(*e));
        co_await e->reader->finally();
        ++_readers_evicted;
        delete e; // NOLINT
    }
}

bool l1_reader_cache::over_size_limit() const {
    return !_readers.empty()
           && _readers.size() + _in_use.size() > _target_max_size();
}

void l1_reader_cache::maybe_evict_size() {
    if (!over_size_limit()) [[likely]] {
        return;
    }
    _readers.pop_front_and_dispose([this](entry* e) {
        // CORE-15812: a reusable reader dropped purely for space means the
        // cache filled with readers that were never reused (offset-mismatch
        // pileup -> A).
        ++_evicted_size;
        dispose_in_background(e);
    });
}

void l1_reader_cache::setup_metrics() {
    namespace sm = ss::metrics;
    if (config::shard_local_cfg().disable_public_metrics()) {
        return;
    }
    const auto group_name = prometheus_sanitize::metrics_name(
      "cloud_topics_l1_reader_cache");
    const std::vector<sm::label> aggregate_labels{sm::shard_label};
    _public_metrics.add_group(
      group_name,
      {
        sm::make_gauge(
          "cached_readers",
          [this] { return _readers.size(); },
          sm::description("Idle L1 readers currently held in the cache."))
          .aggregate(aggregate_labels),
        sm::make_gauge(
          "in_use_readers",
          [this] { return _in_use.size(); },
          sm::description("L1 readers currently checked out of the cache."))
          .aggregate(aggregate_labels),
        sm::make_counter(
          "hits",
          [this] { return _cache_hits; },
          sm::description("L1 reader cache hits (positioned reader reused)."))
          .aggregate(aggregate_labels),
        sm::make_counter(
          "misses",
          [this] { return _cache_misses; },
          sm::description("L1 reader cache misses (reader rebuilt)."))
          .aggregate(aggregate_labels),
        sm::make_counter(
          "readers_added",
          [this] { return _readers_added; },
          sm::description("L1 readers added to the cache."))
          .aggregate(aggregate_labels),
        sm::make_counter(
          "readers_evicted",
          [this] { return _readers_evicted; },
          sm::description(
            "L1 readers evicted from the cache (size limit or idle timeout)."))
          .aggregate(aggregate_labels),
        sm::make_counter(
          "offset_mismatch",
          [this] { return _offset_mismatch; },
          sm::description(
            "Cache misses where a reader for the partition was "
            "cached at a different offset (over-read)."))
          .aggregate(aggregate_labels),
        sm::make_counter(
          "evicted_eos",
          [this] { return _evicted_eos; },
          sm::description(
            "Readers disposed on return because they reached "
            "end-of-stream (read to the frontier/end)."))
          .aggregate(aggregate_labels),
        sm::make_counter(
          "evicted_not_reusable",
          [this] { return _evicted_not_reusable; },
          sm::description(
            "Readers disposed on return non-reusable for a "
            "non-EOS reason (abort/error)."))
          .aggregate(aggregate_labels),
        sm::make_counter(
          "evicted_size",
          [this] { return _evicted_size; },
          sm::description(
            "Reusable readers dropped to stay under the size "
            "cap (offset-mismatch pileup)."))
          .aggregate(aggregate_labels),
      });
}

} // namespace cloud_topics
