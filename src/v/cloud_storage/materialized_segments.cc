/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/materialized_segments.h"

#include "cloud_storage/logger.h"
#include "cloud_storage/remote_partition.h"
#include "cloud_storage/remote_segment.h"
#include "config/configuration.h"
#include "resource_mgmt/available_memory.h"
#include "ssx/future-util.h"
#include "vlog.h"

#include <seastar/core/memory.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/with_scheduling_group.hh>

#include <absl/container/btree_map.h>

#include <chrono>
#include <variant>

namespace cloud_storage {

using namespace std::chrono_literals;

static constexpr ss::lowres_clock::duration stm_jitter_duration = 10s;
static constexpr ss::lowres_clock::duration stm_max_idle_time = 60s;
static constexpr size_t min_reclaim_memory_request = 512_KiB;
static constexpr float eviction_background_priority = 100;
static constexpr float eviction_foreground_priority = 1000;

// If no reader limit is set, permit the per-shard partition count limit,
// multiplied by this factor (i.e. each partition gets this many readers
// on average).
static constexpr uint32_t default_reader_factor = 1;

// If no materialized segment limit is set, permit the per-shard partition count
// limit, multiplied by this factor (i.e. each partition gets this many segments
// on average).
static constexpr uint32_t default_segment_factor = 2;

materialized_segments::materialized_segments(ss::scheduling_group sg)
  : _stm_jitter(stm_jitter_duration)
  , _max_partitions_per_shard(
      config::shard_local_cfg().topic_partitions_per_shard.bind())
  , _max_readers_per_shard(
      config::shard_local_cfg().cloud_storage_max_readers_per_shard.bind())
  , _max_segments_per_shard(
      config::shard_local_cfg()
        .cloud_storage_max_materialized_segments_per_shard.bind())
  , _reader_units(max_readers(), "cst_reader")
  , _segment_units(max_segments(), "cst_segment")
  , _reclaimer(
      [this](ss::memory::reclaimer::request r) { return reclaim(r); },
      ss::memory::reclaimer_scope::async)
  , _eviction_sg(sg) {
    _max_readers_per_shard.watch(
      [this]() { _reader_units.set_capacity(max_readers()); });
    _max_segments_per_shard.watch(
      [this]() { _segment_units.set_capacity(max_segments()); });
    _max_partitions_per_shard.watch(
      [this]() { _reader_units.set_capacity(max_readers()); });
}

ss::memory::reclaiming_result
materialized_segments::reclaim(ss::memory::reclaimer::request req) {
    vlog(
      cst_log.info,
      "Memory reclamer request. {} bytes requested.",
      req.bytes_to_reclaim);
    try {
        auto am = resources::available_memory::local().available();
        if (am > 64_MiB) {
            return ss::memory::reclaiming_result::reclaimed_nothing;
        }
        auto to_reclaim = std::max(
          req.bytes_to_reclaim, min_reclaim_memory_request);

        auto before = reclaimable_memory();

        // Estimate number of segments/readers that should be evicted
        if (before.num_segments) {
            auto segments_to_reclaim
              = to_reclaim
                / (before.segments_memory_use_estimate / before.num_segments);

            vlog(
              cst_log.info,
              "Going to reclaim {} segments to free up to {} bytes",
              segments_to_reclaim,
              to_reclaim);

            trim_segments(segments_to_reclaim);
        }

        if (before.num_readers) {
            auto readers_to_reclaim
              = to_reclaim
                / (before.readers_memory_use_estimate / before.num_readers);

            vlog(
              cst_log.info,
              "Going to reclaim readers to free up to {} bytes",
              readers_to_reclaim,
              to_reclaim);

            trim_readers(readers_to_reclaim);
        }

        if (_eviction_pending.size() > 0) {
            _eviction_sg.set_shares(eviction_foreground_priority);
            return ss::memory::reclaiming_result::reclaimed_something;
        }
    } catch (const std::bad_alloc&) {
        // The method can be called in low memory state. Since it allocates
        // memory we can expect some bad_alloc exceptions to be thrown.
    }
    return ss::memory::reclaiming_result::reclaimed_nothing;
}

ss::future<> materialized_segments::stop() {
    _stm_timer.cancel();
    _cvar.broken();

    co_await _gate.close();

    // Do the last pass over the eviction list to stop remaining items returned
    // from readers after the eviction loop stopped.
    for (auto& rs : _eviction_pending) {
        co_await std::visit(
          [](auto&& rs) {
              if (!rs->is_stopped()) {
                  return rs->stop();
              } else {
                  return ss::make_ready_future<>();
              }
          },
          rs);
    }
}
ss::future<> materialized_segments::start() {
    // Fiber that consumes from _eviction_list and calls stop
    // on items before destroying them
    ssx::spawn_with_gate(_gate, [this] {
        return ss::with_scheduling_group(
          _eviction_sg, [this] { return run_eviction_loop(); });
    });

    // Timer to invoke TTL eviction of segments
    _stm_timer.set_callback([this] {
        trim_segments(std::nullopt);
        _stm_timer.rearm(_stm_jitter());
    });
    _stm_timer.rearm(_stm_jitter());

    co_return;
}

size_t materialized_segments::max_readers() const {
    return static_cast<size_t>(_max_readers_per_shard().value_or(
      _max_partitions_per_shard() * default_reader_factor));
}

size_t materialized_segments::max_segments() const {
    return static_cast<size_t>(_max_segments_per_shard().value_or(
      _max_partitions_per_shard() * default_segment_factor));
}

size_t materialized_segments::current_readers() const {
    return _reader_units.outstanding();
}

size_t materialized_segments::current_segments() const {
    return _segment_units.outstanding();
}

void materialized_segments::evict_reader(
  std::unique_ptr<remote_segment_batch_reader> reader) {
    _eviction_pending.push_back(std::move(reader));
    _cvar.signal();
}
void materialized_segments::evict_segment(
  ss::lw_shared_ptr<remote_segment> segment) {
    _eviction_pending.push_back(std::move(segment));
    _cvar.signal();
}

static size_t reader_memory_use_upper_bound() {
    // We can't measure memory consumption by the buffer directly but
    // we can estimate it by using readahead count and read buffer size.
    // ss::input_stream doesn't consume that much memory all the time, only
    // if it's actively used.
    return config::shard_local_cfg().storage_read_buffer_size()
           + config::shard_local_cfg().storage_read_readahead_count();
}

materialized_segments::reclaimable_memory_t
materialized_segments::reclaimable_memory() const {
    reclaimable_memory_t result{};
    const auto reader_size_bytes = reader_memory_use_upper_bound();

    for (const auto& es : _materialized) {
        result.segments_memory_use_estimate += es.reclaimable_memory();
        result.num_segments += 1;
    }
    result.readers_memory_use_estimate += reader_size_bytes * current_readers();
    result.num_readers = current_readers();
    return result;
}

ss::future<> materialized_segments::flush_evicted() {
    if (_eviction_pending.empty() && _eviction_in_flight.empty()) {
        // Fast path, avoid waking up the eviction loop if there is no work.
        co_return;
    }

    auto barrier = ss::make_lw_shared<eviction_barrier>();

    // Write a barrier to the list and wait for the eviction consumer
    // to reach it: this
    _eviction_pending.push_back(barrier);
    _cvar.signal();

    co_await barrier->promise.get_future();
}

ss::future<> materialized_segments::run_eviction_loop() {
    // Evict readers asynchronously
    while (true) {
        co_await _cvar.wait([this] { return !_eviction_pending.empty(); });
        _eviction_in_flight = std::exchange(_eviction_pending, {});
        while (!_eviction_in_flight.empty()) {
            co_await std::visit(
              [](auto&& rs) { return rs->stop(); },
              _eviction_in_flight.front());
            _eviction_in_flight.pop_front();
        }
        if (_eviction_pending.empty()) {
            // If there was a low memory situation the priority of the
            // eviction loop might have been bumped. We need to decrease
            // it back to normal when all evicted entities are processed.
            _eviction_sg.set_shares(eviction_background_priority);
        }
    }
}

void materialized_segments::register_segment(materialized_segment_state& s) {
    _materialized.push_back(s);
}

ssx::semaphore_units materialized_segments::get_reader_units() {
    if (_reader_units.available_units() <= 0) {
        trim_readers(max_readers() / 2);
    }

    // TOOD: make this function async so that it can wait until we succeed
    // in evicting some readers: trim_readers is not
    // guaranteed to do this, if all readers are in use.

    return _reader_units.take(1).units;
}

ssx::semaphore_units materialized_segments::get_segment_units() {
    if (_segment_units.available_units() <= 0) {
        trim_segments(max_segments() / 2);
    }

    vlog(
      cst_log.debug,
      "get_segment_units: taking 1 from {}",
      _segment_units.available_units());

    return _segment_units.take(1).units;
}

void materialized_segments::trim_readers(size_t target_free) {
    vlog(
      cst_log.debug,
      "Trimming readers until {} reader slots are free (current {})",
      target_free,
      _reader_units.available_units());

    // We sort segments by their reader count before culling, to avoid unfairly
    // targeting whichever segments happen to be first in the list.
    // Sorting by atime doesn't work well, because atime is a per-segment
    // level, not a per-reader level: a segment can have a very recent
    // atime, but be sitting on a large population of inactive segment readers.
    //
    // A sorted btree is used instead of a vector+sort, because the contiguous
    // vector could generate oversized allocations for large numbers of
    // hydrated segments (a 128kb buffer is only enough for 16k pointers)
    absl::
      btree_multimap<size_t, std::reference_wrapper<materialized_segment_state>>
        candidates;
    for (auto& st : _materialized) {
        if (st.segment->download_in_progress()) {
            continue;
        }

        if (st.readers.empty()) {
            continue;
        }

        vlog(
          cst_log.debug,
          "trim_readers: {} {} has {} readers",
          st.ntp(),
          st.base_rp_offset(),
          st.readers.size());

        candidates.insert(std::make_pair(st.readers.size(), std::ref(st)));
    }

    for (auto i = candidates.rbegin();
         i != candidates.rend() && _reader_units.current() < target_free;
         ++i) {
        auto& st = i->second.get();

        vlog(
          cst_log.debug,
          "Trimming readers from segment {} offset {} ({} refs, {} readers)",
          st.ntp(),
          st.base_rp_offset(),
          st.segment.use_count(),
          st.readers.size());

        // Readers hold a reference to the segment, so for the
        // segment.owned() check to pass, we need to clear them out.
        while (!st.readers.empty() && _reader_units.current() < target_free) {
            evict_reader(std::move(st.readers.front()));
            st.readers.pop_front();
        }
    }
}

/**
 * TTL based demotion of materialized_segment_state objects and background
 * eviction of the underlying segment and reader objects.
 *
 * This method does not guarantee to free any resources: it will not do
 * anything if no segments have an atime older than the TTL.  Ssee trim_readers
 * for how to trim the reader population back to a specific size
 *
 * NOTE: This method must never be made async or yield while iterating over
 * segments. If it does yield and remote_partition::stop runs, remote_partition
 * can clear out the segments map, and a subsequent offload of that segment will
 * cause a vassert failure.
 *
 * @param target_free: if set, the trim will remove segments until the
 *        number of units available in segments_semaphore reaches the
 *        target, or until it runs out of candidates for eviction.  This
 *        is not a guarantee that units will be freed (segments may be
 *        pinned by active readers or ongoing downloads).  In this mode,
 *        TTL is not relevant.
 *
 */
void materialized_segments::trim_segments(std::optional<size_t> target_free) {
    vlog(
      cst_log.debug,
      "collecting stale materialized segments, {} segments materialized",
      _materialized.size());

    auto now = ss::lowres_clock::now();

    // The pointers in offload_list_t are safe because there are no scheduling
    // points between here and the ultimate eviction at end of function.
    offload_list_t to_offload;
    for (auto& st : _materialized) {
        auto deadline = st.atime + stm_max_idle_time;

        // We freed enough, drop out of iterating over segments
        if (target_free && _segment_units.current() >= target_free) {
            break;
        }

        if (target_free || (now >= deadline)) {
            maybe_trim_segment(st, to_offload);
        }
    }

    vlog(cst_log.debug, "found {} eviction candidates ", to_offload.size());
    for (auto [p, offset] : to_offload) {
        // Should not happen because we handled the case of parent==null
        // above, before inserting into to_offload
        vassert(p, "Unexpected orphan segment!");

        p->offload_segment(offset);
    }
}

/**
 * Inner part of trim_segments, that decides whether a segment can be
 * trimmed, and if so pushes to the to_offload list for the caller
 * to later call offload_segment on the list's contents.
 */
void materialized_segments::maybe_trim_segment(
  materialized_segment_state& st, offload_list_t& to_offload) {
    if (st.segment->is_stopped()) {
        return;
    }

    if (st.segment->download_in_progress()) {
        return;
    }

    if (st.segment.owned()) {
        // This segment is not referred to by any readers, we may
        // enqueue it for eviction.
        vlog(
          cst_log.debug,
          "Materialized segment {} offset {} is stale",
          st.ntp(),
          st.base_rp_offset());
        // this will delete and unlink the object from
        // _materialized collection
        if (st.parent) {
            to_offload.push_back(
              std::make_pair(st.parent.get(), st.base_rp_offset()));
        } else {
            // This cannot happen, because materialized_segment_state
            // is only instantiated by remote_partition and will
            // be disposed before the remote_partition it points to.
            vassert(
              false,
              "materialized_segment_state outlived remote_partition (offset "
              "{})",
              st.base_rp_offset());
        }
    } else {
        // We would like to trim this segment, but cannot right now
        // because it has some readers referring to it.  Enqueue these
        // readers for eviction, in the expectation that on the next
        // periodic pass through this function, the segment will be
        // eligible for eviction, if it does not create any new readers
        // in the meantime.
        vlog(
          cst_log.debug,
          "Materialized segment {} base-offset {} is not stale: {} "
          "readers={}",
          st.ntp(),
          st.base_rp_offset(),
          st.segment.use_count(),
          st.readers.size());

        // Readers hold a reference to the segment, so for the
        // segment.owned() check to pass, we need to clear them out.
        while (!st.readers.empty()) {
            evict_reader(std::move(st.readers.front()));
            st.readers.pop_front();
        }
    }
}

} // namespace cloud_storage
