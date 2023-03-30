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
#include "ssx/future-util.h"
#include "vlog.h"

#include <seastar/core/loop.hh>

#include <absl/container/btree_map.h>

#include <chrono>

namespace cloud_storage {

using namespace std::chrono_literals;

static constexpr ss::lowres_clock::duration stm_jitter_duration = 10s;
static constexpr ss::lowres_clock::duration stm_max_idle_time = 60s;

// If no reader limit is set, permit the per-shard partition count limit,
// multiplied by this factor (i.e. each partition gets this many readers
// on average).
static constexpr uint32_t default_reader_factor = 1;

// If no materialized segment limit is set, permit the per-shard partition count
// limit, multiplied by this factor (i.e. each partition gets this many segments
// on average).
static constexpr uint32_t default_segment_factor = 2;

materialized_segments::materialized_segments()
  : _stm_jitter(stm_jitter_duration)
  , _max_partitions_per_shard(
      config::shard_local_cfg().topic_partitions_per_shard.bind())
  , _max_readers_per_shard(
      config::shard_local_cfg().cloud_storage_max_readers_per_shard.bind())
  , _max_segments_per_shard(
      config::shard_local_cfg()
        .cloud_storage_max_materialized_segments_per_shard.bind())
  , _reader_units(max_readers(), "cst_reader")
  , _segment_units(max_segments(), "cst_segment") {
    _max_readers_per_shard.watch(
      [this]() { _reader_units.set_capacity(max_readers()); });
    _max_segments_per_shard.watch(
      [this]() { _segment_units.set_capacity(max_segments()); });
    _max_partitions_per_shard.watch(
      [this]() { _reader_units.set_capacity(max_readers()); });
}

ss::future<> materialized_segments::stop() {
    cst_log.debug("Stopping materialized_segments...");
    _stm_timer.cancel();

    co_await _gate.close();
    cst_log.debug("Stopped materialized_segments...");
}
ss::future<> materialized_segments::start() {
    // Timer to invoke TTL eviction of segments
    _stm_timer.set_callback([this] {
        trim_segments(std::nullopt);
        _stm_timer.rearm(_stm_jitter());
    });
    _stm_timer.rearm(_stm_jitter());

    return ss::now();
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
            auto partition = st.parent;
            // TODO: consider asserting here instead: it's a bug for
            // 'partition' to be null, since readers outlive the partition, and
            // we can't create new readers if the partition has been shut down.
            if (likely(partition)) {
                partition->evict_reader(std::move(st.readers.front()));
            }
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
} // namespace cloud_storage

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
            // TODO: consider asserting here instead: it's a bug for
            // 'partition' to be null, since readers outlive the partition, and
            // we can't create new readers if the partition has been shut down.
            auto partition = st.parent;
            if (likely(partition)) {
                partition->evict_reader(std::move(st.readers.front()));
            }
            st.readers.pop_front();
        }
    }
}

} // namespace cloud_storage
