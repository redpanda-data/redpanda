/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/materialized_resources.h"

#include "base/vlog.h"
#include "cloud_storage/logger.h"
#include "cloud_storage/materialized_manifest_cache.h"
#include "cloud_storage/remote_partition.h"
#include "cloud_storage/remote_segment.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "resource_mgmt/io_priority.h"
#include "resource_mgmt/memory_groups.h"
#include "ssx/future-util.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/temporary_buffer.hh>

#include <absl/container/btree_map.h>

#include <algorithm>
#include <chrono>
#include <exception>
#include <limits>

namespace cloud_storage {

using namespace std::chrono_literals;

static constexpr ss::lowres_clock::duration stm_jitter_duration = 10s;
static constexpr ss::lowres_clock::duration stm_max_idle_time = 60s;

materialized_resources::materialized_resources()
  : _stm_jitter(stm_jitter_duration)
  , _max_segment_readers_per_shard(
      config::shard_local_cfg()
        .cloud_storage_max_segment_readers_per_shard.bind())
  , _storage_read_buffer_size(
      config::shard_local_cfg().storage_read_buffer_size.bind())
  , _storage_read_readahead_count(
      config::shard_local_cfg().storage_read_readahead_count.bind())
  , _mem_units(
      memory_groups().tiered_storage_max_memory(),
      "cst_materialized_resources_memory")
  , _manifest_meta_size(
      config::shard_local_cfg().cloud_storage_manifest_cache_size.bind())
  , _manifest_cache(ss::make_shared<materialized_manifest_cache>(
      config::shard_local_cfg().cloud_storage_manifest_cache_size()))
  , _cache_carryover_bytes(config::shard_local_cfg()
                             .cloud_storage_cache_trim_carryover_bytes.bind()) {
    auto update_max_mem = [this]() {
        // Update memory capacity to accommodate new max number of segment
        // readers
        _mem_units.set_capacity(max_memory_utilization());
    };
    _max_segment_readers_per_shard.watch(update_max_mem);
    _storage_read_buffer_size.watch(update_max_mem);
    _storage_read_readahead_count.watch(update_max_mem);

    _manifest_meta_size.watch([this] {
        ssx::background = ss::with_gate(_gate, [this] {
            vlog(
              cst_log.info,
              "Manifest cache capacity will be changed from {} to {}",
              _manifest_cache->get_capacity(),
              _manifest_meta_size());
            return _manifest_cache->set_capacity(_manifest_meta_size());
        });
    });

    if (ss::this_shard_id() == 0) {
        // Take into account number of bytes used by cache carryover mechanism.
        // The cache doesn't have access to 'materialized_resources' because
        // otherwise it'd create a dependency cycle.
        _carryover_units = _mem_units.try_get_units(_cache_carryover_bytes());
        vlog(
          cst_log.info,
          "{} units reserved for cache trim carryover mechanism",
          _carryover_units.has_value() ? _carryover_units->count() : 0);

        _cache_carryover_bytes.watch([this] {
            // We're using best effort approach here. Under memory pressure we
            // might not be able to change reservation
            auto current_units = _carryover_units.has_value()
                                   ? _carryover_units->count()
                                   : 0;
            auto upd = _cache_carryover_bytes();
            if (upd < current_units) {
                // Free units that represent memory used by carryover cache
                // trim. It's guaranteed that optional is not null.
                _carryover_units->return_units(current_units - upd);
            } else {
                // Acquire new units
                auto tmp = _mem_units.try_get_units(upd - current_units);
                if (tmp.has_value()) {
                    if (_carryover_units.has_value()) {
                        _carryover_units->adopt(std::move(tmp.value()));
                    } else {
                        _carryover_units = std::move(tmp.value());
                    }
                } else {
                    vlog(
                      cst_log.info,
                      "Failed to reserve {} units for the cache carryover "
                      "mechanism because tiered-storage is likely under memory "
                      "pressure",
                      upd - current_units);
                }
            }
            vlog(
              cst_log.info,
              "{} units reserved for cache trim carryover mechanism",
              _carryover_units.has_value() ? _carryover_units->count() : 0);
        });
    }
}

ts_read_path_probe& materialized_resources::get_read_path_probe() {
    return _read_path_probe;
}

ss::future<> materialized_resources::stop() {
    cst_log.debug("Stopping materialized_segments...");

    co_await _manifest_cache->stop();

    _stm_timer.cancel();

    _mem_units.broken();

    co_await _gate.close();
    cst_log.debug("Stopped materialized_segments...");
}

ss::future<> materialized_resources::start() {
    // Timer to invoke TTL eviction of segments
    _stm_timer.set_callback([this] {
        trim_segments(std::nullopt);
        _stm_timer.rearm(_stm_jitter());
    });
    _stm_timer.rearm(_stm_jitter());

    co_await _manifest_cache->start();

    co_return;
}

inline ssize_t projected_remote_segment_reader_memory_usage() {
    // We can't measure memory consumption by the buffer directly but
    // we can estimate it by using readahead count and read buffer size.
    // ss::input_stream doesn't consume that much memory all the time, only
    // if it's actively used.
    return static_cast<ssize_t>(
      config::shard_local_cfg().storage_read_buffer_size()
      * config::shard_local_cfg().storage_read_readahead_count());
}

inline ssize_t projected_remote_partition_reader_memory_usage() {
    // This value is just an estimate of a real thing. The reader contains
    // a bunch of fields and offset translation state which is variable size.
    // We can't really know in advance how big it is (usually it's not big).
    static size_t sz = remote_partition::reader_mem_use_estimate();
    return static_cast<ssize_t>(sz);
}

inline ssize_t projected_remote_segment_memory_usage() {
    // This is an estimate. When the reader is created it's size should be
    // checked and if it's larger units should be acquired. If it's smaller
    // some units should be released.
    static constexpr size_t segment_index_projected_size = 0x1000;
    return sizeof(remote_segment) + segment_index_projected_size;
}

materialized_manifest_cache&
materialized_resources::get_materialized_manifest_cache() {
    return *_manifest_cache;
}

size_t materialized_resources::max_memory_utilization() const {
    auto max_readers
      = config::shard_local_cfg().cloud_storage_max_segment_readers_per_shard();
    if (max_readers.has_value()) {
        auto max_mem = projected_remote_segment_reader_memory_usage()
                       * max_readers.value();
        return std::min(
          memory_groups().tiered_storage_max_memory(),
          static_cast<size_t>(max_mem));
    }
    return memory_groups().tiered_storage_max_memory();
}

size_t materialized_resources::current_segment_readers() const {
    size_t n = 0;
    for (const auto& s : _materialized) {
        n += s.readers.size();
    }
    return n;
}

size_t materialized_resources::current_segments() const {
    return _materialized.size();
}

void materialized_resources::register_segment(materialized_segment_state& s) {
    // We can only estimate the actual memory usage after the object is
    // created. We're using projected memory useage to actually acquire the
    // units. To fix this we need to check the acquired units and adjust
    // them. If the units overshoot we need to return some. Otherwise we
    // need to adopt extra units. When this happens the semaphore counter
    // may go negative. This means that we used more memory than needed.
    // In this case we need to mark the segment using 'overcommit' flag
    // so next eviction will remove it.
    auto estimate = s.segment->estimate_memory_use();
    auto units = s._units.count();
    if (units > estimate) {
        vlog(
          cst_log.debug,
          "Returning extra units. Current: {}, extimate: {}",
          units,
          estimate);
        s._units.return_units(units - estimate);
    } else {
        vlog(
          cst_log.debug,
          "Adopting extra units. Current: {}, extimate: {}",
          units,
          estimate);
        auto tr = _mem_units.take(estimate - units);
        s._units.adopt(std::move(tr.units));
        if (tr.checkpoint_hint) {
            // We hit the memory limit and the semaphore counter
            // went below zero. We need to mark this segment as a
            // candidate for eviction.
            s.overcommit = true;
            vlog(
              cst_log.info,
              "{} Overcommit detected, segment overshoot by {}",
              s.segment->get_ntp(),
              estimate - units);
        }
    }
    _materialized.push_back(s);
}

namespace {

ss::future<ssx::semaphore_units> get_units_abortable(
  adjustable_semaphore& sem, ssize_t units, storage::opt_abort_source_t as) {
    return as.has_value() ? sem.get_units(units, as.value())
                          : sem.get_units(units);
}

} // namespace

ss::future<segment_reader_units>
materialized_resources::get_segment_reader_units(
  storage::opt_abort_source_t as) {
    // Estimate segment reader memory requirements
    auto size_bytes = projected_remote_segment_reader_memory_usage();
    if (_mem_units.available_units() <= size_bytes) {
        // Update metrics counter if we are trying to acquire units while
        // saturated
        _segment_readers_delayed += 1;

        trim_segment_readers(max_memory_utilization() / 2);
    }

    auto semaphore_units = co_await get_units_abortable(
      _mem_units, size_bytes, as);
    co_return segment_reader_units{std::move(semaphore_units)};
}

ss::future<ssx::semaphore_units>
materialized_resources::get_partition_reader_units(
  storage::opt_abort_source_t as) {
    auto sz = projected_remote_partition_reader_memory_usage();
    if (_mem_units.available_units() <= sz) {
        // Update metrics counter if we are trying to acquire units while
        // saturated
        _partition_readers_delayed += 1;
    }

    return get_units_abortable(_mem_units, sz, as);
}

ss::future<segment_units>
materialized_resources::get_segment_units(storage::opt_abort_source_t as) {
    auto sz = projected_remote_segment_memory_usage();
    if (_mem_units.available_units() <= sz) {
        // Update metrics counter if we are trying to acquire units while
        // saturated
        _segments_delayed += 1;

        trim_segments(max_memory_utilization() / 2);
    }
    auto semaphore_units = co_await get_units_abortable(_mem_units, sz, as);
    co_return segment_units{std::move(semaphore_units)};
}

void materialized_resources::trim_segment_readers(size_t target_free) {
    vlog(
      cst_log.debug,
      "Trimming segment readers until {} bytes are free (current {})",
      target_free,
      _mem_units.available_units());

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
         i != candidates.rend() && _mem_units.current() < target_free;
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
        while (!st.readers.empty() && _mem_units.current() < target_free) {
            auto partition = st.parent;
            // TODO: consider asserting here instead: it's a bug for
            // 'partition' to be null, since readers outlive the partition, and
            // we can't create new readers if the partition has been shut down.
            if (likely(partition)) {
                partition->evict_segment_reader(std::move(st.readers.front()));
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
void materialized_resources::trim_segments(std::optional<size_t> target_free) {
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
        if (target_free && _mem_units.current() >= target_free) {
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
void materialized_resources::maybe_trim_segment(
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
            to_offload.emplace_back(st.parent.get(), st.base_rp_offset());
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
                partition->evict_segment_reader(std::move(st.readers.front()));
            }
            st.readers.pop_front();
        }
    }
}

} // namespace cloud_storage
