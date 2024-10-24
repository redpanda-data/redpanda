/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "storage_resources.h"

#include "base/seastarx.h"
#include "base/vlog.h"
#include "config/configuration.h"
#include "storage/chunk_cache.h"
#include "storage/logger.h"

#include <seastar/core/smp.hh>

namespace {
uint64_t per_shard_target_replay_bytes(uint64_t global_target_replay_bytes) {
    return global_target_replay_bytes / ss::smp::count;
}
} // namespace

namespace storage {

storage_resources::storage_resources(
  config::binding<size_t> falloc_step,
  config::binding<uint64_t> target_replay_bytes,
  config::binding<uint64_t> max_concurrent_replay,
  config::binding<uint64_t> compaction_index_memory)
  : _segment_fallocation_step(falloc_step)
  , _global_target_replay_bytes(target_replay_bytes)
  , _max_concurrent_replay(max_concurrent_replay)
  , _compaction_index_mem_limit(compaction_index_memory)
  , _append_chunk_size(internal::chunks().chunk_size())
  , _offset_translator_dirty_bytes(
      _global_target_replay_bytes() / ss::smp::count)
  , _configuration_manager_dirty_bytes(
      _global_target_replay_bytes() / ss::smp::count)
  , _stm_dirty_bytes(_global_target_replay_bytes() / ss::smp::count)
  , _compaction_index_bytes(_compaction_index_mem_limit())
  , _inflight_recovery(
      std::max(_max_concurrent_replay() / ss::smp::count, uint64_t{1}))
  , _inflight_close_flush(
      std::max(_max_concurrent_replay() / ss::smp::count, uint64_t{1})) {
    // Register notifications on configuration changes
    _global_target_replay_bytes.watch([this]() {
        auto v = per_shard_target_replay_bytes(_global_target_replay_bytes());

        _offset_translator_dirty_bytes.set_capacity(v);
        _stm_dirty_bytes.set_capacity(v);
        _configuration_manager_dirty_bytes.set_capacity(v);
        update_min_checkpoint_bytes();
    });

    _max_concurrent_replay.watch([this]() {
        auto v = _max_concurrent_replay() / ss::smp::count;

        // Guard against case where core count is higher than
        // total concurrent replay count.
        v = std::max(v, uint64_t{1});

        _inflight_recovery.set_capacity(v);
        _inflight_close_flush.set_capacity(v);
    });

    _compaction_index_mem_limit.watch([this] {
        _compaction_index_bytes.set_capacity(_compaction_index_mem_limit());
    });
}

// Unit test convenience for tests that want to control the falloc step
// but otherwise do not want to override anything.
storage_resources::storage_resources(config::binding<size_t> falloc_step)
  : storage_resources(
      std::move(falloc_step),
      config::shard_local_cfg().storage_target_replay_bytes.bind(),
      config::shard_local_cfg().storage_max_concurrent_replay.bind(),
      config::shard_local_cfg().storage_compaction_index_memory.bind()) {}

storage_resources::storage_resources()
  : storage_resources(
      config::shard_local_cfg().segment_fallocation_step.bind(),
      config::shard_local_cfg().storage_target_replay_bytes.bind(),
      config::shard_local_cfg().storage_max_concurrent_replay.bind(),
      config::shard_local_cfg().storage_compaction_index_memory.bind()) {}

void storage_resources::update_allowance(uint64_t total, uint64_t free) {
    // TODO: also take as an input the disk consumption of the SI cache:
    // it knows this because it calculates it when doing periodic trimming.
    if (
      config::shard_local_cfg().cloud_storage_enabled
      && total > config::shard_local_cfg().cloud_storage_cache_size()) {
        total -= config::shard_local_cfg().cloud_storage_cache_size();
    }

    _space_allowance = total;
    _space_allowance_free = std::min(free, total);

    _falloc_step = calc_falloc_step();
}

void storage_resources::update_min_checkpoint_bytes() {
    if (_partition_count) {
        // Limit the checkpoint frequency to 2x what it would be under
        // uniform traffic to all partitions. This permits us to overshoot
        // target_replay_bytes by approx 50% under non-uniform writes.
        _min_checkpoint_bytes = per_shard_target_replay_bytes(
                                  _global_target_replay_bytes())
                                / (_partition_count * 2);
    } else {
        _min_checkpoint_bytes = 0;
    }
}

void storage_resources::update_partition_count(size_t partition_count) {
    _partition_count = partition_count;
    _falloc_step_dirty = true;
    update_min_checkpoint_bytes();
}

size_t storage_resources::calc_falloc_step() {
    // Heuristic: use at most half the available disk space for per-allocating
    // space to write into.

    // At most, use the configured fallocation step.
    size_t step = _segment_fallocation_step();

    if (_partition_count == 0) {
        // Called before log_manager, this is an internal kvstore, give it a
        // full falloc step.
        return step;
    }

    // Initial disk stats read happens very early in startup, we should
    // never be called before that.
    vassert(_space_allowance > 0, "Called before disk stats init");

    // Pessimistic assumption that each shard may use _at most_ the
    // disk space divided by the shard count.  If allocation of partitions
    // is uneven, this may lead to us underestimasting how much space
    // is available, which is safe.

    uint64_t space_free_this_shard = _space_allowance_free / ss::smp::count;

    // Only use up to half the available space for fallocs.
    uint64_t space_per_partition = (space_free_this_shard / 2)
                                   / (_partition_count);

    step = std::min(space_per_partition, step);

    // Round down to nearest append chunk size
    auto remainder = step % _append_chunk_size;
    step = step - remainder;

    // At the minimum, falloc one chunk's worth of space.
    if (step < min_falloc_step) {
        // If we have less than the minimum step, don't both falloc'ing at all.
        step = _append_chunk_size;
    }

    vlog(
      rslog.debug,
      "calc_falloc_step: step {} (max {})",
      step,
      _segment_fallocation_step());
    return step;
}

size_t
storage_resources::get_falloc_step(std::optional<uint64_t> segment_size_hint) {
    if (_falloc_step_dirty) {
        _falloc_step = calc_falloc_step();
        _falloc_step_dirty = false;
    }

    auto step = _falloc_step;

    if (step == 0) {
        // Disk stats not initialized, give them the full sized step.
        step = _segment_fallocation_step();
    }

    if (segment_size_hint) {
        // Don't falloc more than the segment size, plus a little extra because
        // we don't roll segments until they overshoot the size.
        step = std::min(step, segment_size_hint.value() + _append_chunk_size);
    }
    return step;
}

bool storage_resources::offset_translator_take_bytes(
  int32_t bytes, ssx::semaphore_units& units) {
    vlog(
      rslog.trace,
      "offset_translator_take_bytes {} += {} (current {})",
      units.count(),
      bytes,
      _offset_translator_dirty_bytes.available_units());

    return filter_checkpoints(
      _offset_translator_dirty_bytes.take(bytes), units);
}

bool storage_resources::configuration_manager_take_bytes(
  size_t bytes, ssx::semaphore_units& units) {
    vlog(
      rslog.trace,
      "configuration_manager_take_bytes {} += {} (current {})",
      units.count(),
      bytes,
      _configuration_manager_dirty_bytes.available_units());

    return filter_checkpoints(
      _configuration_manager_dirty_bytes.take(bytes), units);
}

bool storage_resources::stm_take_bytes(
  size_t bytes, ssx::semaphore_units& units) {
    vlog(
      rslog.trace,
      "stm_take_bytes {} += {} (current {})",
      units.count(),
      bytes,
      _stm_dirty_bytes.available_units());

    return filter_checkpoints(_stm_dirty_bytes.take(bytes), units);
}

bool storage_resources::filter_checkpoints(
  adjustable_semaphore::take_result&& tr, ssx::semaphore_units& units) {
    // Adopt units from the take_result into the caller's unit store
    if (units.count()) {
        units.adopt(std::move(tr.units));
    } else {
        units = std::move(tr.units);
    }

    return tr.checkpoint_hint && (units.count() > _min_checkpoint_bytes);
}

adjustable_semaphore::take_result
storage_resources::compaction_index_take_bytes(size_t bytes) {
    vlog(
      rslog.trace,
      "compaction_index_take_bytes {} (current {})",
      bytes,
      _compaction_index_bytes.current());

    return _compaction_index_bytes.take(bytes);
}

} // namespace storage
