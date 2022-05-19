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

#include "config/configuration.h"
#include "storage/logger.h"
#include "vlog.h"

namespace storage {

// How many bytes are we willing to replay on the next start
// after a clean shutdown?
static constexpr uint64_t max_dirty_bytes_clean_shutdown = 128_MiB;

// Concurrently limit on startup recovery and shutdown flush
static constexpr uint64_t max_concurrent_startup_shutdown = 1024;

storage_resources::storage_resources(config::binding<size_t> falloc_step)
  : _segment_fallocation_step(falloc_step)
  , _append_chunk_size(config::shard_local_cfg().append_chunk_size())
  , _offset_translator_dirty_bytes(
      max_dirty_bytes_clean_shutdown / ss::smp::count)
  , _configuration_manager_dirty_bytes(
      max_dirty_bytes_clean_shutdown / ss::smp::count)
  , _consensus_stm_dirty_bytes(max_dirty_bytes_clean_shutdown / ss::smp::count)

  , _inflight_recovery(max_concurrent_startup_shutdown / ss::smp::count)
  , _inflight_close_flush(max_concurrent_startup_shutdown / ss::smp::count) {}

storage_resources::storage_resources()
  : storage_resources(
    config::shard_local_cfg().segment_fallocation_step.bind()) {}

void storage_resources::update_allowance(uint64_t total, uint64_t free) {
    // TODO: also take as an input the disk consumption of the SI cache:
    // it knows this because it calculates it when doing periodic trimming.
    if (
      config::shard_local_cfg().cloud_storage_enabled
      && total < config::shard_local_cfg().cloud_storage_cache_size()) {
        total -= config::shard_local_cfg().cloud_storage_cache_size();
    }

    _space_allowance = total;
    _space_allowance_free = std::min(free, total);

    _falloc_step = calc_falloc_step();
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
      stlog.debug,
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

storage_resources::take_result
storage_resources::offset_translator_take_bytes(int32_t bytes) {
    take_result result = {
      .units = ss::consume_units(_offset_translator_dirty_bytes, bytes),
      .checkpoint_hint = _offset_translator_dirty_bytes.current() <= 0};

    vlog(
      stlog.trace,
      "offset_translator_take_bytes {} (remaining {})",
      bytes,
      _offset_translator_dirty_bytes.current());

    return result;
}

storage_resources::take_result
storage_resources::configuration_manager_take_bytes(size_t bytes) {
    take_result result = {
      .units = ss::consume_units(_configuration_manager_dirty_bytes, bytes),
      .checkpoint_hint = _configuration_manager_dirty_bytes.current() <= 0};

    vlog(
      stlog.trace,
      "configuration_manager_take_bytes {} (remaining {})",
      bytes,
      _configuration_manager_dirty_bytes.current());

    return result;
}

storage_resources::take_result
storage_resources::consensus_stm_take_bytes(size_t bytes) {
    take_result result = {
      .units = ss::consume_units(_consensus_stm_dirty_bytes, bytes),
      .checkpoint_hint = _consensus_stm_dirty_bytes.current() <= 0};

    vlog(
      stlog.trace,
      "consensus_stm_take_bytes {} (remaining {})",
      bytes,
      _consensus_stm_dirty_bytes.current());

    return result;
}

} // namespace storage
