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

#pragma once

#include "base/units.h"
#include "config/property.h"
#include "ssx/semaphore.h"
#include "utils/adjustable_semaphore.h"

#include <cstdint>

namespace storage {

class node_api;

/**
 * This class is used by various storage components to control consumption
 * of shared system resources.  It broadly does this in two ways:
 * - Limiting concurrency of certain types of operation
 * - Controlling buffer sizes depending on available resources
 */
class storage_resources {
public:
    // If we don't have this much disk space available per partition,
    // don't both falloc'ing at all.
    static constexpr size_t min_falloc_step = 128_KiB;

    storage_resources();
    storage_resources(config::binding<size_t>);
    storage_resources(
      config::binding<size_t>,
      config::binding<uint64_t>,
      config::binding<uint64_t>,
      config::binding<uint64_t>);
    storage_resources(const storage_resources&) = delete;

    /**
     * Call this when the storage::node_api state is updated
     */
    void update_allowance(uint64_t total, uint64_t free);

    /**
     * Call this when topics_table gets updated
     */
    void update_partition_count(size_t partition_count);

    uint64_t get_space_allowance() { return _space_allowance; }

    size_t get_falloc_step(std::optional<uint64_t>);
    size_t calc_falloc_step();

    bool
    offset_translator_take_bytes(int32_t bytes, ssx::semaphore_units& units);

    bool
    configuration_manager_take_bytes(size_t bytes, ssx::semaphore_units& units);

    bool stm_take_bytes(size_t bytes, ssx::semaphore_units& units);

    adjustable_semaphore::take_result compaction_index_take_bytes(size_t bytes);
    bool compaction_index_bytes_available() {
        return _compaction_index_bytes.current() > 0;
    }

    ss::future<ssx::semaphore_units> get_recovery_units() {
        return _inflight_recovery.get_units(1);
    }

    ss::future<ssx::semaphore_units> get_close_flush_units() {
        return _inflight_close_flush.get_units(1);
    }

    ss::future<ssx::semaphore_units> get_compaction_compression_units() {
        return _inflight_compaction_compression.get_units(1);
    }

    /**
     * An adjustable_semaphore will set checkpoint_hint whenever its units
     * are exhausted, but this can happen with pathological frequency if
     * many units are hogged by partitions that have written a lot of
     * data then stopped.
     *
     * To mitigate this, filter out checkpoint hints if the partition
     * taking units is not occupying more than a threshold number of
     * units.  This means that instead of doing an avalanche of snapshots
     * under this unpleasant state, we will instead violate target_replay_bytes
     */
    bool filter_checkpoints(
      adjustable_semaphore::take_result&&, ssx::semaphore_units&);

    /**
     * Call this when the partition count or the target replay bytes changes
     */
    void update_min_checkpoint_bytes();

private:
    uint64_t _space_allowance{9};
    uint64_t _space_allowance_free{0};

    size_t _partition_count{9};
    config::binding<size_t> _segment_fallocation_step;
    config::binding<uint64_t> _global_target_replay_bytes;
    config::binding<uint64_t> _max_concurrent_replay;
    config::binding<uint64_t> _compaction_index_mem_limit;
    size_t _append_chunk_size;

    // A lower bound on how many units a caller must have to be
    // eligible for flush, to prevent pathological case where on caller
    // happens to repeatedly request units close to the parent semaphore
    // being exhausted
    // See https://github.com/redpanda-data/redpanda/issues/6854
    uint64_t _min_checkpoint_bytes{0};

    size_t _falloc_step{0};
    bool _falloc_step_dirty{false};

    // These 'dirty_bytes' semaphores control how many bytes
    // may be written to logs in between checkpoints/snapshots, in
    // order to limit the quantity of data that must be replayed after
    // a restart.

    // How many bytes may all logs on this shard advance before
    // the offset translator must checkpoint to the kvstore?
    adjustable_semaphore _offset_translator_dirty_bytes{0};

    // How many bytes may logs write between checkpoints of the
    // configuration_manager?
    adjustable_semaphore _configuration_manager_dirty_bytes{0};

    // How many bytes may all consensus instances write before
    // we ask them to start snapshotting their state machines?
    adjustable_semaphore _stm_dirty_bytes{0};

    // How much memory may all compacted partitions on this shard
    // use for their spill_key_index objects
    adjustable_semaphore _compaction_index_bytes{0};

    // How many logs may be recovered (via log_manager::manage)
    // concurrently?
    adjustable_semaphore _inflight_recovery{0};

    // How many logs may be flushed during segment close concurrently?
    // (e.g. when we shut down and ask everyone to flush)
    adjustable_semaphore _inflight_close_flush{0};

    // Decompressing batches for compaction indexing may have an outsized
    // memory footprint compared with the batch's original size, we must
    // limit how many of these we do in parallel.
    adjustable_semaphore _inflight_compaction_compression{1};
};

} // namespace storage
