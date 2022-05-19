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

#include "config/property.h"
#include "units.h"

#include <seastar/core/semaphore.hh>

#include <cstdint>

namespace storage {

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
    storage_resources(const storage_resources&) = delete;

    /**
     * Call this when the storage::node_api state is updated
     */
    void update_allowance(uint64_t total, uint64_t free);

    /**
     * Call this when topics_table gets updated
     */
    void update_partition_count(size_t partition_count) {
        _partition_count = partition_count;
        _falloc_step_dirty = true;
    }

    uint64_t get_space_allowance() { return _space_allowance; }

    size_t get_falloc_step(std::optional<uint64_t>);
    size_t calc_falloc_step();

    /**
     * When the offset translator adds some dirty bytes, it gets semaphore
     * units and a hint as to whether it should proactively checkpoint.
     */
    struct take_result {
        ss::semaphore_units<> units;
        bool checkpoint_hint{false};
    };

    take_result offset_translator_take_bytes(int32_t bytes);

    take_result configuration_manager_take_bytes(size_t bytes);

    take_result consensus_stm_take_bytes(size_t bytes);

    ss::future<ss::semaphore_units<>> get_recovery_units() {
        return ss::get_units(_inflight_recovery, 1);
    }

    ss::future<ss::semaphore_units<>> get_close_flush_units() {
        return ss::get_units(_inflight_close_flush, 1);
    }

private:
    uint64_t _space_allowance{9};
    uint64_t _space_allowance_free{0};

    size_t _partition_count{9};
    config::binding<size_t> _segment_fallocation_step;
    size_t _append_chunk_size;

    size_t _falloc_step{0};
    bool _falloc_step_dirty{false};

    // These 'dirty_bytes' semaphores control how many bytes
    // may be written to logs in between checkpoints/snapshots, in
    // order to limit the quantity of data that must be replayed after
    // a restart.

    // How many bytes may all logs on this shard advance before
    // the offset translator must checkpoint to the kvstore?
    ss::semaphore _offset_translator_dirty_bytes{0};

    // How many bytes may logs write between checkpoints of the
    // configuration_manager?
    ss::semaphore _configuration_manager_dirty_bytes{0};

    // How many bytes may all consensus instances write before
    // we ask them to start snapshotting their state machines?
    ss::semaphore _consensus_stm_dirty_bytes{0};

    // How many logs may be recovered (via log_manager::manage)
    // concurrently?
    ss::semaphore _inflight_recovery{0};

    // How many logs may be flushed during segment close concurrently?
    // (e.g. when we shut down and ask everyone to flush)
    ss::semaphore _inflight_close_flush{0};
};

} // namespace storage