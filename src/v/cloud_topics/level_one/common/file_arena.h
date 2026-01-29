/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/file_arena_probe.h"
#include "config/property.h"
#include "ssx/semaphore.h"

#include <seastar/core/sharded.hh>

struct FileArenaTestFixture;

namespace cloud_topics::l1 {

// A thin wrapper around a staging file which also holds on to some semaphore
// units. These units are obtained from a `file_arena` and are held until
// `finalize()` & `remove()` are called, or the `staging_file_with_reservation`
// goes out of scope. `staging_file_with_reservation`s must not outlive the
// `file_arena` from which they are created.
class staging_file_with_reservation {
public:
    staging_file_with_reservation(
      std::unique_ptr<staging>, ssx::semaphore_units);

    // Returns `_file->size()`.
    ss::future<size_t> size();
    // Returns `_file->output_stream()`.
    ss::future<ss::output_stream<char>> output_stream();

    // Intended to be called when `_file` is no longer going to be written to
    // (but is still in use/present on disk otherwise). This can help in
    // proactively freeing any unused units in `_disk_reservation`, if the size
    // of `_file` on disk if smaller than what was originally requested.
    void finalize(size_t);

    // Calls `_file->remove()` and returns all remaining units in
    // `_disk_reservation`.
    ss::future<> remove();

private:
    std::unique_ptr<staging> _file;
    ssx::semaphore_units _disk_reservation;
};

class file_arena_manager;

// A file arena which has a disk reservation allocated to it. Memory is
// managed both shard-locally and globally in a work-stealing fashion via the
// `file_arena_manager` (which exists only on shard 0).
// When memory is first requested on any shard, a cross-core retrieval of units
// from the global pool is made and added to the available shard-local
// resources. A soft limit (which is some percentage
// of the overall reservation) is used to trigger memory reclamation from the
// shard-local pools to return them to the global pool.
class file_arena : public ss::peering_sharded_service<file_arena> {
public:
    static constexpr ss::shard_id manager_shard = 0;

    file_arena(
      io*,
      config::binding<size_t>,
      config::binding<double>,
      config::binding<size_t>,
      std::string_view,
      ss::logger&);

    // Starts the `file_arena_manager`'s resource management.
    ss::future<> start();

    // Stops the `file_arena_manager`'s disk management.
    ss::future<> stop();

    // Waits for available resources via `reserve_units()` per the requested
    // number of bytes, and returns either a `staging_file_with_reservation`
    // with the units held or an `io::errc` detailing the error.
    ss::future<std::expected<staging_file_with_reservation, io::errc>>
    create_tmp_file(size_t, ss::abort_source&);

private:
    friend file_arena_manager;
    friend file_arena_probe;
    friend ::FileArenaTestFixture;

    // Tries to obtain units from the shard-local resources. If not enough are
    // available per the requested file size, they are requested from the
    // global pool held in `file_arena_manager` on core0 (which will actively
    // try to reclaim memory from other shards until enough are available on
    // this shard).
    ss::future<ssx::semaphore_units> reserve_units(size_t, ss::abort_source&);

    // Releases all unused units available in `_local_disk_bytes_reservable` and
    // returns that count.
    size_t release_unused_disk_units();

    // Requests units from the global pool held in the `file_arena_manager`.
    ss::future<size_t> request_units_from_core0_manager();

private:
    // Generates our `staging_files` for us.
    io* _io;

    // The number of bytes available for reservation on this shard.
    ssx::semaphore _local_disk_bytes_reservable;

    ss::logger& _logger;

    // The manager of the file arena. Exists only on the shard0 instance
    // of this object. Responsible for providing access to the global pool of
    // memory allocated for this file arena, and managing memory across all
    // shards of the `file_arena`.
    std::unique_ptr<file_arena_manager> _core0_arena_manager{nullptr};

    std::unique_ptr<file_arena_probe> _probe{nullptr};
};

// A manager of the global and shard-local memory reservation in a `file_arena`.
// Owns the global pool of bytes from which users across all shards request
// units, and is responsible for actively reclaiming unused units when necessary
// across all shards.
class file_arena_manager {
public:
    file_arena_manager(
      config::binding<size_t>,
      config::binding<double>,
      config::binding<size_t>,
      std::string_view,
      ss::logger&);

    // Starts resource management for the provided `file_arena`.
    ss::future<> start(ss::sharded<file_arena>*);

    // Stops resource management.
    ss::future<> stop();

    // Gets units from the global pool. May trigger reclamation in case the soft
    // limit is exceeded.
    size_t get_units(ss::shard_id);

private:
    // Check if the total use (including outstanding reservations) on the global
    // memory pool has exceeded the soft limit.
    bool disk_space_soft_limit_exceeded() const;

    // A backgrounded task which calls `manage_disk_space()` when
    // `_disk_space_monitor_cv` is triggered, typically when the soft limit has
    // been exceeded.
    ss::future<> disk_space_monitor();

    // Attempts to reclaim memory from shard-local reservation pools for use in
    // the global memory pool.
    ss::future<> reclaim_disk_space();

    // Updates internal disk limits per changes in configuration parameters.
    void update_disk_limits();

private:
    friend file_arena_probe;
    friend ::FileArenaTestFixture;

    // The upper limit of reservable disk space. At most times equivalent to
    // `_scratch_space_size_bytes`- it is duplicated in order to make
    // required changes to the number of bytes accessible from
    // `_disk_bytes_reservable` when the configuration for
    // `_scratch_space_size_bytes` is altered.
    size_t _disk_bytes_reservable_total{0};

    // The soft limit of reservable disk space. When this limit is breached, the
    // `file_arena_manager` will begin to attempt to reclaim memory in
    // shard-local reservation pools for use in the global memory pool.
    size_t _disk_bytes_reservable_soft_limit{0};

    // The config binding for the amount of scratch space allocated for this
    // arena.
    config::binding<size_t> _scratch_space_size_bytes;

    // The config binding for the percentage of the total scratch space that
    // forms the soft limit.
    config::binding<double> _scratch_space_soft_limit_size_percent;

    // The expected number of units that will be returned by a call to
    // `get_units()`.
    config::binding<size_t> _block_size;

    // The total number of bytes currently available for reservation across
    // all shards.
    ssx::semaphore _disk_bytes_reservable;

    ss::logger& _logger;

    ss::sharded<file_arena>* _arena{nullptr};

    // Used to alert `disk_space_monitor()` that it should attempt to reclaim
    // memory for the global pool.
    ss::condition_variable _disk_space_monitor_cv;

    ss::gate _gate;
    ss::abort_source _as;
};

} // namespace cloud_topics::l1
