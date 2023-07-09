/*
 * Copyright 2023 Redpanda Data, Inc.
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
#include "raft/types.h"
#include "seastarx.h"
#include "ssx/semaphore.h"
#include "storage/node.h"

#include <seastar/core/sharded.hh>

#include <iostream>

namespace cloud_storage {
class cache;
}

namespace cluster {
class partition_manager;
}

namespace storage {

class api;
class node;

class eviction_policy {
public:
    /*
     * tracks reclaimable data in a partition. the raft::group_id
     * is used to decouple the process of evaluating the policy from the
     * lifetime of any one partition. if a partition is removed during the
     * process, we will notice it missing when looking it up by its raft group
     * id and skip that particular partition.
     */
    struct partition {
        raft::group_id group;
        reclaimable_offsets offsets;

        /*
         * used when applying policies to the schedule.
         *
         * the offset at which the scheduling policy would like this partition
         * to be prefix truncated, along with the total amount of space
         * estimated to be reclaimed.
         */
        std::optional<model::offset> decision;
        size_t total{0};
    };

    /*
     * shard-tagged set of partitions. scheduling state is collected on core-0
     * before being analyzed to determine which decisions to broadcast back to
     * each core. tagging the partitions with the shard makes it easier to track
     * which decisions route to which core.
     */
    struct shard_partitions {
        ss::shard_id shard;
        fragmented_vector<partition> partitions;
    };

    /*
     * holds information about reclaimable space partitions across all cores.
     * policies are applied to the schedule and manipulate it (e.g. recording
     * eviction decisions). the schedule exposes a round-robin iterator
     * interface for policies.
     */
    struct schedule {
        std::vector<shard_partitions> shards;
        const size_t sched_size;

        // iterator position in the shards vector
        size_t shard_idx{0};
        // iterator position in the shards[shard_idx].partitions vector
        size_t partition_idx{0};

        explicit schedule(std::vector<shard_partitions> shards, size_t size)
          : shards(std::move(shards))
          , sched_size(size) {}

        /*
         * reposition the iterator at the cursor location. note that the cursor
         * doesn't correspond to a specific position. after a call to seek(N)
         * then current() will return a pointer to the (N % sched_size)-th
         * partition managed by this schedule.
         *
         * preconditions:
         *   - sched_size > 0
         */
        void seek(size_t cursor);

        /*
         * advance the iterator to the next partition.
         *
         * preconditions:
         *   - seek() has been invoked
         */
        void next();

        /*
         * return current partition's reclaimable offsets
         *
         * preconditions:
         *   - seek() has been invoked
         */
        partition* current();
    };

public:
    eviction_policy(
      ss::sharded<cluster::partition_manager>* pm,
      ss::sharded<storage::api>* storage)
      : _pm(pm)
      , _storage(storage) {}

    /*
     * create a new schedule containing information about partitions on the
     * system. initially the schedule will contain no eviction decisions.
     */
    ss::future<schedule> create_new_schedule();

    /*
     * install the schedule by applying eviction decisions on all cores.
     */
    ss::future<> install_schedule(schedule);

private:
    ss::sharded<cluster::partition_manager>* _pm;
    ss::sharded<storage::api>* _storage;

    ss::future<fragmented_vector<partition>> collect_reclaimable_offsets();
    ss::future<size_t> install_schedule(shard_partitions);
};

/*
 *
 */
class disk_space_manager {
    static constexpr ss::shard_id run_loop_core = 0;

public:
    disk_space_manager(
      config::binding<bool> enabled,
      config::binding<std::optional<uint64_t>> log_storage_target_size,
      ss::sharded<storage::api>* storage,
      ss::sharded<storage::node>* storage_node,
      ss::sharded<cloud_storage::cache>* cache,
      ss::sharded<cluster::partition_manager>* pm);

    disk_space_manager(disk_space_manager&&) noexcept = delete;
    disk_space_manager& operator=(disk_space_manager&&) noexcept = delete;
    disk_space_manager(const disk_space_manager&) = delete;
    disk_space_manager& operator=(const disk_space_manager&) = delete;
    ~disk_space_manager() = default;

    ss::future<> start();
    ss::future<> stop();

private:
    config::binding<bool> _enabled;
    ss::sharded<storage::api>* _storage;
    ss::sharded<storage::node>* _storage_node;
    ss::sharded<cloud_storage::cache>* _cache;
    ss::sharded<cluster::partition_manager>* _pm;

    node::notification_id _cache_disk_nid;
    node::notification_id _data_disk_nid;
    // details from last disk notification
    node::disk_space_info _cache_disk_info{};
    node::disk_space_info _data_disk_info{};

    ss::future<> manage_data_disk(uint64_t target_size);
    config::binding<std::optional<uint64_t>> _log_storage_target_size;

    ss::gate _gate;
    ss::future<> run_loop();
    ssx::semaphore _control_sem{0, "resource_mgmt::space_manager"};
    bool _previous_reclaim{false};
};

} // namespace storage
