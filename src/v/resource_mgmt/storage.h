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

#include "base/seastarx.h"
#include "config/property.h"
#include "raft/fundamental.h"
#include "ssx/semaphore.h"
#include "storage/node.h"

#include <seastar/core/sharded.hh>

namespace cloud_storage {
class cache;
}

namespace cluster {
class partition_manager;
namespace node {
class local_monitor;
}
} // namespace cluster

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

        // estimated partitions ize
        size_t size;

        /*
         * used when applying policies to the schedule.
         *
         * the offset at which the scheduling policy would like this partition
         * to be prefix truncated, along with the total amount of space
         * estimated to be reclaimed.
         */
        std::optional<model::offset> decision;
        size_t total{0};

        /*
         * used when applying policies to the schedule.
         *
         * pointer to one of the offset groups in the offsets member. this
         * pointer allows policy evaluation to know when the iterator needs
         * to initialized for the given phase.
         */
        ss::chunked_fifo<reclaimable_offsets::offset>* level{nullptr};

        /*
         * used when applying policies to the schedule.
         *
         * the iterator points to the next reclaimable offset for consideration
         * within the context of the current policy phase being evaluated.
         *
         * the only reason this is an optional<T> is because the seastar
         * chunked_fifo iterator doesn't have a default constructor.
         */
        std::optional<ss::chunked_fifo<reclaimable_offsets::offset>::iterator>
          iter;
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
     * if the resulting schedule is non-empty then the policy's cursor will be
     * normalized to the new schedule's size.
     */
    ss::future<schedule> create_new_schedule();

    /*
     * balanced eviction of segments across all partitions without violating any
     * partition's local retention policy. when local retention is advisory
     * this evicts data that has expand best-effort past local retention.
     */
    size_t evict_until_local_retention(schedule&, size_t);

    /*
     * balanced eviction of segments across all partitions without explicit
     * local retention settings. eviction does not proceed past the configured
     * low space level for the partition.
     */
    size_t evict_until_low_space_non_hinted(schedule&, size_t);

    /*
     * same as non-hinted variant, but includes partitions with explicitly
     * configured local retention.
     */
    size_t evict_until_low_space_hinted(schedule&, size_t);

    /*
     * balanced eviction until partition active segment is reached.
     */
    size_t evict_until_active_segment(schedule&, size_t);

    /*
     * install the schedule by applying eviction decisions on all cores.
     */
    ss::future<> install_schedule(schedule);

private:
    ss::sharded<cluster::partition_manager>* _pm;
    ss::sharded<storage::api>* _storage;

    /*
     * used to approximate round-robin iteration across partitions in a
     * schedule, such as balanced removal of old segments.
     */
    size_t _cursor{0};

    /*
     * marks segments for eviction from a scheduling level using a round robin
     * balanced strategy. the process ends if the target eviction size is
     * achieved or the process stops making progress.
     */
    using level_selector = std::function<
      ss::chunked_fifo<reclaimable_offsets::offset>*(partition*)>;
    size_t evict_balanced_from_level(
      schedule&, size_t, std::string_view, const level_selector&);

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
      config::binding<bool> enabled_override,
      config::binding<std::optional<uint64_t>> retention_target_capacity_bytes,
      config::binding<std::optional<double>> retention_target_capacity_percent,
      config::binding<double> disk_reservation_percent,
      ss::sharded<cluster::node::local_monitor>* local_monitor,
      ss::sharded<storage::api>* storage,
      ss::sharded<storage::node>* storage_node,
      ss::sharded<cloud_storage::cache>* cache,
      ss::sharded<cluster::partition_manager>* pm);

    disk_space_manager(disk_space_manager&&) noexcept = delete;
    disk_space_manager& operator=(disk_space_manager&&) noexcept = delete;
    disk_space_manager(const disk_space_manager&) = delete;
    disk_space_manager& operator=(const disk_space_manager&) = delete;
    ~disk_space_manager() = default;

    bool enabled();

    ss::future<> start();
    ss::future<> stop();

private:
    class probe {
    public:
        explicit probe(disk_space_manager*);

        void setup_metrics();

        /*
         * The total amount of data usage from the perspective of space
         * management. This is generally a bit different from the total amount
         * of space that redpanda is using because space management only
         * considers data that it may eventually be able to reclaim. This value
         * should reflect the total disk usage for both cloud and non-cloud
         * enabled topics.
         */
        void set_total_usage(size_t usage) noexcept { _total_usage = usage; }

        /*
         * This is the amount of data that is currently reclaimable by the
         * normal retention policy. Space management only kicks in when it needs
         * to reclaim more data than is available through the normal retention
         * mechanism.
         */
        void set_retention_reclaimable(size_t size) noexcept {
            _retention_reclaimable = size;
        }

        /*
         * The total available amount for reclaim by space management is the
         * amount of data reclaimable by normal retention policy plus the amount
         * of data has been uploaded into the cloud, and thus safe to remove if
         * necessary to meet the configured usage target.
         */
        void set_available_reclaimable(size_t size) noexcept {
            _available_reclaimable = size;
        }

        /*
         * The total amount of data that can be safely reclaimed and which if
         * reclaimed would not violate the local retention policy.
         */
        void set_local_retention_reclaimable(size_t size) noexcept {
            _local_retention_reclaimable = size;
        }

        /*
         * The amount of data usage above the target threshold. when space
         * management runs if this value is positive then space management will
         * try to ensure this much data is reclaimed, if that is by applying
         * normal retention policy or applying space management over policy.
         */
        void set_target_excess(size_t size) noexcept { _target_excess = size; }

        /*
         * The amount of data estimated to be reclaimed by the currently
         * installed space management schedule that is above the local retention
         * policy setting.
         */
        void set_reclaim_local(size_t size) noexcept { _reclaim_local = size; }

        /*
         * The amount of data estimated to be reclaimed by the currently
         * installed space management schedule that is above the non-hinted
         * low-space threshold setting.
         */
        void set_reclaim_low_non_hinted(size_t size) noexcept {
            _reclaim_low_non_hinted = size;
        }

        /*
         * The amount of data estimated to be reclaimed by the currently
         * installed space management schedule that is above the hinted
         * low-space threshold setting.
         */
        void set_reclaim_low_hinted(size_t size) noexcept {
            _reclaim_low_hinted = size;
        }

        /*
         * The amount of data estimated to be reclaimed by the currently
         * installed space management schedule that is above the active segment.
         */
        void set_reclaim_active_segment(size_t size) noexcept {
            _reclaim_active_segment = size;
        }

        /*
         * The amount of data that the latest reclaim schedule has estimated
         * will be reclaimed. A positive value means that space management is
         * trying to reclaim data that would othewise not be reclaimed by the
         * normal retention policy.
         */
        void set_reclaim_estimate(size_t size) noexcept {
            _reclaim_estimate = size;
        }

    private:
        disk_space_manager* _sm;
        metrics::internal_metric_groups _metrics;
        size_t _total_usage{0};
        size_t _retention_reclaimable{0};
        size_t _available_reclaimable{0};
        size_t _local_retention_reclaimable{0};
        size_t _target_excess{0};
        size_t _reclaim_local{0};
        size_t _reclaim_low_non_hinted{0};
        size_t _reclaim_low_hinted{0};
        size_t _reclaim_active_segment{0};
        size_t _reclaim_estimate{0};
    };

    config::binding<bool> _enabled;
    config::binding<bool> _enabled_override;
    ss::sharded<cluster::node::local_monitor>* _local_monitor;
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
    config::binding<std::optional<uint64_t>> _retention_target_capacity_bytes;
    config::binding<std::optional<double>> _retention_target_capacity_percent;
    config::binding<double> _disk_reservation_percent;
    size_t _data_disk_size;
    size_t _target_size;
    void update_target_size();

    eviction_policy _policy;

    ss::gate _gate;
    ss::future<> run_loop();
    ssx::semaphore _control_sem{0, "resource_mgmt::space_manager"};
    bool _previous_reclaim{false};
    probe _probe;
};

} // namespace storage
