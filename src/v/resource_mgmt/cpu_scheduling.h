/*
 * Copyright 2020 Redpanda Data, Inc.
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

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/scheduling.hh>

// Manages cpu scheduling groups. Use the singleton instance() method to access
// scheduling groups from any server or shard that needs to schedule
// continuations into a given group.
//
// IMPORTANT: Scheduling groups are intentionally never destroyed after
// creation, as they may be captured by lingering continuations/timers and
// accessed after destruction, leading to undefined behavior. The singleton
// pattern enforces one instance per process, avoiding reactor limits (~16
// groups) and metric registration conflicts.
class scheduling_groups final {
public:
    /// Returns the singleton instance, creating it on first access.
    static scheduling_groups& instance() {
        static scheduling_groups instance = []() {
            scheduling_groups groups;
            groups.create_groups().get();
            return groups;
        }();
        return instance;
    }

    ss::future<> create_groups() {
        /**
         * Scheduling group to process requests received via the REST API of
         * admin server.
         */
        _admin = co_await ss::create_scheduling_group("admin", 100);
        /**
         * Raft receive scheduling group. Used for processing Raft
         * requests on the receiver side of an RPC protocol. i.e. on the
         * follower.
         */
        _raft_recv = co_await ss::create_scheduling_group("raft_recv", 1000);
        /**
         * Kafka scheduling group. Used for parsing and processing Kafka
         * requests.
         */
        _kafka = co_await ss::create_scheduling_group("kafka", 1000);
        /**
         * Group used for handling cluster metadata.
         */
        _cluster = co_await ss::create_scheduling_group("cluster", 300);
        /**
         * Batch cache background reclaimer works in this scheduling group.
         */
        _cache_background_reclaim = co_await ss::create_scheduling_group(
          "cache_background_reclaim", 200);
        /**
         * Log compaction scheduling group. Dynamically adjustted by compaction
         * backlog controller.
         */
        _compaction = co_await ss::create_scheduling_group(
          "log_compaction", 100);
        /**
         * Raft send scheduling group. Used for Raft send part. Raft leader
         * election and buffered protocol dispatch loop runs in this group.
         */
        _raft_send = co_await ss::create_scheduling_group("raft_send", 1000);
        /**
         * Group used to run the archival upload process. Controller dynamically
         * based on the upload backlog.
         */
        _archival_upload = co_await ss::create_scheduling_group(
          "archival_upload", 100);
        /**
         * Raft group to run the raft heartbeats in. This group has the highest
         * priority to make sure Raft failure detector is not starved even in
         * highly loaded clusters.
         */
        _raft_heartbeats = co_await ss::create_scheduling_group(
          "raft_hb", 1500);
        /**
         * Group used to schedule a self test.
         */
        _self_test = co_await ss::create_scheduling_group("self_test", 100);
        /**
         * Group used to handle computational expensive fetch request processing
         * part.
         */
        _fetch = co_await ss::create_scheduling_group("fetch", 1000);
        /**
         * WASM transforms scheduling group.
         */
        _transforms = co_await ss::create_scheduling_group("transforms", 100);
        /**
         * Group used to run datalake translation.
         */
        _datalake = co_await ss::create_scheduling_group("datalake", 100);
        /**
         * Group used to handle Kafka produce requests, most of the Raft leader
         * replication part is done in this scheduling group.
         */
        _produce = co_await ss::create_scheduling_group("produce", 1000);
        /**
         * Group used to handle tiered storage reads.
         * Lower priority than raft writes, to mitigate promotion to tiered
         * storage cache starving out producers.
         */
        _ts_read = co_await ss::create_scheduling_group("ts_read", 500);
        /**
         * Group used to handle cluster linking tasks. It includes tasks
         * synchronizing metadata as well as replication.
         */
        _cluster_linking = co_await ss::create_scheduling_group(
          "cluster_linking", 600);
    }

    ss::scheduling_group admin_sg() { return _admin; }
    ss::scheduling_group raft_recv_sg() { return _raft_recv; }
    ss::scheduling_group kafka_sg() { return _kafka; }
    ss::scheduling_group cluster_sg() { return _cluster; }

    ss::scheduling_group cache_background_reclaim_sg() {
        return _cache_background_reclaim;
    }
    ss::scheduling_group compaction_sg() { return _compaction; }
    ss::scheduling_group raft_send_sg() { return _raft_send; }
    ss::scheduling_group archival_upload() { return _archival_upload; }
    ss::scheduling_group raft_heartbeats() { return _raft_heartbeats; }
    ss::scheduling_group self_test_sg() { return _self_test; }
    ss::scheduling_group transforms_sg() { return _transforms; }
    ss::scheduling_group datalake_sg() { return _datalake; }
    /**
     * @brief Scheduling group for fetch requests.
     *
     * This scheduling group is used for consumer fetch processing. We assign
     * it the same priority as the default group (where most other kafka
     * handling takes place), but by putting it into its own group we prevent
     * non-fetch requests from being significantly delayed when fetch requests
     * use all the CPU.
     */
    ss::scheduling_group fetch_sg() { return _fetch; }
    /**
     * Scheduling group for produce requests.
     *
     * We use separate scheduling group for produce requests to prevent them
     * from negatively impacting other work executed in the default scheduling
     * group.
     */
    ss::scheduling_group produce_sg() { return _produce; }

    ss::scheduling_group ts_read_sg() { return _ts_read; }

    ss::scheduling_group cluster_linking_sg() { return _cluster_linking; }

    std::vector<std::reference_wrapper<const ss::scheduling_group>>
    all_scheduling_groups() const {
        return {
          std::cref(_default),
          std::cref(_admin),
          std::cref(_raft_recv),
          std::cref(_kafka),
          std::cref(_cluster),
          std::cref(_cache_background_reclaim),
          std::cref(_compaction),
          std::cref(_raft_send),
          std::cref(_archival_upload),
          std::cref(_raft_heartbeats),
          std::cref(_self_test),
          std::cref(_fetch),
          std::cref(_transforms),
          std::cref(_datalake),
          std::cref(_produce),
          std::cref(_ts_read),
          std::cref(_cluster_linking)};
    }

private:
    scheduling_groups() = default;

    ss::scheduling_group _default{
      seastar::default_scheduling_group()}; // created and managed by seastar
    ss::scheduling_group _admin;
    ss::scheduling_group _raft_recv;
    ss::scheduling_group _kafka;
    ss::scheduling_group _cluster;
    ss::scheduling_group _cache_background_reclaim;
    ss::scheduling_group _compaction;
    ss::scheduling_group _raft_send;
    ss::scheduling_group _archival_upload;
    ss::scheduling_group _raft_heartbeats;
    ss::scheduling_group _self_test;
    ss::scheduling_group _fetch;
    ss::scheduling_group _transforms;
    ss::scheduling_group _datalake;
    ss::scheduling_group _produce;
    ss::scheduling_group _ts_read;
    ss::scheduling_group _cluster_linking;
};
