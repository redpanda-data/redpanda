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

#include "cloud_topics/level_one/common/file_io.h"
#include "cloud_topics/level_one/maintenance/logger.h"

namespace cloud_topics {
class l1_reader_cache;
} // namespace cloud_topics
#include "cloud_topics/level_one/maintenance/meta.h"
#include "cloud_topics/level_one/maintenance/scheduler_probe.h"
#include "cloud_topics/level_one/maintenance/worker.h"
#include "cloud_topics/level_one/metastore/replicated_metastore.h"
#include "cluster/metadata_cache.h"
#include "container/chunked_hash_map.h"
#include "model/fundamental.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/sharded.hh>

class WorkerManagerTestFixture;
class SchedulerTestFixture;

namespace cloud_topics::l1 {

// A worker_manager which exists as a singleton on shard0, owns a sharded pool
// of `compaction_worker`s, and provides access to a priority queue of CTPs
// which require compaction. Manages inflight compactions and can request early
// abort of inflight jobs.
// TODO: Hook this up to the AdminAPI to allow for users to customize which
// shards have active `compaction_worker`s, and persist that information in e.g.
// the kvstore.
class worker_manager {
public:
    static constexpr ss::shard_id worker_manager_shard = 0;

    worker_manager(
      log_compaction_queue&,
      ss::sharded<file_io>*,
      ss::sharded<replicated_metastore>*,
      ss::sharded<cluster::metadata_cache>*,
      compaction_scheduler_probe&,
      ss::sharded<level_one_reader_probe>*,
      ss::sharded<cloud_topics::l1_reader_cache>*);

    // Starts the pool of workers, making them available for compaction jobs.
    ss::future<> start();

    // Stops all workers (and inflight compaction jobs) and then destructs
    // workers. Workers will no longer accept compaction jobs after this
    // function has been called, and waiters will be declined. This should only
    // be invoked during application shutdown.
    ss::future<> stop();

    // Returns the top entry of `_compaction_queue`, if it is not empty, and
    // sets inflight state for the provided shard & CTP. Returns `std::nullopt`
    // if the `_compaction_queue` is empty.
    std::optional<foreign_log_compaction_meta_ptr>
      try_acquire_compaction_work(ss::shard_id);

    // Resets inflight state for the provided CTP.
    void complete_compaction_work(log_compaction_meta*);

    // If an inflight compaction job for the provided log exists, a signal is
    // sent to the worker shard on which the job is occurring to request an
    // early abort. The returned future from this function does not, upon
    // resolving, guarantee that the inflight compaction (if underway) has been
    // stopped, only that a pre-emption request has been made.
    //
    // Note that stopping compaction is much different than fully stopping a
    // worker. This function leaves the worker in a valid state, allowing future
    // compaction jobs to be ran. This function is ideally used when e.g. a
    // partition is removed or the `cleanup.policy` for a topic is changed and a
    // single compaction job must be stopped.
    void request_stop_compaction(log_compaction_meta_ptr);

    // Alert all workers that new jobs have become available in the
    // `_compaction_queue`.
    ss::future<> alert_compaction_workers();

    // Pauses the worker on the provided shard.
    ss::future<> pause_worker(ss::shard_id);

    // Resumes the worker on the provided shard.
    ss::future<> resume_worker(ss::shard_id);

private:
    friend class ::WorkerManagerTestFixture;
    friend class ::SchedulerTestFixture;

    // Owned by `scheduler`.
    log_compaction_queue& _compaction_queue;

    // Owned by `app`.
    ss::sharded<file_io>* _io;

    // Owned by `app`.
    ss::sharded<replicated_metastore>* _metastore;

    ss::sharded<cluster::metadata_cache>* _metadata_cache;

    // Owned by `scheduler`.
    compaction_scheduler_probe& _probe;

    // Owned by `app`.
    ss::sharded<level_one_reader_probe>* _l1_reader_probe;

    // Owned by `app`.
    ss::sharded<cloud_topics::l1_reader_cache>* _reader_cache;

    // A sharded pool of compaction workers.
    ss::sharded<compaction_worker> _workers;

    ss::gate _gate;
};

} // namespace cloud_topics::l1
