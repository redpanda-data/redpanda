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
#include "cloud_topics/level_one/compaction/committer.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/compaction/source.h"
#include "cloud_topics/level_one/compaction/worker_probe.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cluster/metadata_cache.h"
#include "compaction/key_offset_map.h"
#include "config/property.h"
#include "ssx/work_queue.h"

class WorkerManagerTestFixture;

namespace cloud_topics::l1 {

class worker_manager;

// A per-shard worker that accepts compaction jobs and performs de-duplication
// using a `sink`, `source`, and `reducer`.
// Can be pre-empted to either cancel or stop a compaction job.
class compaction_worker {
public:
    // Describes whether a worker on a given shard is `active` and available
    // for compaction jobs, or `paused` and temporarily unavailable, or fully
    // `stopped`.
    enum class worker_state { active, paused, stopped };

    // io, metastore, and committer are all passed to the compaction `source`
    // and `sink`.
    compaction_worker(
      worker_manager*,
      io*,
      metastore*,
      compaction_committer*,
      cluster::metadata_cache*);

    // Launches background loop.
    ss::future<> start();

    // Closes concurrency primitives and sets `_job_state` and `_worker_state`
    // to `stopped` to indicate to a potential inflight compaction job that it
    // should exit early before waiting on and clearing `_work_fut`.
    ss::future<> stop();

    // Sets `_state = compaction_job_state::soft_stop`. This is a request to
    // checkpoint any valuable progress from the inflight compaction job and
    // finish at earliest convenience, e.g. when a worker shard is being
    // pre-empted for various reasons. It is up to users/currently running
    // compaction jobs to respect this flag.
    //
    // This function cancels the inflight compaction job but does not affect the
    // worker state- the worker will continue to accept compaction jobs
    // after this function is called.
    void interrupt_current_job();

    // Sets `_state = compaction_job_state::hard_stop`, indicating the inflight
    // compaction job should stop promptly and abandon any in progress work,
    // e.g. during shutdown. It is up to users/currently running compaction jobs
    // to respect this flag.
    //
    // This function stops the inflight compaction job but does not affect
    // the worker state- the worker will continue to accept compaction jobs
    // after this function is called.
    void terminate_current_job();

    // Submits a `do_pause_worker()` job to the `_worker_update_queue`.
    ss::future<> pause_worker();

    // Submits a `do_resume_worker()` job to the `_worker_update_queue`.
    ss::future<> resume_worker();

    // Alert the worker that new work has become available by signalling
    // `_worker_cv`.
    void alert_worker();

private:
    // Kicks off a backgrounded loop held in `_work_fut` which waits for alerts
    // and polls occasionally to perform compaction work.
    void start_work_loop();

    // The main compaction loop which waits for jobs to become available.
    ss::future<> work_loop();

    // Waits for `_work_fut`'s future to resolve and clears its value (if it has
    // one). Leaves `_work_fut`'s value as `std::nullopt`.
    ss::future<> clear_work_fut();

    // Pauses the compaction worker by setting `_worker_state` to `paused` and
    // waits for the backgrounded `_work_fut` to complete. `_work_fut` is left
    // as `std::nullopt` as a result of this function- no new compaction jobs
    // will be processed until the worker is resumed. If `_worker_state` is not
    // `active`, this function is a no-op.
    ss::future<> do_pause_worker();

    // Resumes the compaction worker by setting `_worker_state` to `active` and
    // launches a new backgrounded job held in `_work_fut`, allowing this worker
    // to process new compaction jobs. If `_worker_state` is not `paused`, this
    // function is a no-op.
    ss::future<> do_resume_worker();

    // Requests a compaction of the provided CTP and its `compaction_offsets`
    // as obtained from the `metastore`.
    ss::future<> compact_log(log_compaction_meta*);

    // Retrieves a job from the `_worker_manager`, if there is one available.
    ss::future<std::optional<foreign_log_compaction_meta_ptr>>
    try_acquire_work_from_manager();

    // After completing a compaction job, go back to the `worker_manager` shard
    // to mark the work as "complete" (i.e reset the `meta->inflight` value to
    // indicate there is no longer an in-process compaction occurring).
    ss::future<> complete_work_on_manager(foreign_log_compaction_meta_ptr);

    // Performs lazy initialization of the `compaction::key_offset_map` using
    // its reserved memory, if it is uninitialized.
    ss::future<> initialize_map();

    // Returns `true` iff the worker is currently in an `active` state. That is,
    // the worker has not been `paused`, nor has it been `stopped` or is in the
    // process of shutdown.
    bool is_active() const;

private:
    friend class ::WorkerManagerTestFixture;

    // The state of a potentially inflight compaction job (`idle`, `running`,
    // `cancelled`, or `stopped`) on this worker. `idle` means no compaction job
    // is currently running on this worker. `running` means a compaction job is
    // inflight. `cancelled` means that the inflight compaction job on this
    // worker has been requested to checkpoint its valuable progress and finish
    // at earliest convenience (a graceful stop), whereas `stopped` means that
    // the inflight compaction job running on this worker has been pre-empted to
    // abandon all work and return as soon as possible. `cancelled`/`stopped` do
    // not mean that the worker itself is stopped from running future compaction
    // jobs.
    compaction_job_state _job_state{compaction_job_state::idle};

    // The state of the worker, which is `active`, `paused`, or `stopped`.
    // * A worker in an `active` state should have an active `_work_fut` value
    //   which is accepting and completing compaction jobs.
    // * A worker in a `paused` state has `_work_fut == std::nullopt` and is not
    //   accepting compaction jobs.
    // * A worker in a `stopped` state is in the process of shutting down and
    //   therefore has its concurrency primitives closed and is not accepting
    //   compaction jobs.
    worker_state _worker_state{worker_state::active};

    std::optional<model::ntp> _inflight_ntp;

    // If set, this is the active background loop for taking jobs from the
    // `_worker_manager` and compacting them.
    std::optional<ss::future<>> _work_fut;

    // A queue which is used to linearize pause/resume requests of this worker.
    ssx::work_queue _worker_update_queue;

    // The shard local key-offset map used for de-duplication during compaction.
    // This is lazily initialized when a compaction job is first ran on this
    // worker/shard.
    std::unique_ptr<compaction::hash_key_offset_map> _map{nullptr};

    ss::gate _gate;

    ss::abort_source _as;

    // Used to alert worker that a job has become available, or when
    // `cloud_topics_compaction_interval_ms` config changes.
    ss::condition_variable _worker_cv;

    // The interval on which the worker polls for new work.
    config::binding<std::chrono::milliseconds> _poll_interval;

    // Owned by `scheduler`.
    worker_manager* _worker_manager;

    // Owned by `app`.
    io* _io;

    // Owned by `app`.
    metastore* _metastore;

    // Owned by `scheduler`.
    compaction_committer* _committer;

    cluster::metadata_cache* _metadata_cache;

    compaction_worker_probe _probe;
};

} // namespace cloud_topics::l1
