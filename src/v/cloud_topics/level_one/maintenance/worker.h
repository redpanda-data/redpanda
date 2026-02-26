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
#include "cloud_topics/level_one/frontend_reader/level_one_reader_probe.h"
#include "cloud_topics/level_one/maintenance/compaction/compaction_source.h"
#include "cloud_topics/level_one/maintenance/meta.h"
#include "cloud_topics/level_one/maintenance/worker_probe.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cluster/metadata_cache.h"
#include "compaction/key_offset_map.h"
#include "config/property.h"
#include "ssx/work_queue.h"
#include "utils/adjustable_semaphore.h"

#include <seastar/core/scheduling.hh>

#include <list>

class WorkerManagerTestFixture;

namespace cloud_topics::l1 {

class worker_manager;

// A per-shard worker that accepts maintenance jobs (compaction and leveling)
// and performs the relevant work using a `sink`, `source`, and `reducer`.
// Can be pre-empted to either cancel or stop an inflight job.
class maintenance_worker {
public:
    // Describes whether a worker on a given shard is `active` and available
    // for maintenance jobs, or `paused` and temporarily unavailable, or fully
    // `stopped`.
    enum class worker_state { active, paused, stopped };

    // io and metastore are passed to the `source` and `sink`.
    maintenance_worker(
      worker_manager*,
      io*,
      metastore*,
      cluster::metadata_cache*,
      ss::scheduling_group,
      level_one_reader_probe*);

    // Launches background loop.
    ss::future<> start();

    // Closes concurrency primitives and sets job states and `_worker_state`
    // to `stopped` to indicate to potential inflight jobs that they
    // should exit early before waiting on and clearing work futures.
    ss::future<> stop();

    // Sets job state to `soft_stop` for the inflight job matching `ntp`.
    // This is a request to checkpoint any valuable progress and finish at
    // earliest convenience. Does not affect the worker state.
    void interrupt_current_job(const model::ntp& ntp);

    // Sets job state to `hard_stop` for the inflight job matching `ntp`,
    // indicating it should stop promptly and abandon any in progress work.
    // Does not affect the worker state.
    void terminate_current_job(const model::ntp& ntp);

    // Interrupts all inflight compaction and leveling jobs (soft_stop).
    void interrupt_all_jobs();

    // Terminates all inflight compaction and leveling jobs (hard_stop).
    void terminate_all_jobs();

    // Submits a `do_pause_worker()` job to the `_worker_update_queue`.
    ss::future<> pause_worker();

    // Submits a `do_resume_worker()` job to the `_worker_update_queue`.
    ss::future<> resume_worker();

    void alert_compaction();
    void alert_leveling();

private:
    // Per-job context for a concurrent leveling fiber.
    struct leveling_job_ctx {
        maintenance_job_state state{maintenance_job_state::idle};
        model::ntp ntp;
    };

    // Kicks off backgrounded compaction and leveling loops.
    void start_compaction_loop();
    void start_leveling_loop();

    // The compaction work loop which waits for compaction jobs.
    ss::future<> compaction_work_loop();

    // The leveling work loop which waits for leveling jobs.
    ss::future<> leveling_work_loop();

    // Waits for the compaction/leveling future to resolve and clears its value.
    ss::future<> clear_compaction_fut();
    ss::future<> clear_leveling_fut();

    // Pauses the worker by setting `_worker_state` to `paused` and waits for
    // backgrounded work futures to complete. No new maintenance jobs will be
    // processed until the worker is resumed. If `_worker_state` is not
    // `active`, this function is a no-op.
    ss::future<> do_pause_worker();

    // Resumes the worker by setting `_worker_state` to `active` and relaunches
    // backgrounded work loops, allowing this worker to process new maintenance
    // jobs. If `_worker_state` is not `paused`, this function is a no-op.
    ss::future<> do_resume_worker();

    // Requests a compaction of the provided CTP and its `compaction_offsets`
    // as obtained from the `metastore`.
    ss::future<> compact_log(log_maintenance_meta*);

    // Requests a leveling rewrite of the provided CTP using the leveling
    // ranges obtained from the `metastore`.
    ss::future<> level_log(log_maintenance_meta*, leveling_job_ctx&);

    // Retrieves a compaction job from the `_worker_manager`, if available.
    ss::future<std::optional<foreign_log_maintenance_meta_ptr>>
    try_acquire_compaction_work_from_manager();

    // Retrieves a leveling job from the `_worker_manager`, if available.
    ss::future<std::optional<foreign_log_maintenance_meta_ptr>>
    try_acquire_leveling_work_from_manager();

    // After completing a maintenance job, go back to the `worker_manager` shard
    // to mark the work as "complete" (i.e reset the inflight state to indicate
    // there is no longer an in-process maintenance job occurring).
    ss::future<> complete_work_on_manager(foreign_log_maintenance_meta_ptr);

    // Performs lazy initialization of the `compaction::key_offset_map` using
    // its reserved memory, if it is uninitialized.
    ss::future<> initialize_map();

    // Returns `true` iff the worker is currently in an `active` state. That is,
    // the worker has not been `paused`, nor has it been `stopped` or is in the
    // process of shutdown.
    bool is_active() const;

private:
    friend class ::WorkerManagerTestFixture;

    // Job state for the compaction fiber.
    maintenance_job_state _compaction_job_state{maintenance_job_state::idle};

    // The state of the worker, which is `active`, `paused`, or `stopped`.
    worker_state _worker_state{worker_state::active};

    std::optional<model::ntp> _compaction_inflight_ntp;

    // Active leveling job contexts for preemption broadcast.
    std::list<leveling_job_ctx> _leveling_jobs;

    // Background loop for compaction work.
    std::optional<ss::future<>> _compaction_work_fut;

    // Background loop for leveling work (dispatcher).
    std::optional<ss::future<>> _leveling_work_fut;

    // A queue which is used to linearize pause/resume requests of this worker.
    ssx::work_queue _worker_update_queue;

    // The shard local key-offset map used for de-duplication during compaction.
    // This is lazily initialized when a compaction job is first ran on this
    // worker/shard.
    std::unique_ptr<compaction::hash_key_offset_map> _map{nullptr};

    ss::gate _gate;

    ss::abort_source _as;

    // Used to alert compaction fiber that a job has become available.
    ss::condition_variable _compaction_cv;

    // Used to alert leveling fiber that a job has become available.
    ss::condition_variable _leveling_cv;

    // The interval on which the worker polls for new compaction work.
    config::binding<std::chrono::milliseconds> _compaction_poll_interval;

    // The interval on which the worker polls for new leveling work.
    config::binding<std::chrono::milliseconds> _leveling_poll_interval;

    // Max concurrent leveling ops per worker shard.
    config::binding<size_t> _max_concurrent_leveling_ops;

    // Limits concurrent leveling fibers. Initialized from config.
    adjustable_semaphore _leveling_sem;

    // Owned by `scheduler`.
    worker_manager* _worker_manager;

    // Owned by `app`.
    io* _io;

    // Owned by `app`.
    metastore* _metastore;

    cluster::metadata_cache* _metadata_cache;

    ss::scheduling_group _compaction_sg;

    maintenance_worker_probe _probe;

    // Owned by `app`.
    level_one_reader_probe* _l1_reader_probe;
};

} // namespace cloud_topics::l1
