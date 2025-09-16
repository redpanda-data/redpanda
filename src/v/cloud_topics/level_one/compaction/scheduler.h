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
#include "cloud_topics/level_one/compaction/committer.h"
#include "cloud_topics/level_one/compaction/executor.h"
#include "cloud_topics/level_one/compaction/log_collector.h"
#include "cloud_topics/level_one/compaction/log_sampler.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/compaction/scheduling_policies.h"
#include "config/property.h"
#include "model/fundamental.h"
#include "ssx/semaphore.h"

namespace cloud_topics::l1 {

/*
 * Responsible for scheduling compaction for cloud topic partitions.
 */
class compaction_scheduler {
public:
    compaction_scheduler(
      compaction_cluster_state,
      std::unique_ptr<scheduling_policy>,
      ss::sharded<file_io>*);

    // Starts the contained `_log_collector`, `_executor`, and the backgrounded
    // scheduling loop.
    ss::future<> start();

    // Shuts down concurrency primitives, thereby stopping the backgrounded
    // scheduling loop, stops the `_log_collector`, requests inflight compaction
    // jobs in the `_executor` be stopped, drains the managed partition log
    // list, and finally shuts down the `_executor` once it is safe to do so.
    ss::future<> stop();

    // Returns `true` iff the provided `tid_p` is managed by this scheduler.
    bool is_managed(const model::ntp&) const noexcept;

    // Pushes a new `tid_p` to be managed by this scheduler to the list of
    // `tid_p`s. It is the caller's responsibility to ensure the partition is
    // not already managed by this scheduler.
    void manage_partition(
      const model::ntp&, const model::topic_id_partition&, std::string_view);

    // Removes the `tid_p` from the list of managed partitions. No-ops if the
    // provided `tid_p` is not managed by this scheduler. Because the `tid_p`
    // may be undergoing an inflight compaction, this function will block until
    // it is complete (an early stop is requested by this function).
    ss::future<> unmanage_partition(const model::ntp&, std::string_view);

private:
    // Starts the backgrounded scheduling loop.
    void start_bg_loop();

    // The main compaction loop. Invoked in a background fiber until `_as` has
    // an abort requested or the `_gate` is closed.
    ss::future<> scheduling_loop();

    // Samples managed logs and schedules compactions.
    ss::future<> schedule_some();

    // Filters provided vector of log information, leaving only logs that
    // require compaction in the container.
    void filter_log_infos(chunked_vector<log_info_and_meta>&) const;

private:
    // Pointer to sharded `file_io` held by `app`. Used by the `executor` for
    // writing to local files and by the `committer` for writing to cloud
    // storage.
    ss::sharded<file_io>* _io;

    // Pointer to metastore.
    metastore* _metastore;

private:
    // Responsible for pushing logs to manage/unmanage to this scheduler.
    std::unique_ptr<log_collector> _log_collector;

    // Responsible for collecting compaction metadata (see: `log_info` in
    // `meta.h`) for managed logs during a scheduling loop.
    log_sampler _log_sampler;

    // Responsible for sorting logs collected by sampler for compaction.
    std::unique_ptr<scheduling_policy> _scheduling_policy;

    // Responsible for dispatching compaction jobs to per-shard workers.
    compaction_executor _executor;

    // Responsible for committing updates from sharded jobs ran on the
    // `executor` to the metastore and uploading compacted objects to cloud
    // storage.
    ss::sharded<compaction_committer> _committer;

    // The interval on which compaction loop is executed.
    config::binding<std::chrono::milliseconds> _compaction_interval;

    // This semaphore is used as a way to signal a change to
    // `log_compaction_interval_ms` during the `wait()` operation in the main
    // scheduling loop.
    ssx::semaphore _sem{0, "cloud_topics::compaction::scheduling_loop"};

    ss::abort_source _as;
    ss::gate _gate;

    // Set of logs this scheduler is responsible for issuing compaction jobs
    // for.
    logs_type_t _logs;

    // Intrusive list of logs this scheduler is responsible for issuing
    // compaction jobs for.
    log_list_t _logs_list;
};

std::unique_ptr<compaction_scheduler> make_default_compaction_scheduler(
  compaction_cluster_state, ss::sharded<file_io>*);

} // namespace cloud_topics::l1
