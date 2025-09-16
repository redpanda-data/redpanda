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
#include "cloud_topics/level_one/compaction/logger.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "cloud_topics/level_one/compaction/worker.h"
#include "container/chunked_circular_buffer.h"
#include "container/chunked_hash_map.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "ssx/future-util.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/sharded.hh>

namespace cloud_topics::l1 {

class compaction_executor {
public:
    compaction_executor(
      ss::sharded<file_io>*, ss::sharded<compaction_committer>*);

    // Starts the pool of workers, making them available for compaction jobs.
    ss::future<> start();

    // Destructs workers.
    // Should only be called after all inflight compactions have been stopped
    // (`request_stop_inflight_compactions()` is a _request_ to stop inflight
    // compactions, but does not upon return guarantee all inflight jobs have
    // yet been stopped)
    ss::future<> stop();

    // Waits for an available worker from the pool and then issues a
    // backgrounded compaction job for the provided `log` on that worker's
    // shard.
    ss::future<> compact_log(log_info_and_meta);

    // If an inflight compaction job for the provided `ntp` exists, a signal is
    // sent to the worker shard on which the job is occurring to request an
    // early abort. The returned future from this function does not, upon
    // resolving, guarantee that the inflight compaction (if underway) has been
    // stopped, only that a pre-emption request has been made.
    ss::future<> request_stop_compaction(model::ntp);

    // Requests that all workers (and inflight compaction jobs) be stopped
    // promptly, and requests an abort of the local abort source. Workers will
    // no longer accept compaction jobs after this function has been called, and
    // waiters will be declined. The returned future from this function does
    // not, upon resolving, guarantee that inflight compactions (if any) have
    // been stopped, only that pre-emption requests have been made.
    ss::future<> request_stop_executor();

private:
    using worker_shard = ss::shard_id;

    // Dispatches a background compaction job for the provided `log` on the
    // provided `worker_shard`.
    void do_compact_log(worker_shard, log_info_and_meta);

    // Returns a shard for which a compaction job can be immediately scheduled
    // on the local worker. If no worker is immediatel available, one is waited
    // upon.
    ss::future<worker_shard> get_available_worker();

private:
    // Owned by `app`.
    ss::sharded<file_io>* _io;

    // Owned by `scheduler`.
    ss::sharded<compaction_committer>* _committer;

    ss::abort_source _as;

    // Used to alert worker waiters that a shard has become available.
    ss::condition_variable _cvar;

    // Tracks available workers and is used as a pool from which new compaction
    // jobs can be issued.
    chunked_circular_buffer<worker_shard> _avail_workers;

    // Tracks inflight compaction jobs by mapping `ntp`s being compacted to the
    // `shard` on which they are being compacted.
    chunked_hash_map<model::ntp, worker_shard> _inflight;

    // A sharded pool of compaction workers.
    ss::sharded<compaction_worker> _workers;
};

} // namespace cloud_topics::l1
