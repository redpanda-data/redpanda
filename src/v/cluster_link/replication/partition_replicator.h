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

#include "cluster_link/replication/deps.h"
#include "ssx/semaphore.h"
#include "utils/prefix_logger.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>

#include <utils/backoff_policy.h>

namespace cluster_link::replication {

/**
 * A partition replicator is responsible for replicating data from a remote
 * partition to the corresponding local partition. Each partition replicator
 * instance manages replication for a single local partition leader on the
 * current shard.
 *
 * Architecture:
 *
 *   +------------+                    +-------------+
 *   |data_source |                    | data_sink   |
 *   +------------+                    +-------------+
 *        |                                   ^
 *        | fetch_batches()                   | replicate()
 *        v                                   |
 *   +-----------------------------------------+
 *   |        partition_replicator             |
 *   |    fetch_and_replicate() loop           |
 *   +-----------------------------------------+
 *
 * Operation:
 * 1. The fetch_and_replicate() method executes a continuous loop that fetches
 *    data from the data_source and replicates it to the data_sink
 * 2. A semaphore (_max_requests) controls the maximum number of concurrent
 *    replicate requests to enable request pipelining
 * 3. The wait_for_replication_result() method manages replication completion
 *    and error handling
 * 4. Requests are enqueued synchronously while replication results are
 *    processed asynchronously
 * 5. Any replication failure causes the fetch loop to abort and triggers
 *    a reset of the data_source
 */

class partition_replicator {
public:
    explicit partition_replicator(
      const ::model::ntp& ntp,
      ::model::term_id,
      std::unique_ptr<data_source> source,
      std::unique_ptr<data_sink> sink,
      ss::scheduling_group sg = ss::default_scheduling_group());
    ss::future<> start();
    ss::future<> stop();

    ::model::term_id term() const { return _term; }

    void notify_sink_on_failure(::model::term_id) const;

private:
    struct replicate_ctx {
        ::model::offset begin;
        ::model::offset end;
        chunked_vector<::model::record_batch> batches;
        ssx::semaphore_units inflight_units;
        ssx::semaphore_units data_units;
    };
    ss::future<> fetch_and_replicate();
    ss::future<>
    replicate_and_wait(replicate_ctx, ss::gate&, ss::abort_source&);
    // Returns true if replication was successful, false if it failed
    ss::future<bool> handle_replication_result(
      ss::future<result<raft::replicate_result>>,
      ::model::offset begin,
      ::model::offset end) noexcept;
    ::model::term_id _term;
    prefix_logger _log;
    ss::gate _gate;
    ss::abort_source _as;
    std::unique_ptr<data_source> _source;
    std::unique_ptr<data_sink> _sink;
    ss::scheduling_group _scheduling_group;
    // to pipeline multiple replicate requests in parallel
    static constexpr ssize_t max_in_flight_requests = 5;
    ssx::semaphore _max_requests{
      max_in_flight_requests, "partition_replicator"};
    backoff_policy _backoff_policy;
};

} // namespace cluster_link::replication
