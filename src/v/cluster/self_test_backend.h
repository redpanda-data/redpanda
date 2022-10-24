/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "self_test_benchmarks.h"
#include "self_test_rpc_types.h"
#include "utils/mutex.h"
#include "utils/uuid.h"

#include <seastar/core/smp.hh>

namespace cluster {

/// Class representing the logic and state of how redpanda self tests run
///
/// Within this class there is a mutex that ensures only one test can
/// be executing at once. start_test() immediately returns and tests execute in
/// the background, its the clients job to periodicially query for finished
/// tests. The last successful test results and its test identifier are cached
/// for later retrival.
class self_test_backend {
public:
    static constexpr ss::shard_id shard = 0;

    ss::future<> start();
    ss::future<> stop();

    /// Starts a test if one isn't already executing
    /// Important to note that this method immediately returns, however it
    /// starts asynchronous jobs that will run in the background
    ///
    /// @param req Parameters passed by the leader
    /// @returns status of having started the tests
    get_status_response start_test(start_test_request req);

    /// Stops all currently executing tests
    /// Important to note that the future returned will be resolved when all
    /// asynchronous work has stopped if this can be done within a 5s window.
    /// Otherwise a running response status will be returned to caller.
    ///
    /// @returns Future<status> after attempt to stop all jobs
    ss::future<get_status_response> stop_test();

    /// Returns the current status of the system, i.e. are any tests currently
    /// running or not
    get_status_response get_status() const;

private:
    ss::future<std::vector<self_test_result>> do_start_test(
      std::optional<diskcheck_opts> dto, std::optional<netcheck_opts> nto);

private:
    // cached values
    uuid_t _id;
    get_status_response _prev_run{.status = self_test_status::idle};

    ss::gate _gate;
    mutex _lock;
    diskcheck _disk_test;
    networkcheck _network_test;
};
} // namespace cluster
