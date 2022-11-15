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

#include "cluster/node/local_monitor.h"
#include "cluster/self_test/metrics.h"
#include "cluster/self_test_rpc_types.h"
#include "config/node_config.h"
#include "likely.h"
#include "seastarx.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/io_intent.hh>
#include <seastar/core/lowres_clock.hh>

#include <chrono>
#include <exception>

namespace cluster::self_test {

class diskcheck_exception : public std::runtime_error {
public:
    explicit diskcheck_exception(const std::string& msg)
      : std::runtime_error(msg) {}
};
class diskcheck_aborted_exception final : public diskcheck_exception {
public:
    diskcheck_aborted_exception()
      : diskcheck_exception("User aborted benchmark") {}
};
class diskcheck_misconfigured_exception final : public diskcheck_exception {
public:
    explicit diskcheck_misconfigured_exception(const ss::sstring& msg)
      : diskcheck_exception(msg) {}
};
class diskcheck_option_out_of_range final : public diskcheck_exception {
public:
    explicit diskcheck_option_out_of_range(const ss::sstring& msg)
      : diskcheck_exception(msg) {}
};

/// disk benchmark leveraging only the seastar framework
///
/// The idea is to report some metrics that the user can use to compare to
/// expected defaults to observe if a disk is configured correctly or not. Can
/// be useful when debugging to narrow down the possible causes of a 'slow' disk
/// scenario.
class diskcheck final {
public:
    /// Made public for unit testing, only used internally
    ///
    static void validate_options(const diskcheck_opts& opts);

    /// Class constructor
    ///
    explicit diskcheck(ss::sharded<node::local_monitor>& nlm);

    /// Initialize the benchmark
    ///
    ss::future<> start();

    /// Stops the benchmark
    ///
    /// On resolution of the future returned all async work will have completed
    /// upon success or with failure (abruptly stoppped - work incomplete)
    ss::future<> stop();

    /// Run the actual disk benchmark
    ///
    /// Runs sequential write then read benchmarks (unless otherwise either
    /// marked as skip in configuration options). Note that each sub-benchmark
    /// will run for at least the total run time desired.
    ss::future<std::vector<self_test_result>> run(diskcheck_opts);

    /// Signal to stop all work as soon as possible
    ///
    /// Immediately returns, waiter can expect to wait on the results to be
    /// returned by \run to be available shortly
    void cancel();

private:
    enum class read_or_write { read, write };

    ss::future<std::vector<self_test_result>> initialize_benchmark(ss::sstring);
    ss::future<std::vector<self_test_result>>
    run_configured_benchmarks(ss::file&);

    ss::future<> verify_remaining_space(size_t dataset_size);

    template<read_or_write mode>
    ss::future<metrics> do_run_benchmark(ss::file&);

    template<read_or_write mode>
    ss::future<> run_benchmark_fiber(
      ss::lowres_clock::time_point start, ss::file& file, metrics& m);

    uint64_t get_pos();

private:
    /// To ensure test doesn't attempt to take all available disk space
    ss::sharded<node::local_monitor>& _nlm;

    ss::io_intent _intent{};
    bool _cancelled{false};
    /// Next read/write offset in file
    uint64_t _last_pos{0};
    /// For shutting down service
    ss::abort_source _as;
    ss::gate _gate;
    diskcheck_opts _opts;
};

} // namespace cluster::self_test
