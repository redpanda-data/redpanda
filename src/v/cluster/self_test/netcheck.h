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

#include "bytes/iobuf.h"
#include "cluster/self_test/metrics.h"
#include "cluster/self_test_rpc_service.h"
#include "cluster/self_test_rpc_types.h"
#include "rpc/connection_cache.h"
#include "seastarx.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>

#include <boost/range/irange.hpp>

namespace cluster::self_test {

class netcheck_exception : public std::runtime_error {
public:
    explicit netcheck_exception(const std::string& msg)
      : std::runtime_error(msg) {}
};
class netcheck_option_out_of_range final : public netcheck_exception {
public:
    explicit netcheck_option_out_of_range(const std::string& msg)
      : netcheck_exception(msg) {}
};
class netcheck_aborted_exception final : public netcheck_exception {
public:
    netcheck_aborted_exception()
      : netcheck_exception("User aborted benchmark") {}
};

class netcheck {
public:
    using plan_t
      = absl::flat_hash_map<model::node_id, std::vector<model::node_id>>;

    /// Made public for unit testing, only used internally
    ///
    static void validate_options(const netcheck_opts& opts);

    /// Made public for unit testing, only used internally
    ///
    static plan_t network_test_plan(std::vector<model::node_id> nodes);

    /// Class constructor
    ///
    netcheck(
      model::node_id self, ss::sharded<rpc::connection_cache>& connections);

    /// Initialize the benchmark
    ///
    /// Functionally does nothing, throws on detection of bad or invalid test
    /// parameters
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
    ss::future<std::vector<self_test_result>> run(netcheck_opts);

    /// Signal to stop all work as soon as possible
    ///
    /// Immediately returns, waiter can expect to wait on the results to be
    /// returned by \run to be available shortly
    void cancel();

private:
    /// State used to manage single network test fiber. Contains logic to bound
    /// test end duration.
    struct run_fiber_opts {
        enum class netcheck_status { not_started, running };
        netcheck_status nstatus{netcheck_status::not_started};

        const ss::lowres_clock::time_point max_deadline;
        const ss::lowres_clock::duration duration;
        ss::lowres_clock::time_point begin;
        ss::lowres_clock::time_point end;

        run_fiber_opts(
          ss::lowres_clock::time_point max, ss::lowres_clock::duration d)
          : max_deadline(max)
          , duration(d) {}

        bool should_continue() const {
            auto now = ss::lowres_clock::now();
            if (not_started()) {
                return now < max_deadline;
            }
            return now < std::min(max_deadline, end);
        }

        bool not_started() const {
            return nstatus == netcheck_status::not_started;
        }
        void start() {
            nstatus = netcheck_status::running;
            begin = ss::lowres_clock::now();
            end = begin + duration;
        }
        void clear() { nstatus = netcheck_status::not_started; }
    };

    ss::future<self_test_result> run_individual_benchmark(model::node_id peer);

    ss::future<ss::lowres_clock::duration> run_benchmark_fiber(
      run_fiber_opts fiber_state, model::node_id peer, metrics& m);

    ss::future<size_t> process_netcheck_reply(
      result<rpc::client_context<netcheck_response>> reply,
      run_fiber_opts& fiber_state,
      model::node_id peer,
      metrics& m);

private:
    model::node_id _self;
    bool _cancelled{false};
    netcheck_opts _opts;
    ss::abort_source _as;
    ss::gate _gate;
    ss::sharded<rpc::connection_cache>& _connections;
};

} // namespace cluster::self_test
