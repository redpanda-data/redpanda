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

#include "cluster/self_test/netcheck.h"

#include "cluster/logger.h"
#include "utils/gate_guard.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>

#include <absl/container/flat_hash_map.h>
#include <boost/range/irange.hpp>

namespace cluster::self_test {

class netcheck_unreachable_peer_exception final : public netcheck_exception {
public:
    explicit netcheck_unreachable_peer_exception(model::node_id peer)
      : netcheck_exception(
        fmt::format("Failed to reach peer with node_id: {}", peer)) {}
};

void netcheck::validate_options(const netcheck_opts& opts) {
    if (opts.peers.empty()) {
        throw netcheck_exception("Internal setting, 'peers' must not be empty");
    }
    if (opts.request_size <= 0 || opts.request_size > (1ULL << 27)) {
        throw netcheck_option_out_of_range(
          "Request size must be > 0 and < 128MB");
    }
    if (opts.parallelism <= 0 || opts.parallelism > 256) {
        throw netcheck_option_out_of_range(
          "Parallelism option must be > 0 and <= 256");
    }
    const auto duration = std::chrono::duration_cast<std::chrono::seconds>(
      opts.duration);
    if (duration < 1s || opts.duration > (5 * 60s)) {
        throw netcheck_option_out_of_range(
          "Duration must be between 1 second and 5 minutes");
    }
}

/// Returns groupings of nodes that the network bench will run between. Simple
/// for loop groups unique pairs of nodes with eachother.
netcheck::plan_t
netcheck::network_test_plan(std::vector<model::node_id> nodes) {
    /// Choose unique pairs of nodes - order doesn't matter. This creates a list
    /// of client/server pairs where there are no instances of a pair for which
    /// the client/server roles are reversed. The intention is to only perform
    /// the netcheck benchmark one time per unique pair.
    plan_t plan;
    for (size_t i = 0; i < nodes.size(); ++i) {
        for (size_t j = i + 1; j < nodes.size(); ++j) {
            plan[nodes[i]].emplace_back(nodes[j]);
        }
    }
    return plan;
}

netcheck::netcheck(
  model::node_id self, ss::sharded<rpc::connection_cache>& connections)
  : _self(self)
  , _connections(connections) {}

ss::future<> netcheck::start() { return ss::now(); }

ss::future<> netcheck::stop() {
    _as.request_abort();
    return _gate.close();
}

void netcheck::cancel() { _cancelled = true; }

ss::future<std::vector<self_test_result>> netcheck::run(netcheck_opts opts) {
    _cancelled = false;
    _opts = opts;
    try {
        gate_guard g{_gate};
        co_await ss::futurize_invoke(validate_options, opts);
        vlog(
          clusterlog.info,
          "Starting redpanda self-test network benchmark, with options: {}",
          opts);
        co_return co_await ss::with_scheduling_group(opts.sg, [this]() {
            return ssx::async_transform(
              _opts.peers, [this](model::node_id peer) {
                  return run_individual_benchmark(peer);
              });
        });
    } catch (const netcheck_aborted_exception& ex) {
        vlog(clusterlog.debug, "netcheck stopped due to call of stop()");
    } catch (const ss::gate_closed_exception&) {
        vlog(clusterlog.debug, "netcheck - ss::gate_closed_exception caught");
    }
    co_return std::vector<self_test_result>{};
}

ss::future<self_test_result>
netcheck::run_individual_benchmark(model::node_id peer) {
    auto irange = boost::irange<uint16_t>(0, _opts.parallelism);
    static const auto two_seconds_us = 200000;
    metrics m{two_seconds_us};
    vassert(
      _opts.max_duration >= _opts.duration, "Misconfigured self_test plan");
    const auto max_deadline = ss::lowres_clock::now() + _opts.max_duration;
    self_test_result result;
    try {
        auto durations = co_await ssx::parallel_transform(
          irange, [this, max_deadline, peer, &m](auto) {
              return run_benchmark_fiber(
                run_fiber_opts(max_deadline, _opts.duration), peer, m);
          });

        /// Total time will be the average time all fibers took to complete
        const auto total_duration = std::accumulate(
          durations.begin(), durations.end(), ss::lowres_clock::duration{0});
        m.set_total_time(total_duration / _opts.parallelism);
        result = m.to_st_result();
        if (total_duration == 0ms) {
            /// Constant timeouts prevented test from any successful work
            throw netcheck_unreachable_peer_exception(peer);
        }
        if (_cancelled) {
            result.warning = "Run was manually cancelled";
        }
    } catch (const netcheck_unreachable_peer_exception& ex) {
        result.error = ex.what();
    }
    result.name = _opts.name;
    result.info = fmt::format("Test performed against node: {}", peer);
    result.test_type = "network";
    co_return result;
}

ss::future<ss::lowres_clock::duration> netcheck::run_benchmark_fiber(
  run_fiber_opts fiber_state, model::node_id peer, metrics& m) {
    /// run_fiber_opts manages the the RPC request timeouts to ensure that
    /// the maximum amount of time for an individual RPC never exceeds the
    /// remaining time of the test
    while (!_cancelled && fiber_state.should_continue()) {
        if (unlikely(_as.abort_requested())) {
            throw netcheck_aborted_exception();
        }
        auto req = co_await make_netcheck_request(_self, _opts.request_size);
        co_await m.measure(
          [this, req = std::move(req), peer, &fiber_state, &m]() mutable {
              return _connections.local()
                .with_node_client<self_test_rpc_client_protocol>(
                  _self,
                  ss::this_shard_id(),
                  peer,
                  1s,
                  [req = std::move(req)](
                    self_test_rpc_client_protocol c) mutable {
                      return c.netcheck(
                        std::move(req), rpc::client_opts(200ms));
                  })
                .then([this, peer, &fiber_state, &m](auto reply) {
                    return process_netcheck_reply(
                      std::move(reply), fiber_state, peer, m);
                });
          });
    }
    co_return fiber_state.end - fiber_state.begin;
}

ss::future<size_t> netcheck::process_netcheck_reply(
  result<rpc::client_context<netcheck_response>> reply,
  run_fiber_opts& fiber_state,
  model::node_id peer,
  metrics& m) {
    if (!reply) {
        if (
          reply.error() == rpc::errc::client_request_timeout
          || reply.error() == rpc::errc::exponential_backoff) {
            co_await ss::sleep_abortable(200ms, _as);
            throw omit_measure_timed_out_exception();
        }
        if (reply.error() == rpc::errc::disconnected_endpoint) {
            /// Peer is unreachable exit test
            throw netcheck_unreachable_peer_exception(peer);
        }
        throw netcheck_exception(reply.error().message());
    } else if (reply.value().data.bytes_read == 0) {
        /// Server is already processing requests from a benchmark
        /// via another node, sleep until can proceed, measurement
        /// will be thrown away
        if (fiber_state.not_started()) {
            co_await ss::sleep_abortable(200ms, _as);
        } else {
            /// Server replied with benchmark already running, however this
            /// fiber previously was in a state of 'running'. Possible that this
            /// fiber has lost connection long enough for another fiber on
            /// another node to take hold of the servers "lock".
            throw netcheck_exception(
              "Fiber lost benchmark lock, encountered a network interruption "
              "delay");
        }
        /// Not an error, waiting for test to actually begin
        throw omit_metrics_measurement_exception();
    }
    /// Otherwise, server ack'ed positively, benchmark is proceeding
    if (fiber_state.not_started()) {
        fiber_state.start();
    }
    vassert(
      reply.value().data.bytes_read == _opts.request_size,
      "Benchmark expected different value in response");

    /// Return number of bytes moved through current send/recv.
    /// NOTE: actual value is slightly more due to serde headers.
    co_return reply.value().data.bytes_read;
}

} // namespace cluster::self_test
