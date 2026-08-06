// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Multi-connection fairness + fan-in/fan-out.
//
// RTT and throughput benches run a single connection; real workloads
// don't. This file exercises the concurrency axis at N ∈ {1, 2, 4, 8,
// 16, 32, 64}, in two shapes:
//
//   fan_in  : N clients → one shared listener → one accept loop per
//             connection. Stresses the server's accept+scheduler path
//             and the per-connection input/output streams. This is
//             the typical "many producers to one broker" shape.
//
//   fan_out : reverse — one client coroutine opens N connections to
//             one listener, each with its own accept/echo. Measures
//             whether a single reactor can keep N sockets fed.
//
// Each cell reports the aggregate "RPC count" across all N connections
// so PERF_TEST_CN derives per-RPC cost; the harness's own histogram
// gives the tail latency number. Per-client variance is not captured
// at this layer — that's what the rpk-driven end-to-end bench in
// nix/bench.nix handles with a producer-per-client fan-out run.
//
// Keep --smp=1: this bench measures transport + Seastar scheduler
// under one reactor, not cross-shard dispatch.

#include "base/units.h"
#include "net/tests/uds_bench_common.hh"

#include <seastar/core/future.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/when_all.hh>
#include <seastar/testing/perf_tests.hh>

#include <vector>

namespace {

using uds_bench::run_echo;
using uds_bench::transport;

// Per-connection RPC count; keep modest so the 64-client cell doesn't
// dominate total test time. Aggregate RPCs = rpcs_per_conn * N.
constexpr size_t rpcs_per_conn = 64;

// Build a listener and an address we can connect to repeatedly.
struct listener_only {
    ss::server_socket listener;
    ss::socket_address addr;
};

ss::future<listener_only> make_listener(transport t, std::string_view tag) {
    ss::listen_options lo;
    lo.reuse_address = true;
    ss::socket_address addr;
    if (t == transport::uds) {
        auto path = uds_bench::uds_socket_path(tag);
        std::error_code ec;
        std::filesystem::remove(path.c_str(), ec);
        addr = ss::socket_address(ss::unix_domain_addr(std::string(path)));
    } else {
        addr = ss::socket_address(ss::ipv4_addr("127.0.0.1", 0));
    }
    auto listener = ss::engine().listen(addr, lo);
    auto local = t == transport::uds ? addr : listener.local_address();
    co_return listener_only{
      .listener = std::move(listener),
      .addr = local,
    };
}

// One client coroutine: N back-to-back RPCs of `payload` bytes each.
template<typename Tag>
ss::future<> one_client(ss::socket_address addr, size_t payload) {
    constexpr transport T = Tag::value;
    auto sock = co_await ss::engine().connect(addr);
    if (T == transport::tcp_nodelay) {
        sock.set_nodelay(true);
    }
    auto in = sock.input();
    auto out = sock.output();
    ss::sstring filler(payload, 'x');
    for (size_t i = 0; i < rpcs_per_conn; ++i) {
        co_await out.write(filler.data(), payload);
        co_await out.flush();
        auto buf = co_await in.read_exactly(payload);
        perf_tests::do_not_optimize(buf);
    }
    co_await out.close();
    co_await in.close();
}

// Accept N connections and hand each to serve_one(). Free function so
// the coroutine frame owns its parameters — a lambda-coroutine IIFE
// with `[&]` captures would be use-after-free per CLAUDE.md.
ss::future<> accept_all(
  ss::server_socket& listener,
  std::vector<ss::future<>>& servers,
  size_t n,
  transport t);

// One server coroutine bound to an accepted connection: echo until
// peer closes.
ss::future<> serve_one(ss::accept_result ar, transport t) {
    if (t == transport::tcp_nodelay) {
        ar.connection.set_nodelay(true);
    }
    auto in = ar.connection.input();
    auto out = ar.connection.output();
    co_await run_echo(in, out);
    co_await out.close();
    co_await in.close();
}

ss::future<> accept_all(
  ss::server_socket& listener,
  std::vector<ss::future<>>& servers,
  size_t n,
  transport t) {
    for (size_t i = 0; i < n; ++i) {
        auto ar = co_await listener.accept();
        servers.push_back(serve_one(std::move(ar), t));
    }
}

// Fan-in: N concurrent clients → one listener. The server spawns an
// accept task and hands each accepted socket off to serve_one(); the
// client side fires N one_client() futures in parallel.
template<typename Tag>
ss::future<size_t> fan_in_run(size_t n_clients, size_t payload) {
    constexpr transport T = Tag::value;
    auto l = co_await make_listener(T, "conc-in");

    std::vector<ss::future<>> servers;
    servers.reserve(n_clients);
    auto accept_loop = accept_all(l.listener, servers, n_clients, T);

    std::vector<ss::future<>> clients;
    clients.reserve(n_clients);
    for (size_t i = 0; i < n_clients; ++i) {
        clients.push_back(one_client<Tag>(l.addr, payload));
    }

    perf_tests::start_measuring_time();
    co_await ss::when_all(clients.begin(), clients.end()).discard_result();
    perf_tests::stop_measuring_time();

    co_await std::move(accept_loop);
    co_await ss::when_all(servers.begin(), servers.end()).discard_result();
    l.listener.abort_accept();
    co_return n_clients* rpcs_per_conn;
}

// Fan-out: identical graph to fan-in in this single-reactor setup
// (clients and servers share the same reactor), but the client side
// launches as a single coroutine that spawns the N sub-coroutines.
// Kept as a separate function so the reported cell name is meaningful
// and so future asymmetric variants (e.g. client-on-shard-0,
// servers-on-shard-1) can diverge here without touching fan_in_run.
template<typename Tag>
ss::future<size_t> fan_out_run(size_t n_clients, size_t payload) {
    // For now identical to fan_in_run — the perf harness reports a
    // separate cell, and the symmetry is an observation worth
    // documenting in the RFC: with --smp=1, fan-in and fan-out hit the
    // same scheduler + transport paths.
    return fan_in_run<Tag>(n_clients, payload);
}

struct tcp_nagle {
    static constexpr uds_bench::transport value
      = uds_bench::transport::tcp_nagle;
};
struct tcp_nodelay {
    static constexpr uds_bench::transport value
      = uds_bench::transport::tcp_nodelay;
};
struct uds {
    static constexpr uds_bench::transport value = uds_bench::transport::uds;
};

} // namespace

// clang-format off
#define FAN_CELL(group, T, shape, name, n, bytes) \
    PERF_TEST_CN(group, name) { co_return co_await shape##_run<T>((n), (bytes)); }

// 1 KiB payload is the "small RPC" shape motivating UDS; pick a single
// payload for the N sweep to keep cell count manageable.
#define FAN_N_SWEEP(group, T, shape)                   \
    FAN_CELL(group, T, shape, shape##_n1,    1,    1_KiB) \
    FAN_CELL(group, T, shape, shape##_n2,    2,    1_KiB) \
    FAN_CELL(group, T, shape, shape##_n4,    4,    1_KiB) \
    FAN_CELL(group, T, shape, shape##_n8,    8,    1_KiB) \
    FAN_CELL(group, T, shape, shape##_n16,  16,    1_KiB) \
    FAN_CELL(group, T, shape, shape##_n32,  32,    1_KiB) \
    FAN_CELL(group, T, shape, shape##_n64,  64,    1_KiB)

#define FAN_TRANSPORT(group, T) \
    FAN_N_SWEEP(group, T, fan_in) \
    FAN_N_SWEEP(group, T, fan_out)

FAN_TRANSPORT(tcp_nagle,   tcp_nagle)
FAN_TRANSPORT(tcp_nodelay, tcp_nodelay)
FAN_TRANSPORT(uds,         uds)
// clang-format on
