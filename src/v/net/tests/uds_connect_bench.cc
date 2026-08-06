// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Connection lifecycle bench. This is the class of test many teams
// forget: TCP pays much more than UDS for churn-heavy workloads because
// of SYN/ACK, accept queue, and TIME_WAIT socket state. We compare:
//
//   short_lived : for each operation, connect → 1 RPC → close.
//                 Measures full connection setup cost.
//
//   long_lived  : single persistent connection, N RPCs.
//                 The baseline most steady-state benches accidentally
//                 measure; included so the ratio is visible.
//
// If your real traffic pattern opens many short-lived channels (batch
// jobs, CLI tools, sidecar health probes), short_lived is what the
// user actually feels. If you already hold a warm pool, long_lived is
// the fair comparison.
//
// TIME_WAIT is a real cost for TCP here: without SO_REUSEADDR on the
// server socket we'd exhaust ephemeral ports. The server socket is
// created once per test-case by make_pair_for_listener() — only the
// client side churns.

#include "base/units.h"
#include "net/tests/uds_bench_common.hh"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/testing/perf_tests.hh>

namespace {

using uds_bench::make_pair;
using uds_bench::transport;

// Build just the listener; the client will connect repeatedly.
struct listener_only {
    ss::server_socket listener;
    ss::socket_address addr;
};

ss::future<listener_only>
make_listener(transport t, std::string_view tag = "conn") {
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

// Server-side accept loop that services N connections: for each
// accepted socket, echo one request of `payload` bytes back and close.
// Runs concurrently with the client churn loop.
ss::future<>
accept_n(ss::server_socket& listener, size_t n, size_t payload, transport t) {
    for (size_t i = 0; i < n; ++i) {
        auto ar = co_await listener.accept();
        if (t == transport::tcp_nodelay) {
            ar.connection.set_nodelay(true);
        }
        auto in = ar.connection.input();
        auto out = ar.connection.output();
        auto buf = co_await in.read_exactly(payload);
        co_await out.write(buf.get(), buf.size());
        co_await out.flush();
        co_await out.close();
        co_await in.close();
    }
}

// Short-lived: each of N iterations opens a fresh connection, sends
// one `payload`-byte request, reads the echo, closes. Measures total
// time for the N handshakes + N RPCs.
template<typename Tag>
ss::future<size_t> short_lived_run(size_t payload, size_t n) {
    constexpr transport T = Tag::value;
    auto l = co_await make_listener(T, "conn-short");
    auto serve = accept_n(l.listener, n, payload, T);

    ss::sstring filler(payload, 'x');

    perf_tests::start_measuring_time();
    for (size_t i = 0; i < n; ++i) {
        auto sock = co_await ss::engine().connect(l.addr);
        if (T == transport::tcp_nodelay) {
            sock.set_nodelay(true);
        }
        auto in = sock.input();
        auto out = sock.output();
        co_await out.write(filler.data(), payload);
        co_await out.flush();
        auto echo = co_await in.read_exactly(payload);
        perf_tests::do_not_optimize(echo);
        co_await out.close();
        co_await in.close();
    }
    perf_tests::stop_measuring_time();

    co_await std::move(serve);
    l.listener.abort_accept();
    co_return n;
}

// Long-lived: single connection, N back-to-back RPCs. Same server-side
// echo loop as the RTT bench. The ratio (short/long) is what the RFC
// cares about per-transport.
template<typename Tag>
ss::future<size_t> long_lived_run(size_t payload, size_t n) {
    constexpr transport T = Tag::value;
    auto pair = co_await make_pair(T, "conn-long");
    auto server_in = pair.server_side.input();
    auto server_out = pair.server_side.output();
    auto client_in = pair.client_side.input();
    auto client_out = pair.client_side.output();
    auto echo = uds_bench::run_echo(server_in, server_out);

    ss::sstring filler(payload, 'x');

    perf_tests::start_measuring_time();
    for (size_t i = 0; i < n; ++i) {
        co_await client_out.write(filler.data(), payload);
        co_await client_out.flush();
        auto buf = co_await client_in.read_exactly(payload);
        perf_tests::do_not_optimize(buf);
    }
    perf_tests::stop_measuring_time();

    co_await client_out.close();
    co_await client_in.close();
    co_await std::move(echo);
    co_await server_out.close();
    co_await server_in.close();
    pair.listener.abort_accept();
    co_return n;
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

// Churn of 256 connections is enough to amortize the PERF_TEST_CN
// per-iteration overhead while keeping each cell under ~500 ms.
constexpr size_t churn_count = 256;

// clang-format off
#define CONN_SHORT(group, T, name, bytes) \
    PERF_TEST_CN(group, name) { co_return co_await short_lived_run<T>((bytes), churn_count); }

#define CONN_LONG(group, T, name, bytes) \
    PERF_TEST_CN(group, name) { co_return co_await long_lived_run<T>((bytes), churn_count); }

#define CONN_TRANSPORT(group, T)                 \
    CONN_SHORT(group, T, short_p64b,  64)        \
    CONN_SHORT(group, T, short_p1k,   1_KiB)     \
    CONN_LONG(group,  T, long_p64b,   64)        \
    CONN_LONG(group,  T, long_p1k,    1_KiB)

CONN_TRANSPORT(tcp_nagle,   tcp_nagle)
CONN_TRANSPORT(tcp_nodelay, tcp_nodelay)
CONN_TRANSPORT(uds,         uds)
// clang-format on
