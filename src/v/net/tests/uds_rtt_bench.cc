// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Round-trip latency microbench: the **headline** UDS-vs-TCP comparison.
//
// Seastar's PERF_TEST_CN harness records p50/p90/p99/max per-operation
// latency for every (transport, payload, inflight) cell. We run:
//
//   transport ∈ {tcp_nagle, tcp_nodelay, uds}
//   payload   ∈ {8 B, 64 B, 256 B, 1 KiB, 4 KiB, 16 KiB, 64 KiB, 1 MiB}
//   inflight  ∈ {1, 8, 64, 256, 1024}   (outstanding requests per batch)
//
// "Inflight" means we write N frames and then read N frames, batched.
// Depth 1 isolates a single-request round trip (worst case for tail
// latency); depth 1024 exposes pipeline saturation and where the buffer
// cache stops helping.
//
// iteration count is the number of PERF_TEST_CN "runs" the harness
// executes per cell; we return `iterations * inflight` so the reported
// per-op number is the cost of one request frame plus one response
// frame, not one batch.
//
// The --smp=1 recommendation is unchanged from the old bench: AF_UNIX
// does not load-balance across Seastar shards the way SO_REUSEPORT TCP
// does, and mixing both would only muddy the data.

#include "base/units.h"
#include "net/tests/uds_bench_common.hh"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/testing/perf_tests.hh>

namespace {

using uds_bench::conn_pair;
using uds_bench::make_pair;
using uds_bench::run_echo;
using uds_bench::transport;

constexpr size_t iterations = 128;

// One pipelined round-trip batch of `depth` frames of `payload_size`
// bytes each. Returns the number of operations (frames) that crossed
// the socket in each direction, so PERF_TEST_CN's latency number is
// "per frame", not "per batch".
//
// Tag is one of the group-tag structs below, each carrying a
// `constexpr transport value` so PERF_TEST_CN(group, …) and
// make_pair(transport) share a single source of truth.
template<typename Tag>
ss::future<size_t> rtt_run(size_t payload_size, size_t depth) {
    constexpr transport T = Tag::value;
    auto pair = co_await make_pair(T, "rtt");
    auto server_in = pair.server_side.input();
    auto server_out = pair.server_side.output();
    auto client_in = pair.client_side.input();
    auto client_out = pair.client_side.output();

    auto echo = run_echo(server_in, server_out);

    ss::sstring filler(payload_size, 'x');

    perf_tests::start_measuring_time();
    for (size_t i = 0; i < iterations; ++i) {
        for (size_t j = 0; j < depth; ++j) {
            co_await client_out.write(filler.data(), payload_size);
        }
        co_await client_out.flush();
        for (size_t j = 0; j < depth; ++j) {
            auto buf = co_await client_in.read_exactly(payload_size);
            perf_tests::do_not_optimize(buf);
        }
    }
    perf_tests::stop_measuring_time();

    co_await client_out.close();
    co_await client_in.close();
    co_await std::move(echo);
    co_await server_out.close();
    co_await server_in.close();
    pair.listener.abort_accept();
    co_return iterations* depth;
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

// Size × depth matrix. The macro's test-case name encodes payload and
// pipeline depth so the report is self-describing.
//
// clang-format off
#define RTT_CELL(group, T, name, bytes, depth) \
    PERF_TEST_CN(group, name) { co_return co_await rtt_run<T>((bytes), (depth)); }

#define RTT_DEPTHS(group, T, name_prefix, bytes)             \
    RTT_CELL(group, T, name_prefix##_d1,    bytes,    1)     \
    RTT_CELL(group, T, name_prefix##_d8,    bytes,    8)     \
    RTT_CELL(group, T, name_prefix##_d64,   bytes,   64)     \
    RTT_CELL(group, T, name_prefix##_d256,  bytes,  256)     \
    RTT_CELL(group, T, name_prefix##_d1024, bytes, 1024)

#define RTT_TRANSPORT(group, T)                              \
    RTT_DEPTHS(group, T, p8b,   8)                           \
    RTT_DEPTHS(group, T, p64b,  64)                          \
    RTT_DEPTHS(group, T, p256b, 256)                         \
    RTT_DEPTHS(group, T, p1k,   1_KiB)                       \
    RTT_DEPTHS(group, T, p4k,   4_KiB)                       \
    RTT_DEPTHS(group, T, p16k,  16_KiB)                      \
    RTT_DEPTHS(group, T, p64k,  64_KiB)                      \
    RTT_DEPTHS(group, T, p1m,   1_MiB)

RTT_TRANSPORT(tcp_nagle,   tcp_nagle)
RTT_TRANSPORT(tcp_nodelay, tcp_nodelay)
RTT_TRANSPORT(uds,         uds)
// clang-format on
