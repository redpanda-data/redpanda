// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Throughput bench: complementary to uds_rtt_bench.
//
// RTT is "how fast is one round-trip"; this file is "how much work can
// the transport move per second, and what does it cost in CPU". Three
// modes, each on the {tcp_nagle, tcp_nodelay, uds} transport axis and a
// small payload ladder:
//
//   unidirectional : client writes N × payload, server drains.
//                    Best case for each transport; exposes send-side
//                    cost and the TCP Nagle penalty on small frames.
//
//   bidirectional  : both sides write simultaneously, each drains the
//                    other. Exposes full-duplex buffering and scheduler
//                    fairness.
//
//   rr_saturation  : as many back-to-back 1-in-flight round-trips as
//                    possible. Exposes per-RPC syscall cost — the
//                    scenario where UDS should shine because the kernel
//                    path is shortest.
//
// For each cell we capture rusage pre/post the timed region and report
// CPU µs/byte + CPU µs/RPC, so the RFC can show "UDS delivers the same
// throughput at half the CPU" rather than just "UDS is faster".

#include "base/units.h"
#include "net/tests/uds_bench_common.hh"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/when_all.hh>
#include <seastar/testing/perf_tests.hh>

namespace {

using uds_bench::conn_pair;
using uds_bench::make_pair;
using uds_bench::run_echo;
using uds_bench::rusage_sample;
using uds_bench::transport;

// Drain an input stream until EOF. Free function (not a lambda
// coroutine) so its frame captures the stream reference safely — see
// CLAUDE.md's lambda-coroutine note about IIFE `[&]` coroutines.
ss::future<> drain_stream(ss::input_stream<char>& in) {
    try {
        while (!in.eof()) {
            auto buf = co_await in.read();
            if (buf.empty()) {
                break;
            }
            perf_tests::do_not_optimize(buf);
        }
    } catch (...) {
    }
}

ss::future<> write_loop(
  ss::output_stream<char>& out,
  const ss::sstring& filler,
  size_t writes,
  size_t flush_every) {
    for (size_t i = 0; i < writes; ++i) {
        co_await out.write(filler.data(), filler.size());
        if ((i + 1) % flush_every == 0) {
            co_await out.flush();
        }
    }
    co_await out.flush();
    co_await out.close();
}

// Total bytes moved per bench invocation is `batches * depth *
// payload_size` — picked so a single run completes in a few hundred
// milliseconds even at 1 MiB payloads, to keep the perf harness's
// per-iteration overhead negligible.
constexpr size_t batches = 64;

// Unidirectional: client writes, server drains and never replies. The
// server side just reads to EOF; the client side does all the work.
// Reports per-payload byte count so PERF_TEST_CN can derive bytes/s.
template<typename Tag>
ss::future<size_t> unidirectional_run(size_t payload_size, size_t depth) {
    constexpr transport T = Tag::value;
    auto pair = co_await make_pair(T, "thr-uni");
    auto server_in = pair.server_side.input();
    auto server_out = pair.server_side.output();
    auto client_out = pair.client_side.output();

    // Server drains input until EOF — symmetric to run_echo() minus the
    // write-back. Uses a free function so the coroutine frame owns the
    // stream reference (lambda-coroutine IIFEs with `[&]` captures are
    // use-after-free per CLAUDE.md).
    auto drain = drain_stream(server_in);

    ss::sstring filler(payload_size, 'x');

    auto r0 = rusage_sample::now();
    perf_tests::start_measuring_time();
    for (size_t b = 0; b < batches; ++b) {
        for (size_t j = 0; j < depth; ++j) {
            co_await client_out.write(filler.data(), payload_size);
        }
        co_await client_out.flush();
    }
    perf_tests::stop_measuring_time();
    auto r1 = rusage_sample::now();
    auto delta = r1 - r0;
    // perf_tests doesn't have a first-class "extra metrics" hook, so we
    // log user/sys µs with the harness's stdout stream; post-processing
    // picks it up per test-case name.
    fmt::print(
      "[usage {} p{} d{}] user_us={} sys_us={} ctxsw_vol={} ctxsw_invol={}\n",
      uds_bench::transport_name(T),
      payload_size,
      depth,
      delta.user_us,
      delta.sys_us,
      delta.ctxsw_vol,
      delta.ctxsw_invol);

    co_await client_out.close();
    co_await std::move(drain);
    co_await server_in.close();
    co_await server_out.close();
    pair.listener.abort_accept();
    co_return batches* depth;
}

// Bidirectional: both ends write and drain concurrently. Uses
// when_all_succeed to fire the four pipelines and wait for completion.
template<typename Tag>
ss::future<size_t> bidirectional_run(size_t payload_size, size_t depth) {
    constexpr transport T = Tag::value;
    auto pair = co_await make_pair(T, "thr-bi");
    auto server_in = pair.server_side.input();
    auto server_out = pair.server_side.output();
    auto client_in = pair.client_side.input();
    auto client_out = pair.client_side.output();

    ss::sstring filler(payload_size, 'x');
    size_t writes_per_side = batches * depth;

    auto r0 = rusage_sample::now();
    perf_tests::start_measuring_time();
    co_await ss::when_all_succeed(
      write_loop(client_out, filler, writes_per_side, depth),
      write_loop(server_out, filler, writes_per_side, depth),
      drain_stream(client_in),
      drain_stream(server_in))
      .discard_result();
    perf_tests::stop_measuring_time();
    auto r1 = rusage_sample::now();
    auto delta = r1 - r0;
    fmt::print(
      "[usage {} bidi p{} d{}] user_us={} sys_us={} ctxsw_vol={} "
      "ctxsw_invol={}\n",
      uds_bench::transport_name(T),
      payload_size,
      depth,
      delta.user_us,
      delta.sys_us,
      delta.ctxsw_vol,
      delta.ctxsw_invol);

    co_await client_in.close();
    co_await server_in.close();
    pair.listener.abort_accept();
    co_return writes_per_side * 2;
}

// Request/response saturation: strict depth-1 ping-pong as fast as the
// transport allows. This is the "tail latency at offered load" scenario
// the RFC cares most about for service-mesh displacement.
template<typename Tag>
ss::future<size_t> rr_saturation_run(size_t payload_size) {
    constexpr transport T = Tag::value;
    auto pair = co_await make_pair(T, "thr-rr");
    auto server_in = pair.server_side.input();
    auto server_out = pair.server_side.output();
    auto client_in = pair.client_side.input();
    auto client_out = pair.client_side.output();

    auto echo = run_echo(server_in, server_out);
    ss::sstring filler(payload_size, 'x');
    size_t rpcs = batches * 256; // tuned so a cell runs in O(100 ms)

    auto r0 = rusage_sample::now();
    perf_tests::start_measuring_time();
    for (size_t i = 0; i < rpcs; ++i) {
        co_await client_out.write(filler.data(), payload_size);
        co_await client_out.flush();
        auto buf = co_await client_in.read_exactly(payload_size);
        perf_tests::do_not_optimize(buf);
    }
    perf_tests::stop_measuring_time();
    auto r1 = rusage_sample::now();
    auto delta = r1 - r0;
    fmt::print(
      "[usage {} rr p{}] user_us={} sys_us={} ctxsw_vol={} ctxsw_invol={}\n",
      uds_bench::transport_name(T),
      payload_size,
      delta.user_us,
      delta.sys_us,
      delta.ctxsw_vol,
      delta.ctxsw_invol);

    co_await client_out.close();
    co_await client_in.close();
    co_await std::move(echo);
    co_await server_out.close();
    co_await server_in.close();
    pair.listener.abort_accept();
    co_return rpcs;
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

// Unidirectional: one representative depth (64) to bound the total
// test time; payload varies across 64B, 4KiB, 64KiB, 1MiB. The cells
// without UDS should show the gap narrowing with payload — that's
// expected and worth keeping in the data so the RFC can argue about
// it honestly.
//
// clang-format off
#define THR_UNI(group, T, name, bytes) \
    PERF_TEST_CN(group, name) { co_return co_await unidirectional_run<T>((bytes), 64); }

#define THR_UNI_TRANSPORT(group, T)              \
    THR_UNI(group, T, uni_p64b,  64)             \
    THR_UNI(group, T, uni_p4k,   4_KiB)          \
    THR_UNI(group, T, uni_p64k,  64_KiB)         \
    THR_UNI(group, T, uni_p1m,   1_MiB)

THR_UNI_TRANSPORT(tcp_nagle,   tcp_nagle)
THR_UNI_TRANSPORT(tcp_nodelay, tcp_nodelay)
THR_UNI_TRANSPORT(uds,         uds)

#define THR_BIDI(group, T, name, bytes) \
    PERF_TEST_CN(group, name) { co_return co_await bidirectional_run<T>((bytes), 64); }

#define THR_BIDI_TRANSPORT(group, T)             \
    THR_BIDI(group, T, bidi_p64b,  64)           \
    THR_BIDI(group, T, bidi_p4k,   4_KiB)        \
    THR_BIDI(group, T, bidi_p64k,  64_KiB)

THR_BIDI_TRANSPORT(tcp_nagle,   tcp_nagle)
THR_BIDI_TRANSPORT(tcp_nodelay, tcp_nodelay)
THR_BIDI_TRANSPORT(uds,         uds)

#define THR_RR(group, T, name, bytes) \
    PERF_TEST_CN(group, name) { co_return co_await rr_saturation_run<T>((bytes)); }

#define THR_RR_TRANSPORT(group, T)               \
    THR_RR(group, T, rr_p8b,   8)                \
    THR_RR(group, T, rr_p64b,  64)               \
    THR_RR(group, T, rr_p1k,   1_KiB)            \
    THR_RR(group, T, rr_p16k,  16_KiB)

THR_RR_TRANSPORT(tcp_nagle,   tcp_nagle)
THR_RR_TRANSPORT(tcp_nodelay, tcp_nodelay)
THR_RR_TRANSPORT(uds,         uds)
// clang-format on
