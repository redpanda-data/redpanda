// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Shared fixture for the UDS-vs-TCP bench family
// (uds_rtt_bench, uds_throughput_bench, uds_connect_bench,
// uds_concurrency_bench). The goal is that any difference surfaced by
// the benches is attributable to the **transport**, not to asymmetric
// implementation details — so read/write strategy, framing, buffering,
// and reactor integration all live here.
//
// Transports covered:
//   tcp_nagle    : AF_INET loopback, TCP_NODELAY off (small-message worst case)
//   tcp_nodelay  : AF_INET loopback, TCP_NODELAY on  (typical streaming client)
//   uds          : AF_UNIX pathname under $TMPDIR
//
// Abstract-namespace UDS is intentionally excluded: the broker's
// `broker_authn_endpoint.unix_path` validator requires absolute
// filesystem paths, so abstract sockets would measure a transport the
// server cannot accept. Treat that as a follow-up.

#pragma once

#include "base/seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/net/api.hh>

#include <fmt/format.h>
#include <sys/resource.h>

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <unistd.h>

namespace uds_bench {

// Wire format: u32 little-endian length prefix + payload. Identical on
// TCP and UDS so the cost of length-framing is constant across the
// matrix — we want to measure transport, not serialization.
inline constexpr size_t frame_header_size = sizeof(uint32_t);

enum class transport : uint8_t {
    tcp_nagle,
    tcp_nodelay,
    uds,
};

inline const char* transport_name(transport t) {
    switch (t) {
    case transport::tcp_nagle:
        return "tcp_nagle";
    case transport::tcp_nodelay:
        return "tcp_nodelay";
    case transport::uds:
        return "uds";
    }
    return "unknown";
}

struct conn_pair {
    ss::server_socket listener;
    ss::connected_socket server_side;
    ss::connected_socket client_side;
};

// Generate a per-process socket path under $TMPDIR. sun_path is 108
// bytes on Linux; the format below fits comfortably.
inline ss::sstring uds_socket_path(std::string_view tag) {
    auto p = std::filesystem::temp_directory_path()
             / fmt::format("rp-{}-{}.sock", tag, ::getpid());
    return ss::sstring{p.string()};
}

// Build a connected_socket pair for the requested transport. Both ends
// live on the same reactor shard (--smp=1 is the intended config).
inline ss::future<conn_pair>
make_pair(transport t, std::string_view tag = "bench") {
    ss::listen_options lo;
    lo.reuse_address = true;
    ss::socket_address addr;
    if (t == transport::uds) {
        auto path = uds_socket_path(tag);
        std::error_code ec;
        std::filesystem::remove(path.c_str(), ec);
        addr = ss::socket_address(ss::unix_domain_addr(std::string(path)));
    } else {
        addr = ss::socket_address(ss::ipv4_addr("127.0.0.1", 0));
    }
    auto listener = ss::engine().listen(addr, lo);
    auto local = t == transport::uds ? addr : listener.local_address();
    auto accepted = listener.accept();
    auto client = co_await ss::engine().connect(local);
    auto ar = co_await std::move(accepted);
    if (t == transport::tcp_nodelay) {
        client.set_nodelay(true);
        ar.connection.set_nodelay(true);
    } else if (t == transport::tcp_nagle) {
        client.set_nodelay(false);
        ar.connection.set_nodelay(false);
    }
    co_return conn_pair{
      .listener = std::move(listener),
      .server_side = std::move(ar.connection),
      .client_side = std::move(client),
    };
}

// Echo loop: reads whatever the peer sent and writes the same bytes
// back. Chunk boundary is determined by the kernel, which is the point
// — we measure real transport behavior, not an artificially-framed
// request/response.
inline ss::future<>
run_echo(ss::input_stream<char>& in, ss::output_stream<char>& out) {
    try {
        while (!in.eof()) {
            auto buf = co_await in.read();
            if (buf.empty()) {
                break;
            }
            co_await out.write(std::move(buf));
            co_await out.flush();
        }
    } catch (...) {
        // Socket torn down mid-bench is expected; don't propagate.
    }
}

// getrusage()-based CPU sampler for the throughput bench. Sampled
// immediately before and after the timed window so ctor/dtor of the
// bench state is not attributed to transport cost.
struct rusage_sample {
    uint64_t user_us{0};
    uint64_t sys_us{0};
    uint64_t ctxsw_vol{0};
    uint64_t ctxsw_invol{0};
    uint64_t max_rss_kb{0};

    static rusage_sample now() {
        struct rusage ru{};
        ::getrusage(RUSAGE_SELF, &ru);
        auto us = [](const struct timeval& tv) -> uint64_t {
            return uint64_t(tv.tv_sec) * 1'000'000 + uint64_t(tv.tv_usec);
        };
        return rusage_sample{
          .user_us = us(ru.ru_utime),
          .sys_us = us(ru.ru_stime),
          .ctxsw_vol = uint64_t(ru.ru_nvcsw),
          .ctxsw_invol = uint64_t(ru.ru_nivcsw),
          .max_rss_kb = uint64_t(ru.ru_maxrss),
        };
    }

    rusage_sample operator-(const rusage_sample& lhs) const {
        return rusage_sample{
          .user_us = user_us - lhs.user_us,
          .sys_us = sys_us - lhs.sys_us,
          .ctxsw_vol = ctxsw_vol - lhs.ctxsw_vol,
          .ctxsw_invol = ctxsw_invol - lhs.ctxsw_invol,
          .max_rss_kb = max_rss_kb, // high-watermark, not delta-able
        };
    }
};

} // namespace uds_bench
