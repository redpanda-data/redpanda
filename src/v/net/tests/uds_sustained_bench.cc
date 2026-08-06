// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Sustained throughput benchmark for UDS vs TCP.
//
// Unlike the PERF_TEST_CN micro-benchmarks (uds_rtt_bench,
// uds_throughput_bench), this is a standalone Seastar app that runs each
// cell for a configurable duration (default 30 s) at a target data rate.
// The goal is to produce numbers that answer "what happens when we push
// X MB/s through each transport for Y seconds" — the kind of data a CEO
// can put on a slide.
//
// Default matrix:
//   message sizes : 256 B, 1 kB, 10 kB, 30 kB, 64 kB, 100 kB
//   target rates  : 10, 100, 200, 500 Mbit/s
//   transports    : tcp_nodelay, uds
//   duration      : 30 s per cell
//
// Modes:
//   rr         : single-connection request-response (latency + throughput)
//   uni        : single-connection unidirectional (max throughput)
//   concurrent : N parallel connections (simulates N producers)
//
// Run:
//   bazel run //src/v/net/tests:uds_sustained_bench -- --smp=1
//   bazel run //src/v/net/tests:uds_sustained_bench -- --smp=1 \
//       --duration=5 --sizes=1024 --rates=10000000

#include "net/tests/uds_bench_common.hh"

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/when_all.hh>

#include <boost/program_options.hpp>
#include <fmt/format.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <deque>
#include <numeric>
#include <string>
#include <vector>

namespace po = boost::program_options;
using namespace std::chrono_literals;
using bench_clock = std::chrono::steady_clock;

namespace {

struct bench_cell {
    uds_bench::transport transport;
    size_t message_size;
    uint64_t target_rate_bps; // bits per second, 0 = unlimited
    uint32_t duration_sec;
    uint32_t warmup_sec;
};

struct cell_result {
    bench_cell cell;
    uint64_t messages_sent{0};
    uint64_t bytes_sent{0};
    double elapsed_sec{0};
    double achieved_mbps{0};
    double achieved_mbits{0};
    uds_bench::rusage_sample cpu_delta;
    // Latency percentiles (microseconds)
    uint64_t lat_p50_us{0};
    uint64_t lat_p99_us{0};
    uint64_t lat_max_us{0};
};

struct bench_config {
    uint32_t duration_sec{30};
    uint32_t warmup_sec{5};
    std::string sizes_str{"256,1024,10240,30720,65536,102400"};
    std::string rates_str{"10000000,100000000,200000000,500000000"};
    std::string transports_str{"tcp_nodelay,uds"};
    std::string mode{"rr"};
    uint32_t concurrency{2};
};

std::vector<size_t> parse_csv_sizes(const std::string& s) {
    std::vector<size_t> out;
    size_t pos = 0;
    while (pos < s.size()) {
        auto end = s.find(',', pos);
        if (end == std::string::npos) {
            end = s.size();
        }
        out.push_back(std::stoull(s.substr(pos, end - pos)));
        pos = end + 1;
    }
    return out;
}

std::vector<uint64_t> parse_csv_rates(const std::string& s) {
    std::vector<uint64_t> out;
    size_t pos = 0;
    while (pos < s.size()) {
        auto end = s.find(',', pos);
        if (end == std::string::npos) {
            end = s.size();
        }
        out.push_back(std::stoull(s.substr(pos, end - pos)));
        pos = end + 1;
    }
    return out;
}

std::vector<uds_bench::transport> parse_transports(const std::string& s) {
    std::vector<uds_bench::transport> out;
    size_t pos = 0;
    while (pos < s.size()) {
        auto end = s.find(',', pos);
        if (end == std::string::npos) {
            end = s.size();
        }
        auto name = s.substr(pos, end - pos);
        if (name == "tcp_nagle") {
            out.push_back(uds_bench::transport::tcp_nagle);
        } else if (name == "tcp_nodelay") {
            out.push_back(uds_bench::transport::tcp_nodelay);
        } else if (name == "uds") {
            out.push_back(uds_bench::transport::uds);
        }
        pos = end + 1;
    }
    return out;
}

std::string format_size(size_t bytes) {
    if (bytes >= 1024 * 1024) {
        return fmt::format("{} MB", bytes / (1024 * 1024));
    }
    if (bytes >= 1024) {
        return fmt::format("{} kB", bytes / 1024);
    }
    return fmt::format("{} B", bytes);
}

std::string format_rate(uint64_t bps) {
    if (bps == 0) {
        return "unlimited";
    }
    if (bps >= 1'000'000'000) {
        return fmt::format("{} Gbit/s", bps / 1'000'000'000);
    }
    if (bps >= 1'000'000) {
        return fmt::format("{} Mbit/s", bps / 1'000'000);
    }
    return fmt::format("{} kbit/s", bps / 1'000);
}

uint64_t percentile(std::vector<uint64_t>& sorted_data, double p) {
    if (sorted_data.empty()) {
        return 0;
    }
    auto idx = static_cast<size_t>(p * static_cast<double>(sorted_data.size()));
    if (idx >= sorted_data.size()) {
        idx = sorted_data.size() - 1;
    }
    return sorted_data[idx];
}

// Request-response mode: send a message, wait for echo, measure
// round-trip latency. Rate-paced via a token bucket.
ss::future<cell_result> run_cell_rr(bench_cell cell) {
    auto pair = co_await uds_bench::make_pair(cell.transport, "sustained");
    auto server_in = pair.server_side.input();
    auto server_out = pair.server_side.output();
    auto client_in = pair.client_side.input();
    auto client_out = pair.client_side.output();

    auto echo = uds_bench::run_echo(server_in, server_out);

    ss::sstring filler(cell.message_size, 'x');
    const double bytes_per_sec
      = cell.target_rate_bps > 0 ? static_cast<double>(cell.target_rate_bps) / 8.0 : 0;

    // Warmup phase
    auto warmup_end = bench_clock::now()
                      + std::chrono::seconds(cell.warmup_sec);
    uint64_t warmup_bytes = 0;
    while (bench_clock::now() < warmup_end) {
        co_await client_out.write(filler.data(), cell.message_size);
        co_await client_out.flush();
        auto resp = co_await client_in.read_exactly(cell.message_size);
        warmup_bytes += cell.message_size;

        // Rate limit during warmup too
        if (bytes_per_sec > 0) {
            auto elapsed = std::chrono::duration<double>(
                             bench_clock::now() - (warmup_end - std::chrono::seconds(cell.warmup_sec)))
                             .count();
            double allowed = elapsed * bytes_per_sec;
            if (static_cast<double>(warmup_bytes) >= allowed) {
                double wait = (static_cast<double>(warmup_bytes) - allowed) / bytes_per_sec;
                if (wait > 0.0001) {
                    co_await ss::sleep(
                      std::chrono::microseconds(static_cast<int64_t>(wait * 1e6)));
                }
            }
        }
    }

    // Measurement phase
    std::vector<uint64_t> latencies_us;
    // Reserve for expected message count
    if (bytes_per_sec > 0) {
        auto expected = static_cast<size_t>(
          bytes_per_sec / static_cast<double>(cell.message_size)
          * cell.duration_sec);
        latencies_us.reserve(expected + 1024);
    } else {
        latencies_us.reserve(1'000'000);
    }

    auto r0 = uds_bench::rusage_sample::now();
    auto start = bench_clock::now();
    auto deadline = start + std::chrono::seconds(cell.duration_sec);
    uint64_t total_bytes = 0;
    uint64_t total_msgs = 0;

    while (bench_clock::now() < deadline) {
        // Rate pacing: check if we're ahead of schedule
        if (bytes_per_sec > 0) {
            auto elapsed = std::chrono::duration<double>(
                             bench_clock::now() - start)
                             .count();
            double allowed = elapsed * bytes_per_sec;
            if (static_cast<double>(total_bytes) >= allowed) {
                double wait = (static_cast<double>(total_bytes) - allowed)
                              / bytes_per_sec;
                if (wait > 0.0001) {
                    co_await ss::sleep(std::chrono::microseconds(
                      static_cast<int64_t>(wait * 1e6)));
                }
                continue;
            }
        }

        auto msg_start = bench_clock::now();
        co_await client_out.write(filler.data(), cell.message_size);
        co_await client_out.flush();
        auto resp = co_await client_in.read_exactly(cell.message_size);
        auto msg_end = bench_clock::now();

        latencies_us.push_back(
          static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(
              msg_end - msg_start)
              .count()));
        total_bytes += cell.message_size;
        total_msgs++;
    }

    auto end = bench_clock::now();
    auto r1 = uds_bench::rusage_sample::now();

    co_await client_out.close();
    co_await client_in.close();
    co_await std::move(echo);
    co_await server_out.close();
    co_await server_in.close();
    pair.listener.abort_accept();

    double elapsed
      = std::chrono::duration<double>(end - start).count();
    std::sort(latencies_us.begin(), latencies_us.end());

    cell_result result;
    result.cell = cell;
    result.messages_sent = total_msgs;
    result.bytes_sent = total_bytes;
    result.elapsed_sec = elapsed;
    result.achieved_mbps = elapsed > 0
                             ? static_cast<double>(total_bytes) / (1024.0 * 1024.0 * elapsed)
                             : 0;
    result.achieved_mbits = elapsed > 0
                              ? static_cast<double>(total_bytes) * 8.0 / (1e6 * elapsed)
                              : 0;
    result.cpu_delta = r1 - r0;
    result.lat_p50_us = percentile(latencies_us, 0.50);
    result.lat_p99_us = percentile(latencies_us, 0.99);
    result.lat_max_us = latencies_us.empty() ? 0 : latencies_us.back();
    co_return result;
}

ss::future<> drain_stream(ss::input_stream<char>& in) {
    try {
        while (!in.eof()) {
            auto buf = co_await in.read();
            if (buf.empty()) {
                break;
            }
        }
    } catch (...) {
    }
}

// Unidirectional mode: client blasts, server drains. No per-message
// latency, but shows max achievable throughput at a given rate.
ss::future<cell_result> run_cell_uni(bench_cell cell) {
    auto pair = co_await uds_bench::make_pair(cell.transport, "sustained");
    auto server_in = pair.server_side.input();
    auto server_out = pair.server_side.output();
    auto client_out = pair.client_side.output();

    auto drain = drain_stream(server_in);

    ss::sstring filler(cell.message_size, 'x');
    const double bytes_per_sec
      = cell.target_rate_bps > 0 ? static_cast<double>(cell.target_rate_bps) / 8.0 : 0;

    // Warmup
    auto warmup_start = bench_clock::now();
    auto warmup_end = warmup_start + std::chrono::seconds(cell.warmup_sec);
    uint64_t warmup_bytes = 0;
    while (bench_clock::now() < warmup_end) {
        co_await client_out.write(filler.data(), cell.message_size);
        warmup_bytes += cell.message_size;
        if (warmup_bytes % (cell.message_size * 64) == 0) {
            co_await client_out.flush();
        }
        if (bytes_per_sec > 0) {
            auto elapsed = std::chrono::duration<double>(
                             bench_clock::now() - warmup_start)
                             .count();
            double allowed = elapsed * bytes_per_sec;
            if (static_cast<double>(warmup_bytes) >= allowed) {
                double wait = (static_cast<double>(warmup_bytes) - allowed) / bytes_per_sec;
                if (wait > 0.0001) {
                    co_await ss::sleep(std::chrono::microseconds(
                      static_cast<int64_t>(wait * 1e6)));
                }
            }
        }
    }

    // Measurement
    auto r0 = uds_bench::rusage_sample::now();
    auto start = bench_clock::now();
    auto deadline = start + std::chrono::seconds(cell.duration_sec);
    uint64_t total_bytes = 0;
    uint64_t total_msgs = 0;
    size_t batch_count = 0;

    while (bench_clock::now() < deadline) {
        if (bytes_per_sec > 0) {
            auto elapsed = std::chrono::duration<double>(
                             bench_clock::now() - start)
                             .count();
            double allowed = elapsed * bytes_per_sec;
            if (static_cast<double>(total_bytes) >= allowed) {
                double wait = (static_cast<double>(total_bytes) - allowed)
                              / bytes_per_sec;
                if (wait > 0.0001) {
                    co_await ss::sleep(std::chrono::microseconds(
                      static_cast<int64_t>(wait * 1e6)));
                }
                continue;
            }
        }

        co_await client_out.write(filler.data(), cell.message_size);
        total_bytes += cell.message_size;
        total_msgs++;
        batch_count++;
        if (batch_count >= 64) {
            co_await client_out.flush();
            batch_count = 0;
        }
    }
    co_await client_out.flush();

    auto end = bench_clock::now();
    auto r1 = uds_bench::rusage_sample::now();

    co_await client_out.close();
    co_await std::move(drain);
    co_await server_in.close();
    co_await server_out.close();
    pair.listener.abort_accept();

    double elapsed = std::chrono::duration<double>(end - start).count();

    cell_result result;
    result.cell = cell;
    result.messages_sent = total_msgs;
    result.bytes_sent = total_bytes;
    result.elapsed_sec = elapsed;
    result.achieved_mbps = elapsed > 0
                             ? static_cast<double>(total_bytes) / (1024.0 * 1024.0 * elapsed)
                             : 0;
    result.achieved_mbits = elapsed > 0
                              ? static_cast<double>(total_bytes) * 8.0 / (1e6 * elapsed)
                              : 0;
    result.cpu_delta = r1 - r0;
    co_return result;
}

// Concurrent mode: run N independent request-response connections in
// parallel (simulates N producers to different topics on the same broker).
// Each connection runs at the full target rate; the total offered load is
// N × target_rate. We measure per-connection latency and aggregate
// throughput/CPU so the comparison shows how UDS scales under contention.
//
// All state is heap-allocated in a struct so that stream references passed
// to run_echo() remain valid for the lifetime of the coroutine.
ss::future<cell_result>
run_cell_concurrent(bench_cell cell, uint32_t concurrency) {
    struct conn_state {
        uds_bench::conn_pair pair;
        ss::input_stream<char> server_in;
        ss::output_stream<char> server_out;
        ss::input_stream<char> client_in;
        ss::output_stream<char> client_out;
        ss::future<> echo = ss::make_ready_future<>();
    };

    // Use a deque so that elements are pointer-stable (vector
    // reallocation would invalidate the references held by run_echo).
    std::deque<conn_state> conns;

    for (uint32_t c = 0; c < concurrency; ++c) {
        auto tag = fmt::format("conc-{}", c);
        auto p = co_await uds_bench::make_pair(cell.transport, tag);
        conn_state cs;
        cs.server_in = p.server_side.input();
        cs.server_out = p.server_side.output();
        cs.client_in = p.client_side.input();
        cs.client_out = p.client_side.output();
        cs.pair = std::move(p);
        conns.push_back(std::move(cs));
    }

    // Start echo servers after all conn_states are in the deque (stable
    // addresses). run_echo takes references to server_in/server_out
    // which must remain valid until the echo future completes.
    for (auto& cs : conns) {
        cs.echo = uds_bench::run_echo(cs.server_in, cs.server_out);
    }

    ss::sstring filler(cell.message_size, 'x');
    const double bytes_per_sec
      = cell.target_rate_bps > 0
          ? static_cast<double>(cell.target_rate_bps) / 8.0
          : 0;

    // Warmup
    auto warmup_end = bench_clock::now()
                      + std::chrono::seconds(cell.warmup_sec);
    while (bench_clock::now() < warmup_end) {
        for (auto& cs : conns) {
            co_await cs.client_out.write(filler.data(), cell.message_size);
            co_await cs.client_out.flush();
            auto resp = co_await cs.client_in.read_exactly(
              cell.message_size);
        }
    }

    // Measurement: round-robin across connections to keep things fair
    // on a single shard. Each connection is rate-limited independently.
    std::vector<uint64_t> all_latencies;
    all_latencies.reserve(1'000'000);
    std::vector<uint64_t> per_conn_bytes(concurrency, 0);

    auto r0 = uds_bench::rusage_sample::now();
    auto start = bench_clock::now();
    auto deadline = start + std::chrono::seconds(cell.duration_sec);
    uint64_t total_bytes = 0;
    uint64_t total_msgs = 0;

    while (bench_clock::now() < deadline) {
        for (uint32_t c = 0; c < concurrency; ++c) {
            // Per-connection rate pacing
            if (bytes_per_sec > 0) {
                auto elapsed = std::chrono::duration<double>(
                                 bench_clock::now() - start)
                                 .count();
                double allowed = elapsed * bytes_per_sec;
                if (static_cast<double>(per_conn_bytes[c]) >= allowed) {
                    continue;
                }
            }

            auto msg_start = bench_clock::now();
            co_await conns[c].client_out.write(
              filler.data(), cell.message_size);
            co_await conns[c].client_out.flush();
            auto resp = co_await conns[c].client_in.read_exactly(
              cell.message_size);
            auto msg_end = bench_clock::now();

            all_latencies.push_back(static_cast<uint64_t>(
              std::chrono::duration_cast<std::chrono::microseconds>(
                msg_end - msg_start)
                .count()));
            per_conn_bytes[c] += cell.message_size;
            total_bytes += cell.message_size;
            total_msgs++;
        }

        // If all connections are ahead of schedule, sleep briefly
        if (bytes_per_sec > 0) {
            bool all_ahead = true;
            auto elapsed
              = std::chrono::duration<double>(bench_clock::now() - start)
                  .count();
            double allowed = elapsed * bytes_per_sec;
            for (uint32_t c = 0; c < concurrency; ++c) {
                if (static_cast<double>(per_conn_bytes[c]) < allowed) {
                    all_ahead = false;
                    break;
                }
            }
            if (all_ahead) {
                co_await ss::sleep(std::chrono::microseconds(100));
            }
        }
    }

    auto end = bench_clock::now();
    auto r1 = uds_bench::rusage_sample::now();

    // Cleanup: close client side first, then wait for echo to drain,
    // then close server side.
    for (auto& cs : conns) {
        co_await cs.client_out.close();
        co_await cs.client_in.close();
    }
    for (auto& cs : conns) {
        co_await std::move(cs.echo);
        co_await cs.server_out.close();
        co_await cs.server_in.close();
        cs.pair.listener.abort_accept();
    }

    double elapsed
      = std::chrono::duration<double>(end - start).count();
    std::sort(all_latencies.begin(), all_latencies.end());

    cell_result result;
    result.cell = cell;
    result.messages_sent = total_msgs;
    result.bytes_sent = total_bytes;
    result.elapsed_sec = elapsed;
    result.achieved_mbps
      = elapsed > 0
          ? static_cast<double>(total_bytes) / (1024.0 * 1024.0 * elapsed)
          : 0;
    result.achieved_mbits
      = elapsed > 0
          ? static_cast<double>(total_bytes) * 8.0 / (1e6 * elapsed)
          : 0;
    result.cpu_delta = r1 - r0;
    result.lat_p50_us = percentile(all_latencies, 0.50);
    result.lat_p99_us = percentile(all_latencies, 0.99);
    result.lat_max_us = all_latencies.empty() ? 0 : all_latencies.back();
    co_return result;
}

void print_results(
  const std::vector<cell_result>& results,
  bool is_rr,
  const std::string& mode_label) {
    fmt::print("\n{:=<90}\n", "");
    fmt::print(" UDS Sustained Throughput Benchmark — {}\n", mode_label);
    fmt::print("{:=<90}\n\n", "");

    // Header
    if (is_rr) {
        fmt::print(
          "{:<12} {:>8} {:>12} {:>10} {:>10} {:>8} {:>8} {:>8}\n",
          "Transport",
          "MsgSize",
          "TargetRate",
          "MB/s",
          "Mbit/s",
          "p50(us)",
          "p99(us)",
          "max(us)");
        fmt::print("{:-<90}\n", "");
    } else {
        fmt::print(
          "{:<12} {:>8} {:>12} {:>10} {:>10} {:>10} {:>10}\n",
          "Transport",
          "MsgSize",
          "TargetRate",
          "MB/s",
          "Mbit/s",
          "Messages",
          "CPU(ms)");
        fmt::print("{:-<78}\n", "");
    }

    for (const auto& r : results) {
        auto tname = uds_bench::transport_name(r.cell.transport);
        auto sz = format_size(r.cell.message_size);
        auto rate = format_rate(r.cell.target_rate_bps);
        auto cpu_ms = (r.cpu_delta.user_us + r.cpu_delta.sys_us) / 1000;

        if (is_rr) {
            fmt::print(
              "{:<12} {:>8} {:>12} {:>10.2f} {:>10.2f} {:>8} {:>8} {:>8}\n",
              tname,
              sz,
              rate,
              r.achieved_mbps,
              r.achieved_mbits,
              r.lat_p50_us,
              r.lat_p99_us,
              r.lat_max_us);
        } else {
            fmt::print(
              "{:<12} {:>8} {:>12} {:>10.2f} {:>10.2f} {:>10} {:>10}\n",
              tname,
              sz,
              rate,
              r.achieved_mbps,
              r.achieved_mbits,
              r.messages_sent,
              cpu_ms);
        }
    }

    // Comparison summary: group by (size, rate), compare transports
    fmt::print("\n{:-<90}\n", "");
    fmt::print(" UDS vs TCP Comparison\n");
    fmt::print("{:-<90}\n", "");

    if (is_rr) {
        fmt::print(
          "{:>8} {:>12} {:>14} {:>14} {:>14}\n",
          "MsgSize",
          "TargetRate",
          "Throughput",
          "p99 Latency",
          "CPU");
        fmt::print("{:-<68}\n", "");
    } else {
        fmt::print(
          "{:>8} {:>12} {:>14} {:>14}\n",
          "MsgSize",
          "TargetRate",
          "Throughput",
          "CPU");
        fmt::print("{:-<52}\n", "");
    }

    // Find TCP/UDS pairs
    for (size_t i = 0; i < results.size(); ++i) {
        if (results[i].cell.transport != uds_bench::transport::uds) {
            continue;
        }
        // Find matching TCP result
        for (size_t j = 0; j < results.size(); ++j) {
            if (
              j == i
              || results[j].cell.transport == uds_bench::transport::uds) {
                continue;
            }
            if (
              results[j].cell.message_size != results[i].cell.message_size
              || results[j].cell.target_rate_bps
                   != results[i].cell.target_rate_bps) {
                continue;
            }
            auto sz = format_size(results[i].cell.message_size);
            auto rate = format_rate(results[i].cell.target_rate_bps);
            double tput_ratio
              = results[j].achieved_mbps > 0
                  ? results[i].achieved_mbps / results[j].achieved_mbps
                  : 0;
            auto tcp_cpu = results[j].cpu_delta.user_us
                           + results[j].cpu_delta.sys_us;
            auto uds_cpu = results[i].cpu_delta.user_us
                           + results[i].cpu_delta.sys_us;
            double cpu_ratio = tcp_cpu > 0
                                 ? static_cast<double>(uds_cpu)
                                     / static_cast<double>(tcp_cpu)
                                 : 0;

            if (is_rr) {
                double lat_ratio
                  = results[j].lat_p99_us > 0
                      ? static_cast<double>(results[i].lat_p99_us)
                          / static_cast<double>(results[j].lat_p99_us)
                      : 0;
                fmt::print(
                  "{:>8} {:>12} {:>13.2f}x {:>13.2f}x {:>13.2f}x\n",
                  sz,
                  rate,
                  tput_ratio,
                  lat_ratio,
                  cpu_ratio);
            } else {
                fmt::print(
                  "{:>8} {:>12} {:>13.2f}x {:>13.2f}x\n",
                  sz,
                  rate,
                  tput_ratio,
                  cpu_ratio);
            }
            break;
        }
    }
    fmt::print("\n");
}

std::vector<bench_cell> build_matrix(const bench_config& cfg) {
    auto sizes = parse_csv_sizes(cfg.sizes_str);
    auto rates = parse_csv_rates(cfg.rates_str);
    auto transports = parse_transports(cfg.transports_str);

    std::vector<bench_cell> cells;
    for (auto sz : sizes) {
        for (auto rate : rates) {
            for (auto t : transports) {
                cells.push_back(bench_cell{
                  .transport = t,
                  .message_size = sz,
                  .target_rate_bps = rate,
                  .duration_sec = cfg.duration_sec,
                  .warmup_sec = cfg.warmup_sec,
                });
            }
        }
    }
    return cells;
}

ss::future<> run_all(bench_config cfg) {
    auto matrix = build_matrix(cfg);
    bool is_rr = cfg.mode == "rr" || cfg.mode == "concurrent";
    bool is_concurrent = cfg.mode == "concurrent";

    std::string mode_label;
    if (cfg.mode == "rr") {
        mode_label = "request-response";
    } else if (cfg.mode == "uni") {
        mode_label = "unidirectional";
    } else if (cfg.mode == "concurrent") {
        mode_label = fmt::format(
          "concurrent ({}x connections)", cfg.concurrency);
    } else {
        mode_label = cfg.mode;
    }

    fmt::print(
      "Running {} cells, {} mode, {}s per cell ({}s warmup)\n",
      matrix.size(),
      mode_label,
      cfg.duration_sec,
      cfg.warmup_sec);

    std::vector<cell_result> results;
    results.reserve(matrix.size());

    for (size_t i = 0; i < matrix.size(); ++i) {
        const auto& cell = matrix[i];
        fmt::print(
          "[{}/{}] {} {} @ {} ...\n",
          i + 1,
          matrix.size(),
          uds_bench::transport_name(cell.transport),
          format_size(cell.message_size),
          format_rate(cell.target_rate_bps));

        cell_result result;
        if (is_concurrent) {
            result = co_await run_cell_concurrent(cell, cfg.concurrency);
        } else if (cfg.mode == "rr") {
            result = co_await run_cell_rr(cell);
        } else {
            result = co_await run_cell_uni(cell);
        }

        fmt::print(
          "        -> {:.2f} MB/s ({:.2f} Mbit/s), {} msgs",
          result.achieved_mbps,
          result.achieved_mbits,
          result.messages_sent);
        if (is_rr) {
            fmt::print(
              ", p50={}us p99={}us",
              result.lat_p50_us,
              result.lat_p99_us);
        }
        fmt::print("\n");

        results.push_back(std::move(result));
    }

    print_results(results, is_rr, mode_label);
}

} // namespace

int main(int ac, char* av[]) {
    ss::app_template::config seastar_cfg;
    seastar_cfg.auto_handle_sigint_sigterm = false;
    ss::app_template app(seastar_cfg);

    bench_config cfg;

    app.add_options()(
      "duration",
      po::value<uint32_t>(&cfg.duration_sec)
        ->default_value(cfg.duration_sec),
      "Measurement duration per cell in seconds")(
      "warmup",
      po::value<uint32_t>(&cfg.warmup_sec)->default_value(cfg.warmup_sec),
      "Warmup duration per cell in seconds")(
      "sizes",
      po::value<std::string>(&cfg.sizes_str)->default_value(cfg.sizes_str),
      "Comma-separated message sizes in bytes")(
      "rates",
      po::value<std::string>(&cfg.rates_str)->default_value(cfg.rates_str),
      "Comma-separated target rates in bits/sec (0=unlimited)")(
      "transports",
      po::value<std::string>(&cfg.transports_str)
        ->default_value(cfg.transports_str),
      "Comma-separated transports: tcp_nagle,tcp_nodelay,uds")(
      "mode",
      po::value<std::string>(&cfg.mode)->default_value(cfg.mode),
      "Benchmark mode: rr (request-response), uni (unidirectional), "
      "or concurrent (N parallel connections)")(
      "concurrency",
      po::value<uint32_t>(&cfg.concurrency)
        ->default_value(cfg.concurrency),
      "Number of parallel connections in concurrent mode");

    return app.run(ac, av, [&cfg]() mutable {
        return run_all(std::move(cfg)).then([] { return 0; });
    });
}
