// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "ssx/future-util.h"
#include "storage/chunk_cache.h"
#include "storage/segment_appender.h"

#include <seastar/core/future.hh>
#include <seastar/core/seastar.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/util/tmp_file.hh>

#include <filesystem>

struct appender_fixture {
    ss::future<std::unique_ptr<storage::segment_appender>>
    make_appender(const ss::sstring& path) {
        auto file = co_await ss::open_file_dma(
          path,
          ss::open_flags::rw | ss::open_flags::create
            | ss::open_flags::truncate,
          ss::file_open_options{});

        storage::segment_appender::options opts(
          std::nullopt, _resources, _stats);

        co_return std::make_unique<storage::segment_appender>(
          std::move(file), opts);
    }
    template<size_t WriteSize>
    ss::future<int>
    run_write_bench(size_t total_bytes_written, bool do_flush = false) {
        /**
         * Prepare appender
         */
        ss::gate gate;
        co_await _resources.start();
        // a unique directory per run: several of these may be running at once
        auto dir = co_await ss::make_tmp_dir(
          std::filesystem::path(".") / "segment_appender_bench-XXXX");
        const ss::sstring path = (dir.get_path() / "appender.log").native();
        auto appender = co_await make_appender(path);

        static constexpr std::array<char, WriteSize> write_buf = [] {
            std::array<char, WriteSize> buf{};
            buf.fill('x');
            return buf;
        }();

        perf_tests::start_measuring_time();
        const auto iterations = total_bytes_written / WriteSize;

        for (size_t i = 0; i < iterations; ++i) {
            co_await appender->append(write_buf.data(), WriteSize);
            if (do_flush) {
                ssx::spawn_with_gate(gate, [&] { return appender->flush(); });
            }
        }
        co_await appender->flush();

        perf_tests::stop_measuring_time();

        co_await gate.close();
        co_await appender->close();
        co_await _resources.stop();
        co_await ss::remove_file(path);
        co_await dir.remove();

        /*
         * Guard against the benchmark quietly going back to measuring nothing.
         * Flushing while appends smaller than a chunk accumulate dispatches
         * several writes for one head, which is what write merging needs, and
         * appends that leave the head on a partial page are what the chunk
         * remainder copy needs.
         */
        static constexpr size_t alignment
          = storage::internal::chunk_cache::alignment();
        if (do_flush && WriteSize < _resources.chunks().chunk_size()) {
            vassert(
              _stats->merged_writes > 0,
              "benchmark merged no writes, the dispatched write path is not "
              "being exercised: {}",
              *_stats);
        }
        if (do_flush && WriteSize % alignment != 0) {
            vassert(
              _stats->bytes_copied_in_chunk_remainder > 0,
              "benchmark copied no chunk remainder, the copy path is not being "
              "exercised: {}",
              *_stats);
        }
        co_return iterations;
    }

    ~appender_fixture() {
        // which appender paths the workload actually reached
        fmt::print(
          "[appender] appends={} dma_writes={} merged={} split={} "
          "bytes_copied_in_chunk_remainder={}\n",
          _stats->appends,
          _stats->writes_completed,
          _stats->merged_writes,
          _stats->split_writes,
          _stats->bytes_copied_in_chunk_remainder);
    }

    ss::lw_shared_ptr<storage::segment_appender::stats> _stats
      = ss::make_lw_shared<storage::segment_appender::stats>();
    storage::storage_resources _resources;
};

// 1 byte writes
PERF_TEST_CN(appender_fixture, 1_byte_writes_no_flush) {
    co_return co_await run_write_bench<1>(2_MiB, true);
}

PERF_TEST_CN(appender_fixture, 1_byte_writes_flush) {
    co_return co_await run_write_bench<1>(2_MiB, true);
}

// Unaligned writes
PERF_TEST_CN(appender_fixture, 97_byte_writes_no_flush) {
    co_return co_await run_write_bench<97>(2_MiB, false);
}
PERF_TEST_CN(appender_fixture, 97_byte_writes_flush) {
    co_return co_await run_write_bench<97>(2_MiB, true);
}

// Page size writes
PERF_TEST_CN(appender_fixture, 4096_byte_writes_no_flush) {
    co_return co_await run_write_bench<4096>(5_MiB, false);
}

PERF_TEST_CN(appender_fixture, 4096_byte_writes_flush) {
    co_return co_await run_write_bench<4096>(5_MiB, true);
}

// Large writes
PERF_TEST_CN(appender_fixture, 128_KiB_writes_no_flush) {
    co_return co_await run_write_bench<128_KiB>(10_MiB, false);
}

PERF_TEST_CN(appender_fixture, 128_KiB_writes_flush) {
    co_return co_await run_write_bench<128_KiB>(10_MiB, true);
}
