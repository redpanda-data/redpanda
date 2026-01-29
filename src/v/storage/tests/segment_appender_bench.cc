// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "ssx/future-util.h"
#include "storage/segment_appender.h"
#include "test_utils/tmpbuf_file.h"

#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/reactor.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/util/defer.hh>

struct appender_fixture {
    ss::future<std::unique_ptr<storage::segment_appender>>
    make_appender(tmpbuf_file::store_t& store) {
        auto file = ss::file(ss::make_shared<tmpbuf_file>(store));

        storage::segment_appender::options opts(
          std::nullopt, _resources, nullptr);

        co_return std::make_unique<storage::segment_appender>(
          std::move(file), opts);
    }
    template<size_t WriteSize>
    ss::future<int>
    run_write_bench(size_t total_bytes_written, bool do_flush = false) {
        /**
         * Prepare appender
         */
        tmpbuf_file::store_t store;
        ss::gate gate;
        auto appender = co_await make_appender(store);

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
        co_return iterations;
    }

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
