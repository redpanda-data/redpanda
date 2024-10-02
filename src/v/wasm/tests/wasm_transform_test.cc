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

#include "wasm/errc.h"
#include "wasm/tests/wasm_fixture.h"

#include <seastar/core/internal/cpu_profiler.hh>
#include <seastar/core/reactor.hh>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

using namespace std::chrono_literals;

TEST_F(WasmTestFixture, IdentityFunction) {
    load_wasm("identity");
    auto batch = make_tiny_batch();
    auto transformed = transform(batch);
    ASSERT_EQ(transformed.copy_records(), batch.copy_records());
}

using ::testing::ElementsAre;

TEST_F(WasmTestFixture, LogsAreEmitted) {
    load_wasm("dynamic");
    ss::sstring msg = "foobar";
    auto value = execute_command("print", msg);
    auto bytes = iobuf_to_bytes(value);
    ss::sstring expected;
    // NOLINTNEXTLINE(*-reinterpret-cast)
    expected.append(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    EXPECT_THAT(log_lines(), ElementsAre(expected));
}

TEST_F(WasmTestFixture, WorksWithCpuProfiler) {
    bool original_enabled = ss::engine().get_cpu_profiler_enabled();
    std::chrono::nanoseconds original_period
      = ss::engine().get_cpu_profiler_period();
    ss::engine().set_cpu_profiler_enabled(true);
    ss::engine().set_cpu_profiler_period(100us);
    load_wasm("dynamic");
    EXPECT_THROW(execute_command("loop", 0), wasm::wasm_exception);
    ss::engine().set_cpu_profiler_enabled(original_enabled);
    ss::engine().set_cpu_profiler_period(original_period);
    std::vector<ss::cpu_profiler_trace> traces;
    ss::engine().profiler_results(traces);
    for (const auto& t : traces) {
        std::cout << t.user_backtrace << "\n";
    }
}
