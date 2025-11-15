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

#include "base/units.h"
#include "metrics/metrics.h"
#include "test_utils/metrics.h"
#include "wasm/logger.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <wasm/engine_probe.h>

#include <optional>

namespace wasm {

namespace {

std::optional<uint64_t>
reported_memory_usage(const ss::sstring& function_name) {
    return test_utils::find_metric_value<uint64_t>(
      "wasm_engine_memory_usage",
      metrics::public_metrics_handle,
      {{"function_name", function_name}});
}

std::optional<uint64_t> reported_max_memory(const ss::sstring& function_name) {
    return test_utils::find_metric_value<uint64_t>(
      "wasm_engine_max_memory",
      metrics::public_metrics_handle,
      {{"function_name", function_name}});
}

std::optional<double> reported_cpu_time(const ss::sstring& function_name) {
    return test_utils::find_metric_value<double>(
      "wasm_engine_cpu_seconds_total",
      metrics::public_metrics_handle,
      {{"function_name", function_name}});
}

} // namespace

using ::testing::Optional;

// NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)

TEST(EngineProbeTest, MemoryReportsDeltas) {
    engine_probe_cache cache;
    std::optional<engine_probe> foobar_probe = cache.make_probe("foobar");
    EXPECT_THAT(reported_max_memory("foobar"), Optional(0));
    EXPECT_THAT(reported_memory_usage("foobar"), Optional(0));
    foobar_probe.value().report_memory_usage(1_KiB);
    foobar_probe.value().report_max_memory(10_KiB);
    EXPECT_THAT(reported_memory_usage("foobar"), Optional(1_KiB));
    EXPECT_THAT(reported_max_memory("foobar"), Optional(10_KiB));
    foobar_probe.value().report_memory_usage(2_KiB);
    foobar_probe.value().report_max_memory(5_KiB);
    EXPECT_THAT(reported_memory_usage("foobar"), Optional(2_KiB));
    EXPECT_THAT(reported_max_memory("foobar"), Optional(5_KiB));
    foobar_probe = std::nullopt;
    EXPECT_THAT(reported_max_memory("foobar"), std::nullopt);
    EXPECT_THAT(reported_memory_usage("foobar"), std::nullopt);
}

TEST(EngineProbeTest, MemorySumsAcrossEngines) {
    engine_probe_cache cache;
    std::optional<engine_probe> p1 = cache.make_probe("foobar");
    std::optional<engine_probe> p2 = cache.make_probe("foobar");
    std::optional<engine_probe> p3 = cache.make_probe("foobar");
    EXPECT_THAT(reported_memory_usage("foobar"), Optional(0));
    p1.value().report_memory_usage(5_KiB);
    p2.value().report_memory_usage(2_KiB);
    p3.value().report_memory_usage(10_KiB);
    EXPECT_THAT(reported_memory_usage("foobar"), 17_KiB);
    p3.value().report_memory_usage(9_KiB);
    EXPECT_THAT(reported_memory_usage("foobar"), 16_KiB);
    p1 = std::nullopt;
    EXPECT_THAT(reported_memory_usage("foobar"), 11_KiB);
    p2 = std::nullopt;
    EXPECT_THAT(reported_memory_usage("foobar"), 9_KiB);
    p3 = std::nullopt;
    EXPECT_THAT(reported_memory_usage("foobar"), std::nullopt);
}

using namespace std::chrono;

auto DoubleNear(double expected) {
    return ::testing::DoubleNear(expected, 0.01);
}

TEST(EngineProbeTest, CpuSumsAcrossEngines) {
    engine_probe_cache cache;
    std::optional<engine_probe> p1 = cache.make_probe("foobar");
    std::optional<engine_probe> p2 = cache.make_probe("foobar");
    EXPECT_THAT(reported_cpu_time("foobar"), Optional(0));
    p1.value().increment_cpu_time(500ms);
    p2.value().increment_cpu_time(250ms);
    EXPECT_THAT(reported_cpu_time("foobar"), Optional(DoubleNear(0.75)));
    p2.value().increment_cpu_time(300ms);
    EXPECT_THAT(reported_cpu_time("foobar"), Optional(DoubleNear(1.05)));
    p1 = std::nullopt;
    EXPECT_THAT(reported_cpu_time("foobar"), Optional(DoubleNear(1.05)));
    p2 = std::nullopt;
    EXPECT_THAT(reported_memory_usage("foobar"), std::nullopt);
}

// NOLINTEND(cppcoreguidelines-avoid-magic-numbers)

} // namespace wasm
