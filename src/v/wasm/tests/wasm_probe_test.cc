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

#include "base/type_traits.h"
#include "base/units.h"
#include "gmock/gmock.h"
#include "metrics/metrics.h"
#include "wasm/logger.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_api.hh>
#include <seastar/core/smp.hh>

#include <absl/algorithm/container.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <wasm/engine_probe.h>

#include <optional>
#include <type_traits>

namespace wasm {

namespace {

template<typename T>
std::optional<T> find_metric_value(
  std::string_view metric_name, const ss::sstring& function_name) {
    auto metrics = ss::metrics::impl::get_value_map(
      metrics::public_metrics_handle);
    auto metrics_it = metrics.find(ss::sstring(metric_name));
    if (metrics_it == metrics.end()) {
        return std::nullopt;
    }
    seastar::metrics::impl::metric_family family = metrics_it->second;
    auto family_it = family.find(
      {{"function_name", function_name},
       {ss::metrics::shard_label.name(), std::to_string(ss::this_shard_id())}});
    if (family_it == family.end()) {
        return std::nullopt;
    }
    ss::metrics::impl::metric_function metric_fn
      = family_it->second->get_function();
    seastar::metrics::impl::metric_value sample = metric_fn();
    if constexpr (std::is_same_v<double, T>) {
        return sample.d();
    } else if constexpr (std::is_same_v<uint64_t, T>) {
        return sample.ui();
    } else {
        static_assert(base::unsupported_type<T>::value, "unsupported type");
    }
}

std::optional<uint64_t>
reported_memory_usage(const ss::sstring& function_name) {
    return find_metric_value<uint64_t>(
      "wasm_engine_memory_usage", function_name);
}

std::optional<uint64_t> reported_max_memory(const ss::sstring& function_name) {
    return find_metric_value<uint64_t>("wasm_engine_max_memory", function_name);
}

std::optional<double> reported_cpu_time(const ss::sstring& function_name) {
    return find_metric_value<double>(
      "wasm_engine_cpu_seconds_total", function_name);
}

} // namespace

using ::testing::Optional;

// NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)

TEST(EngineProbeTest, MemoryReportsDeltas) {
    engine_probe_cache cache;
    std::optional<engine_probe> foobar_probe = cache.make_probe("foobar");
    EXPECT_THAT(reported_max_memory("foobar"), Optional(0));
    EXPECT_THAT(reported_memory_usage("foobar"), Optional(0));
    foobar_probe->report_memory_usage(1_KiB);
    foobar_probe->report_max_memory(10_KiB);
    EXPECT_THAT(reported_memory_usage("foobar"), Optional(1_KiB));
    EXPECT_THAT(reported_max_memory("foobar"), Optional(10_KiB));
    foobar_probe->report_memory_usage(2_KiB);
    foobar_probe->report_max_memory(5_KiB);
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
    p1->report_memory_usage(5_KiB);
    p2->report_memory_usage(2_KiB);
    p3->report_memory_usage(10_KiB);
    EXPECT_THAT(reported_memory_usage("foobar"), 17_KiB);
    p3->report_memory_usage(9_KiB);
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
    p1->increment_cpu_time(500ms);
    p2->increment_cpu_time(250ms);
    EXPECT_THAT(reported_cpu_time("foobar"), Optional(DoubleNear(0.75)));
    p2->increment_cpu_time(300ms);
    EXPECT_THAT(reported_cpu_time("foobar"), Optional(DoubleNear(1.05)));
    p1 = std::nullopt;
    EXPECT_THAT(reported_cpu_time("foobar"), Optional(DoubleNear(1.05)));
    p2 = std::nullopt;
    EXPECT_THAT(reported_memory_usage("foobar"), std::nullopt);
}

// NOLINTEND(cppcoreguidelines-avoid-magic-numbers)

} // namespace wasm
