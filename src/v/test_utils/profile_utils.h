/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"

#include <seastar/core/seastar.hh>

#include <chrono>
#include <filesystem>
#include <string_view>

namespace test_utils {
/**
 * Profile Options
 */
struct profile_options {
    /// For a given section only collect a one profile regardless of how many
    /// times the section runs.
    bool once{false};
    /// The sample period for the CPU profiler.
    std::chrono::milliseconds profile_sample_period{5};
    /// The directory the profiles should be written to.
    std::filesystem::path output_dir{std::filesystem::current_path()};
};

/**
 * Enables profilers for the duration that \p section runs. Writes resulting
 * profiles out to files named using \p section_name.
 *
 * This function is intended to only be used in tests.
 *
 * Usage
 * =====
 *
 * Consider the following section of code from a microbenchmark;
 *
 *     perf_tests::start_measuring_time();
 *     auto res = co_await reader.consume(
 *          std::move(consumer), model::no_timeout);
 *     perf_tests::stop_measuring_time();
 *
 * To get profiles for the section `profile_section` would be used;
 *
 *     co_await profile_section("example_section_name", [&]() -> ss::future<> {
 *          perf_tests::start_measuring_time();
 *          auto res = co_await reader.consume(
 *              std::move(consumer), model::no_timeout);
 *          perf_tests::stop_measuring_time();
 *     });
 *
 * This will result in the following files in the current working directory;
 *     - example_section_name_cpu_profile_N.json
 *     - example_section_name_memory_profile_N.json
 *
 * Where `N` indicates which run of the section the profile is for, with 0
 * indicating the first run.
 *
 * Note that this function will not override any existing files.
 */
ss::future<> profile_section(
  std::string_view section_name,
  std::function<ss::future<>()> section,
  profile_options opts = {});

} // namespace test_utils
