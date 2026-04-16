/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "serde/json/detail/string.h"

#include <seastar/testing/perf_tests.hh>

#include <utility>

using namespace serde::json::detail;

constexpr std::string_view plain_seed
  = R"(0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ)";

constexpr std::string_view unicode_heavy_seed
  = R"(\/\\\"\uCAFE\uBABE\uAB98\uFCDE\ubcda\uef4A\b\f\n\r\t`1~!@#$%^&*()_+-=[]{}|;:',./<>?)";

void run_bench(std::string_view seed) {
    std::vector<ss::temporary_buffer<char>> buffers;

    // Generate ~1MiB of data.
    constexpr size_t test_size = 1024 * 1024;

    {
        size_t buffers_count = test_size / seed.size();
        buffers.reserve(buffers_count);
        for (size_t i = 0; i < buffers_count; ++i) {
            buffers.emplace_back(seed.data(), seed.size());
        }

        // Start and end of JSON string.
        *buffers[0].get_write() = '"';
        *(buffers[buffers_count - 1].get_write()
          + buffers[buffers_count - 1].size() - 1) = '"';
    }

    string_parser p;

    string_parser::result err{};

    perf_tests::start_measuring_time();

    for (size_t i = 0; i < buffers.size() - 1; ++i) {
        p.advance(buffers[i], err);
        vassert(
          err == string_parser::result::need_more_data,
          "got {}",
          std::to_underlying(err));
    }

    p.advance(buffers[buffers.size() - 1], err);
    vassert(
      err == string_parser::result::done, "got {}", std::to_underlying(err));

    perf_tests::stop_measuring_time();

    perf_tests::do_not_optimize(std::move(p).value());
}

PERF_TEST(string_parser, 1MiB_plain) { run_bench(plain_seed); }
PERF_TEST(string_parser, 1MiB_unicode) { run_bench(unicode_heavy_seed); }
