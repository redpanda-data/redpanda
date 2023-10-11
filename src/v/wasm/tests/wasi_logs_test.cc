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

#include "units.h"
#include "wasm/wasi.h"

#include <seastar/util/log.hh>

#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>
#include <gtest/gtest.h>

#include <chrono>

struct test_param {
    std::string input;
    std::string want;
};

class WasiLogTest : public testing::TestWithParam<test_param> {};

TEST_P(WasiLogTest, ChunksLogsCorrectly) {
    auto param = GetParam();
    auto input = param.input;
    auto want = param.want;
    seastar::apply_logging_settings({
      .stdout_enabled = true,
      .syslog_enabled = false,
      .with_color = false,
      // Disable timestamps for easier testing
      .stdout_timestamp_style = ss::logger_timestamp_style::none,
    });
    constexpr std::array<uint32_t, 5> buf_sizes = {1, 8, 10, 32, 10000};
    for (uint32_t size : buf_sizes) {
        ss::logger logger("LOGGER_NAME");
        logger.set_level(ss::log_level::trace);
        size_t amt = 0;
        std::stringstream ss;
        logger.set_ostream(ss);
        wasm::wasi::log_writer w = wasm::wasi::log_writer::make_for_stdout(
          "XFORM_NAME", &logger);
        for (auto chunk : absl::StrSplit(input, absl::ByLength(size))) {
            amt += w.write(chunk);
        }
        amt += w.flush();
        ASSERT_EQ(input.size(), amt);
        ASSERT_EQ(ss.str(), want);
    }
}

INSTANTIATE_TEST_SUITE_P(
  CorrectlyFormatsLogs,
  WasiLogTest,
  testing::Values(
    test_param{
      .input = "",
      .want = "",
    },
    test_param{
      .input = "hello world",
      .want = "INFO   [shard 0:main] LOGGER_NAME - XFORM_NAME - hello world\n",
    },
    test_param{
      .input = "hello world",
      .want = "INFO   [shard 0:main] LOGGER_NAME - XFORM_NAME - hello world\n",
    },
    test_param{
      .input = "hello\n"
               "world\n"
               "foo\n"
               "bar",
      .want = "INFO   [shard 0:main] LOGGER_NAME - XFORM_NAME - hello\n"
              "INFO   [shard 0:main] LOGGER_NAME - XFORM_NAME - world\n"
              "INFO   [shard 0:main] LOGGER_NAME - XFORM_NAME - foo\n"
              "INFO   [shard 0:main] LOGGER_NAME - XFORM_NAME - bar\n",
    },
    test_param{
      .input = "hello\n"
               "world\n",
      .want = "INFO   [shard 0:main] LOGGER_NAME - XFORM_NAME - hello\n"
              "INFO   [shard 0:main] LOGGER_NAME - XFORM_NAME - world\n",
    },
    test_param{
      .input = std::string(8_KiB, 'a'),
      .want = absl::StrCat(
        "INFO   [shard 0:main] LOGGER_NAME - XFORM_NAME - ",
        std::string(2_KiB, 'a'),
        "\n")},
    test_param{
      .input = "The quic\b\b\b\b\b\bk brown "
               "fo\u0007\u0007\u0007\u0007\u0007\u0007\u0007\u0007\u0007\u0007"
               "\u0007x... [Beeeep]\n",
      .want = "INFO   [shard 0:main] LOGGER_NAME - XFORM_NAME - "
              "The quic\\x08\\x08\\x08\\x08\\x08\\x08k brown "
              "fo\\x07\\x07\\x07\\x07\\x07\\x07\\x07\\x07\\x07\\x07"
              "\\x07x... [Beeeep]\n"},
    test_param{
      .input = "invalid utf8: \xF0\xA4\xAD\x7F\n",
      .want = "INFO   [shard 0:main] LOGGER_NAME - XFORM_NAME - invalid utf8: "
              "\\xf0\\xa4\\xad\\x7f\n"}));
