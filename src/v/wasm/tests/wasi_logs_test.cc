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

#include <seastar/util/log.hh>

#include <chrono>
#define BOOST_TEST_MODULE wasi_logs
#include "wasm/wasi.h"

#include <absl/strings/str_split.h>
#include <boost/test/unit_test.hpp>

namespace {

void run_test_case(std::string_view input, std::string_view want) {
    seastar::apply_logging_settings({
      .logger_levels = {},
      .default_level = ss::log_level::trace,
      .stdout_enabled = true,
      .syslog_enabled = false,
      .with_color = false,
      // Disable timestamps for easier testing
      .stdout_timestamp_style = ss::logger_timestamp_style::none,
    });
    constexpr std::array<uint32_t, 5> buf_sizes = {1, 8, 10, 32, 10000};
    for (uint32_t size : buf_sizes) {
        ss::logger logger("LOGGER_NAME");
        size_t amt = 0;
        std::stringstream ss;
        logger.set_ostream(ss);
        wasm::wasi::log_writer w = wasm::wasi::log_writer::make_for_stdout(
          "XFORM_NAME", &logger);
        for (auto chunk : absl::StrSplit(input, absl::ByLength(size))) {
            amt += w.write(chunk);
        }
        amt += w.flush();
        BOOST_CHECK_EQUAL(input.size(), amt);
        BOOST_CHECK_EQUAL(ss.str(), want);
    }
}
} // namespace

BOOST_AUTO_TEST_CASE(wasi_logs) {
    // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define TIME ""
    // empty string
    run_test_case("", "");
    // single line
    run_test_case(
      "hello world", "INFO   LOGGER_NAME - XFORM_NAME - hello world\n");
    // multiple lines
    run_test_case(
      "hello\n"
      "world\n"
      "foo\n"
      "bar",
      "INFO   LOGGER_NAME - XFORM_NAME - hello\n"
      "INFO   LOGGER_NAME - XFORM_NAME - world\n"
      "INFO   LOGGER_NAME - XFORM_NAME - foo\n"
      "INFO   LOGGER_NAME - XFORM_NAME - bar\n");
    // trailing newline
    run_test_case(
      "hello\n"
      "world\n",
      "INFO   LOGGER_NAME - XFORM_NAME - hello\n"
      "INFO   LOGGER_NAME - XFORM_NAME - world\n");
}
