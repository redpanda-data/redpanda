#pragma once

#undef SEASTAR_TESTING_MAIN

#include "random/generators.h"

#include <seastar/testing/entry_point.hh>
#include <seastar/util/log.hh>

#include <fmt/core.h>

constexpr size_t kb = 1024;
constexpr size_t mb = 1024 * kb;
constexpr size_t gb = 1024 * mb;

constexpr size_t operator""_kb(unsigned long long val) { return val * kb; }
constexpr size_t operator""_mb(unsigned long long val) { return val * mb; }
constexpr size_t operator""_gb(unsigned long long val) { return val * gb; }

void configure_unit_test_logging() {
    seastar::global_logger_registry().set_all_loggers_level(
      seastar::log_level::trace);
    seastar::global_logger_registry().set_logger_level(
      "exception", seastar::log_level::debug);

    seastar::apply_logging_settings(seastar::logging_settings{
      .logger_levels = {{"exception", seastar::log_level::debug}},
      .default_level = seastar::log_level::trace,
      .stdout_enabled = true,
      .syslog_enabled = false,
      .stdout_timestamp_style = seastar::logger_timestamp_style::real});
}

static sstring make_test_dir() {
    return fmt::format(
      "test_dir_{}", random_generators::gen_alphanum_string(7));
}

int main(int argc, char** argv) {
    configure_unit_test_logging();
    return seastar::testing::entry_point(argc, argv);
}