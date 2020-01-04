#pragma once

#include "model/fundamental.h"
#include "random/generators.h"
#include "storage2/repository.h"
#include "test_utils/fixture.h"

#include <fmt/core.h>

constexpr size_t kb = 1024;
constexpr size_t mb = 1024 * kb;
constexpr size_t gb = 1024 * mb;

constexpr size_t operator""_kb(unsigned long long val) { return val * kb; }
constexpr size_t operator""_mb(unsigned long long val) { return val * mb; }
constexpr size_t operator""_gb(unsigned long long val) { return val * gb; }

class storage_test_fixture {
public:
    sstring test_dir = "test_data_" + random_generators::gen_alphanum_string(5);

    storage_test_fixture() { configure_unit_test_logging(); }

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

    storage::repository make_repo(
      storage::repository::config cfg
      = storage::repository::config::testing_defaults()) {
        return storage::repository::open(test_dir, std::move(cfg)).get0();
    }

    model::ntp make_ntp(sstring ns, sstring topic, size_t partition_id) {
        return model::ntp{
          .ns = model::ns(std::move(ns)),
          .tp = {.topic = model::topic(std::move(topic)),
                 .partition = model::partition_id(partition_id)}};
    }

    void create_topic_dir(sstring ns, sstring topic, size_t partition_id) {
        auto ntp = make_ntp(ns, topic, partition_id);
        recursive_touch_directory(fmt::format("{}/{}", test_dir, ntp.path()))
          .wait();
    }
};