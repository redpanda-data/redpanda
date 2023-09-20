// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/config_store.h"
#include "config/property.h"
#include "config/validators.h"
#include "model/fundamental.h"

#include <seastar/testing/thread_test_case.hh>

#include <chrono>
#include <limits>
#include <optional>

using namespace std::chrono_literals;

namespace {

constexpr static int32_t LOCKED_MIN = 2048;
constexpr static int32_t LOCKED_MAX = 4096;
constexpr static std::chrono::milliseconds LOCKED_MIN_MS = 1s;
constexpr static std::chrono::milliseconds LOCKED_MAX_MS = 5s;

template<typename T>
struct test_locked_equal_property : public config::locked_equal_property<T> {
    // Overloads a protected constructor so the lock is automatically set
    test_locked_equal_property(
      config::config_store& conf,
      std::string_view name,
      std::string_view desc,
      config::base_property::metadata meta,
      config::binding<T> original_config)
      : config::locked_equal_property<T>(
        conf, name, desc, meta, std::move(original_config), true) {}
};

struct test_config : public config::config_store {
    config::locked_range_property<int32_t> locked_min_int;
    config::locked_range_property<int32_t> locked_max_int;
    config::locked_range_property<std::chrono::milliseconds> locked_min_ms;
    config::locked_range_property<std::chrono::milliseconds> locked_max_ms;
    config::property<model::cleanup_policy_bitflags> original_enum;
    test_locked_equal_property<model::cleanup_policy_bitflags> locked_enum;
    config::property<ss::sstring> original_str;
    test_locked_equal_property<ss::sstring> locked_str;

    test_config()
      : locked_min_int(
        *this,
        "locked_min_int",
        "A locked integral property with lower bound",
        {},
        LOCKED_MIN,
        config::validate_locked_min<int32_t>)
      , locked_max_int(
          *this,
          "locked_max_int",
          "A locked integral property with upper bound",
          {},
          LOCKED_MAX,
          config::validate_locked_max<int32_t>)
      , locked_min_ms(
          *this,
          "locked_min_ms",
          "A locked milliseconds property with lower bound",
          {},
          LOCKED_MIN_MS,
          config::validate_locked_min<std::chrono::milliseconds>)
      , locked_max_ms(
          *this,
          "locked_max_ms",
          "A locked milliseconds property with upper bound",
          {},
          LOCKED_MAX_MS,
          config::validate_locked_max<std::chrono::milliseconds>)
      , original_enum(
          *this,
          "original_enum",
          "An enumertaion property",
          {.needs_restart = config::needs_restart::no},
          model::cleanup_policy_bitflags::deletion)
      , locked_enum(
          *this,
          "locked_enum",
          "A locked enumertaion property",
          {},
          original_enum.bind())
      , original_str(
          *this,
          "original_str",
          "A string property",
          {.needs_restart = config::needs_restart::no},
          ss::sstring("panda"))
      , locked_str(
          *this,
          "locked_str",
          "A locked string property",
          {},
          original_str.bind()) {}
};

SEASTAR_THREAD_TEST_CASE(locked_integral_properties) {
    auto cfg = test_config();

    std::vector<int32_t> valid_values = {LOCKED_MIN + 1, LOCKED_MAX - 1};
    std::vector<int32_t> invalid_min_values = {
      LOCKED_MIN - 1, -1, 0, std::numeric_limits<int32_t>::min()};
    std::vector<int32_t> invalid_max_values = {
      LOCKED_MAX + 1, std::numeric_limits<int32_t>::max()};

    bool satisfied = false;

    for (const auto& v : valid_values) {
        satisfied = cfg.locked_min_int.validate_lock(v);
        BOOST_CHECK(satisfied);

        satisfied = cfg.locked_max_int.validate_lock(v);
        BOOST_CHECK(satisfied);
    }

    for (const auto& v : invalid_min_values) {
        satisfied = cfg.locked_min_int.validate_lock(v);
        BOOST_CHECK(!satisfied);
    }

    for (const auto& v : invalid_max_values) {
        satisfied = cfg.locked_max_int.validate_lock(v);
        BOOST_CHECK(!satisfied);
    }

    std::vector<std::chrono::milliseconds> valid_ms_values = {
      LOCKED_MIN_MS + 1ms, LOCKED_MAX_MS - 1ms};
    std::vector<std::chrono::milliseconds> invalid_min_ms_values = {
      LOCKED_MIN_MS - 1ms, 0ms, -1ms};

    for (const auto& v : valid_ms_values) {
        satisfied = cfg.locked_min_ms.validate_lock(v);
        BOOST_CHECK(satisfied);

        satisfied = cfg.locked_max_ms.validate_lock(v);
        BOOST_CHECK(satisfied);
    }

    for (const auto& v : invalid_min_ms_values) {
        satisfied = cfg.locked_min_ms.validate_lock(v);
        BOOST_CHECK(!satisfied);
    }

    std::chrono::milliseconds invalid_max_ms_value = LOCKED_MAX_MS + 1ms;
    satisfied = cfg.locked_max_ms.validate_lock(invalid_max_ms_value);
    BOOST_CHECK(!satisfied);
}

SEASTAR_THREAD_TEST_CASE(locked_nonintegral_properties) {
    auto cfg = test_config();

    bool satisfied = false;

    // Valid test
    satisfied = cfg.locked_enum.validate_lock(cfg.original_enum.value());
    BOOST_CHECK(satisfied);

    satisfied = cfg.locked_str.validate_lock(cfg.original_str.value());
    BOOST_CHECK(satisfied);

    // Invalid test
    satisfied = cfg.locked_enum.validate_lock(
      model::cleanup_policy_bitflags::compaction);
    BOOST_CHECK(!satisfied);

    ss::sstring str("red");
    satisfied = cfg.locked_str.validate_lock(str);
    BOOST_CHECK(!satisfied);
}

} // namespace
