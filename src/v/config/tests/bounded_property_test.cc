// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/bounded_property.h"
#include "config/config_store.h"

#include <seastar/testing/thread_test_case.hh>

#include <chrono>
#include <optional>

static_assert(config::detail::bounds<config::numeric_integral_bounds<int>>);
static_assert(config::detail::bounds<config::numeric_bounds<double>>);

namespace {

using namespace std::literals;

struct test_config : public config::config_store {
    config::bounded_property<int32_t> bounded_int;
    config::bounded_property<std::optional<int32_t>> bounded_int_opt;
    config::bounded_property<int16_t> odd_constraint;
    config::bounded_property<double, config::numeric_bounds> bounded_double;
    config::bounded_property<std::optional<double>, config::numeric_bounds>
      bounded_double_opt;
    config::bounded_property<std::optional<std::chrono::milliseconds>>
      bounded_opt_ms;

    test_config()
      : bounded_int(
          *this,
          "bounded_int",
          "An integer with some bounds set",
          {},
          16384,
          {.min = 4096, .max = 32768, .align = 16})
      , bounded_int_opt(
          *this,
          "bounded_int_opt",
          "An optional integer with some bounds set",
          {},
          std::nullopt,
          {.min = 4096, .max = 32768, .align = 16})
      , odd_constraint(
          *this,
          "odd_constraint",
          "Property value has to be odd",
          {},
          1,
          {.oddeven = config::odd_even_constraint::odd})
      , bounded_double(
          *this,
          "bounded_double",
          "A float with some bounds set",
          {},
          1.618033988749,
          {.min = -1, .max = 2.236067977})
      , bounded_double_opt(
          *this,
          "bounded_double_opt",
          "An options float with some bounds set",
          {},
          std::nullopt,
          {.min = -1, .max = 2.236067977})
      , bounded_opt_ms(
          *this,
          "bounded_opt_ms",
          "An optional duration",
          {},
          std::nullopt,
          {.min = 5ms}) {}
};

SEASTAR_THREAD_TEST_CASE(numeric_integral_bounds) {
    auto cfg = test_config();

    // We are checking numeric bounds, not YAML syntax/type validation.  The
    // latter shows up as exceptions from validate/set_value, rather than
    // as structured errors, and is not covered by this test.
    auto valid_values = {"4096", "32768", "16384", "4112", "0x1000"};
    auto invalid_values = {
      "0", "1", "-1", "4095", "4097", "32769", "2000000000"};
    auto even_values = {"2", "10", "100"};
    auto odd_values = {"1", "9", "21"};

    std::optional<config::validation_error> verr;

    for (const auto& v : valid_values) {
        verr = cfg.bounded_int.validate(YAML::Load(v));
        BOOST_CHECK(!verr.has_value());

        verr = cfg.bounded_int_opt.validate(YAML::Load(v));
        BOOST_CHECK(!verr.has_value());
    }

    for (const auto& v : invalid_values) {
        verr = cfg.bounded_int.validate(YAML::Load(v));
        BOOST_CHECK(verr.has_value());

        verr = cfg.bounded_int_opt.validate(YAML::Load(v));
        BOOST_CHECK(verr.has_value());
    }

    for (const auto& v : even_values) {
        verr = cfg.odd_constraint.validate(YAML::Load(v));
        BOOST_CHECK(verr.has_value());
    }

    for (const auto& v : odd_values) {
        verr = cfg.odd_constraint.validate(YAML::Load(v));
        BOOST_CHECK(!verr.has_value());
    }

    // Optional variant should also always consider nullopt to be valid.
    verr = cfg.bounded_int_opt.validate(std::nullopt);
    BOOST_CHECK(!verr.has_value());

    // # Invalid values should be clamped by set_value
    // Too low: clamp to minimum
    cfg.bounded_int.set_value(YAML::Load("4095"));
    BOOST_CHECK(cfg.bounded_int() == 4096);

    // Too high: clamp to maximum
    cfg.bounded_int.set_value(YAML::Load("1000000"));
    BOOST_CHECK(cfg.bounded_int() == 32768);

    // Misaligned: clamp to next lowest alignment
    cfg.bounded_int.set_value(YAML::Load("8197"));
    BOOST_CHECK(cfg.bounded_int() == 8192);
}

SEASTAR_THREAD_TEST_CASE(numeric_fp_bounds) {
    auto cfg = test_config();

    // We are checking numeric bounds, not YAML syntax/type validation.  The
    // latter shows up as exceptions from validate/set_value, rather than
    // as structured errors, and is not covered by this test.
    auto valid_values = {"-1", "-0.1", "1.618033988749", "2", "2.236067977"};
    auto invalid_values = {
      "-1000.9", "-1.0001", "3", "4095", "4097", "32769", "2000000000"};

    std::optional<config::validation_error> verr;

    for (const auto& v : valid_values) {
        verr = cfg.bounded_double.validate(YAML::Load(v));
        BOOST_TEST(!verr.has_value());

        verr = cfg.bounded_double_opt.validate(YAML::Load(v));
        BOOST_TEST(!verr.has_value());
    }

    for (const auto& v : invalid_values) {
        verr = cfg.bounded_double.validate(YAML::Load(v));
        BOOST_TEST(verr.has_value());

        verr = cfg.bounded_double_opt.validate(YAML::Load(v));
        BOOST_TEST(verr.has_value());
    }

    // Optional variant should also always consider nullopt to be valid.
    verr = cfg.bounded_double_opt.validate(std::nullopt);
    BOOST_TEST(!verr.has_value());

    // # Invalid values should be clamped by set_value
    // Too low: clamp to minimum
    cfg.bounded_double.set_value(YAML::Load("-2"));
    BOOST_TEST(cfg.bounded_double() == -1);

    // Too high: clamp to maximum
    cfg.bounded_double.set_value(YAML::Load("1000000"));
    BOOST_TEST(cfg.bounded_double() == 2.236067977);
}

SEASTAR_THREAD_TEST_CASE(bounded_property_set_value) {
    auto cfg = test_config();

    BOOST_TEST_INFO("fully specified optional");
    cfg.bounded_opt_ms.set_value(std::optional{20ms});
    BOOST_CHECK_EQUAL(cfg.bounded_opt_ms(), 20ms);
    BOOST_TEST_INFO("std::nullopt");
    cfg.bounded_opt_ms.set_value(std::nullopt);
    BOOST_CHECK(!cfg.bounded_opt_ms());
    BOOST_TEST_INFO("a value convertible to the optional::value type");
    cfg.bounded_opt_ms.set_value(2h);
    BOOST_CHECK_EQUAL(cfg.bounded_opt_ms(), 2h);
}

} // namespace
