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

namespace {

struct test_config : public config::config_store {
    config::bounded_property<int32_t> bounded_int;
    config::bounded_property<std::optional<int32_t>> bounded_int_opt;
    config::bounded_property<int16_t> odd_constraint;

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
          {.oddeven = config::odd_even_constraint::odd}) {}
};

SEASTAR_THREAD_TEST_CASE(numeric_bounds) {
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

} // namespace