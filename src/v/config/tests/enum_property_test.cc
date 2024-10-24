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
    config::enum_property<ss::sstring> enum_str;
    config::enum_property<std::optional<ss::sstring>> opt_enum_str;

    test_config()
      : enum_str(
          *this,
          "enum_str",
          "A string with only certain values allowed",
          {},
          "foo",
          {"foo", "bar", "baz"})
      , opt_enum_str(
          *this,
          "opt_enum_str",
          "A string with only certain values allowed",
          {},
          "foo",
          {std::nullopt, "foo", "bar", "baz"}) {}
};

SEASTAR_THREAD_TEST_CASE(enum_property_validation) {
    auto cfg = test_config();

    auto valid_values = {"foo", "bar", "baz"};
    auto invalid_values = {"fo", "fooo", "rhubarb", "", "\0"};

    std::optional<config::validation_error> verr;
    for (const auto& v : valid_values) {
        verr = cfg.enum_str.validate(YAML::Load(v));
        BOOST_CHECK(!verr.has_value());

        verr = cfg.opt_enum_str.validate(YAML::Load(v));
        BOOST_CHECK(!verr.has_value());
    }

    for (const auto& v : invalid_values) {
        verr = cfg.enum_str.validate(YAML::Load(v));
        BOOST_CHECK(verr.has_value());
        BOOST_REQUIRE(
          verr.value().error_message() == "Must be one of foo,bar,baz");

        verr = cfg.opt_enum_str.validate(YAML::Load(v));
        BOOST_CHECK(verr.has_value());
    }

    // Optional variant should also always consider null to be valid.
    verr = cfg.opt_enum_str.validate(YAML::Load("~"));
    BOOST_CHECK(!verr.has_value());
}

} // namespace
