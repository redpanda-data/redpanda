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

#include <seastar/testing/thread_test_case.hh>

#include <chrono>
#include <optional>

namespace {
using namespace std::chrono_literals;

struct test_config : public config::config_store {
    config::retention_duration_property valid_retention;
    config::retention_duration_property inf_retention;
    config::retention_duration_property default_retention;

    test_config()
      : valid_retention(
          *this,
          "valid_retention",
          "A positive value",
          {.needs_restart = config::needs_restart::no,
           .visibility = config::visibility::user},
          10080min)
      , inf_retention(
          *this,
          "inf_retention",
          "-1 should be interpreted as infinity",
          {.needs_restart = config::needs_restart::no,
           .visibility = config::visibility::user},
          10080min)
      , default_retention(
          *this,
          "default_retention",
          "not set retention config should return default value",
          {.needs_restart = config::needs_restart::no,
           .visibility = config::visibility::user},
          10080min) {}
};

SEASTAR_THREAD_TEST_CASE(test_retention_property) {
    auto cfg = test_config();

    cfg.valid_retention.set_value(YAML::Load("4095"));
    BOOST_CHECK(cfg.valid_retention() == std::chrono::milliseconds(4095));

    cfg.valid_retention.set_value(std::chrono::milliseconds(809098));
    BOOST_CHECK(cfg.valid_retention() == std::chrono::milliseconds(809098));

    cfg.inf_retention.set_value(YAML::Load("-1"));
    BOOST_CHECK(cfg.inf_retention() == std::nullopt);

    cfg.inf_retention.set_value(std::chrono::milliseconds(-1));
    BOOST_CHECK(cfg.inf_retention() == std::nullopt);

    BOOST_CHECK(cfg.default_retention().value() == std::optional(10080min));
}

SEASTAR_THREAD_TEST_CASE(test_retention_property_polymorphism) {
    auto cfg = test_config();
    using base = config::property<std::optional<std::chrono::milliseconds>>;

    base& valid_retention = cfg.valid_retention;
    valid_retention.set_value(YAML::Load("-1"));
    BOOST_CHECK(valid_retention() == std::nullopt);

    base& inf_retention = cfg.inf_retention;
    inf_retention.set_value(YAML::Load("-1"));
    BOOST_CHECK(inf_retention() == std::nullopt);

    inf_retention.set_value(std::chrono::milliseconds(-1));
    BOOST_CHECK(inf_retention() == std::nullopt);

    base& default_retention = cfg.default_retention;
    BOOST_CHECK(default_retention().value() == std::optional(10080min));
}
} // namespace
