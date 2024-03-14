// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/tests/config_response_utils_test_help.h"
#include "utils/to_string.h"

#include <boost/test/auto_unit_test.hpp>
#include <boost/test/test_tools.hpp>
#include <boost/test/tools/old/interface.hpp>

#include <array>
#include <optional>
#include <string_view>
#include <tuple>

std::optional<ss::sstring> get_config(
  const kafka::config_response_container_t& result, std::string_view key) {
    for (const auto& config : result) {
        if (config.name == key) {
            return config.value;
        }
    }
    throw std::runtime_error(fmt::format("Key not found: {}", key));
}

kafka::describe_configs_source get_config_source(
  const kafka::config_response_container_t& result, std::string_view key) {
    for (const auto& config : result) {
        if (config.name == key) {
            return config.config_source;
        }
    }
    throw std::runtime_error(fmt::format("Key not found: {}", key));
}

BOOST_AUTO_TEST_CASE(add_topic_config_if_requested_tristate) {
    using namespace kafka;
    auto verify_tristate_config = [](
                                    std::optional<int> default_value,
                                    tristate<int> override_value,
                                    std::optional<ss::sstring> expected_value) {
        config_response_container_t result;

        add_topic_config_if_requested(
          std::nullopt,
          result,
          "test-global-broker-config-name",
          default_value,
          "test-topic-override-name",
          override_value,
          false,
          std::nullopt);

        BOOST_CHECK_EQUAL(
          get_config(result, "test-topic-override-name"), expected_value);
    };

    // clang-format off
    verify_tristate_config(std::make_optional(2), tristate<int>(1), std::make_optional("1"));
    verify_tristate_config(std::make_optional(2), tristate<int>(std::nullopt), std::make_optional("2"));
    verify_tristate_config(std::make_optional(2), tristate<int>(), std::make_optional("-1"));

    verify_tristate_config(std::nullopt, tristate<int>(1), std::make_optional("1"));
    verify_tristate_config(std::nullopt, tristate<int>(std::nullopt), std::make_optional("-1"));
    verify_tristate_config(std::nullopt, tristate<int>(), std::make_optional("-1"));
    // clang-format on
}

BOOST_AUTO_TEST_CASE(add_topic_config_if_requested_optional) {
    using namespace kafka;
    auto verify_optional_config = [](
                                    int default_value,
                                    std::optional<int> override_value,
                                    std::optional<ss::sstring> expected_value,
                                    bool hide_default_override) {
        config_response_container_t result;

        add_topic_config_if_requested(
          std::nullopt,
          result,
          "test-global-broker-config-name",
          default_value,
          "test-topic-override-name",
          override_value,
          false,
          std::nullopt,
          &describe_as_string<int>,
          hide_default_override);

        BOOST_CHECK_EQUAL(
          get_config(result, "test-topic-override-name"), expected_value);
    };

    // clang-format off
    verify_optional_config(2, std::make_optional(1), std::make_optional("1"), false);
    verify_optional_config(2, std::nullopt, std::make_optional("2"), false);
    // clang-format on
}

BOOST_AUTO_TEST_CASE(add_topic_config_if_requested_optional_hide_default) {
    using namespace kafka;

    auto verify_optional_config_with_hide_override =
      [](
        bool hide_default_override,
        kafka::describe_configs_source expected_source) {
          config_response_container_t result;

          add_topic_config_if_requested(
            std::nullopt,
            result,
            "test-global-broker-config-name",
            2,
            "test-topic-override-name",
            std::make_optional(2),
            false,
            std::nullopt,
            &describe_as_string<int>,
            hide_default_override);

          BOOST_CHECK_EQUAL(
            get_config(result, "test-topic-override-name"),
            std::make_optional("2"));
          BOOST_CHECK_EQUAL(
            get_config_source(result, "test-topic-override-name"),
            expected_source);
      };

    // clang-format off
    verify_optional_config_with_hide_override(false, kafka::describe_configs_source::topic);
    verify_optional_config_with_hide_override(true, kafka::describe_configs_source::default_config);
    // clang-format on
}
