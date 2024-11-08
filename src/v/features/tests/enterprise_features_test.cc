// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/base_property.h"
#include "config/config_store.h"
#include "config/property.h"
#include "config/types.h"
#include "features/enterprise_features.h"

#include <gtest/gtest-typed-test.h>
#include <gtest/gtest.h>

namespace config {
namespace {

struct test_config : public config_store {
    enterprise<property<bool>> enterprise_bool;
    enterprise<enum_property<ss::sstring>> enterprise_str_enum;
    enterprise<property<std::vector<ss::sstring>>> enterprise_str_vec;
    enterprise<property<std::optional<int>>> enterprise_opt_int;
    enterprise<enum_property<tls_version>> enterprise_enum;

    using meta = base_property::metadata;

    test_config()
      : enterprise_bool(
          *this,
          true,
          "enterprise_bool",
          "An enterprise-only bool config",
          meta{.needs_restart = needs_restart::no},
          false,
          property<bool>::noop_validator,
          std::nullopt)
      , enterprise_str_enum(
          *this,
          std::vector<ss::sstring>{"bar"},
          "enterprise_str_enum",
          "An enterprise-only enum property",
          meta{.needs_restart = needs_restart::no},
          "foo",
          std::vector<ss::sstring>{"foo", "bar", "baz"})
      , enterprise_str_vec(
          *this,
          std::vector<ss::sstring>{"GSSAPI"},
          "enterprise_str_vec",
          "An enterprise-only vector of strings",
          meta{.needs_restart = needs_restart::no})
      , enterprise_opt_int(
          *this,
          [](const int& v) -> bool { return v > 1000; },
          "enterprise_opt_int",
          "An enterprise-only optional int",
          meta{.needs_restart = needs_restart::no},
          0)
      , enterprise_enum(
          *this,
          std::vector<tls_version>{tls_version::v1_3},
          "enterprise_str_enum",
          "An enterprise-only enum property",
          meta{.needs_restart = needs_restart::no},
          tls_version::v1_1,
          std::vector<tls_version>{
            tls_version::v1_0,
            tls_version::v1_1,
            tls_version::v1_2,
            tls_version::v1_3}) {}
};

} // namespace
} // namespace config

namespace features {
template<typename Property>
struct EnterpriseFeatureTest : public ::testing::Test {
    using value_type = typename Property::value_type;

    struct test_values {
        value_type default_value;
        value_type allowed_value;
        value_type restricted_value;
    };

    struct test_case {
        config::enterprise<Property>* enterprise_conf;
        test_values ts;
    };

    test_case get_test_case() {
        if constexpr (std::same_as<Property, config::property<bool>>) {
            return {
              &cfg.enterprise_bool,
              {.default_value = false,
               .allowed_value = false,
               .restricted_value = true}};
        }
        if constexpr (std::
                        same_as<Property, config::enum_property<ss::sstring>>) {
            return {
              &cfg.enterprise_str_enum,
              {.default_value = "foo",
               .allowed_value = "baz",
               .restricted_value = "bar"}};
        }
        if constexpr (std::same_as<
                        Property,
                        config::property<std::vector<ss::sstring>>>) {
            return {
              &cfg.enterprise_str_vec,
              {.default_value{},
               .allowed_value = {"OTHER"},
               .restricted_value = {"GSSAPI", "OTHER"}}};
        }
        if constexpr (std::same_as<
                        Property,
                        config::property<std::optional<int>>>) {
            return {
              &cfg.enterprise_opt_int,
              {.default_value{0}, .allowed_value{10}, .restricted_value{1010}}};
        }
        if constexpr (std::same_as<
                        Property,
                        config::enum_property<config::tls_version>>) {
            return {
              &cfg.enterprise_enum,
              {.default_value = config::tls_version::v1_1,
               .allowed_value = config::tls_version::v1_2,
               .restricted_value = config::tls_version::v1_3}};
        }
    }

    config::test_config cfg{};
};

using EnterprisePropertyTypes = ::testing::Types<
  config::property<bool>,
  config::enum_property<ss::sstring>,
  config::property<std::vector<ss::sstring>>,
  config::property<std::optional<int>>,
  config::enum_property<config::tls_version>>;
TYPED_TEST_SUITE(EnterpriseFeatureTest, EnterprisePropertyTypes);

TYPED_TEST(EnterpriseFeatureTest, SanctionedValues) {
    auto tc = this->get_test_case();
    auto& enterprise_conf = *tc.enterprise_conf;
    const auto [default_value, allowed_value, restricted_value] = tc.ts;

    auto binded = sanctioning_binding{enterprise_conf};

    // default value
    EXPECT_EQ(enterprise_conf.value(), default_value);

    EXPECT_EQ(binded(true), std::make_pair(default_value, false));
    EXPECT_EQ(binded(false), std::make_pair(default_value, false));

    // allowed value
    enterprise_conf.set_value(allowed_value);

    EXPECT_EQ(enterprise_conf.value(), allowed_value);

    EXPECT_EQ(binded(true), std::make_pair(allowed_value, false));
    EXPECT_EQ(binded(false), std::make_pair(allowed_value, false));

    // sanctioned value
    enterprise_conf.set_value(restricted_value);

    EXPECT_EQ(enterprise_conf.value(), restricted_value);

    EXPECT_EQ(binded(true), std::make_pair(default_value, true));
    EXPECT_EQ(binded(false), std::make_pair(restricted_value, false));
}

} // namespace features
