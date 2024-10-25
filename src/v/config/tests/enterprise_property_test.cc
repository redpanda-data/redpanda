// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/config_store.h"
#include "config/property.h"

#include <gtest/gtest.h>

#include <iostream>

namespace config {
namespace {

// enum class test_e {
//     foo,
//     bar,
//     baz,
// };

// std::ostream& operator<<(std::ostream& os, test_e e) {
//     switch (e) {
//     case test_e::foo:
//         return os << "foo";
//     case test_e::bar:
//         return os << "bar";
//     case test_e::baz:
//         return os << "baz";
//     }
// }

struct test_config : public config_store {
    enterprise<property<bool>> enterprise_bool;
    enterprise<enum_property<ss::sstring>> enterprise_str_enum;
    enterprise<property<std::vector<ss::sstring>>> enterprise_str_vec;
    // enterprise<enum_property<test_e>> enterprise_enum;

    using meta = base_property::metadata;

    test_config()
      : enterprise_bool(
          *this,
          true,
          "enterprise_bool",
          "An enterprise-only bool config",
          meta{},
          false,
          property<bool>::noop_validator,
          std::nullopt)
      , enterprise_str_enum(
          *this,
          std::vector<ss::sstring>{"bar"},
          "enterprise_str_enum",
          "An enterprise-only enum property",
          meta{},
          "foo",
          std::vector<ss::sstring>{"foo", "bar", "baz"})
      , enterprise_str_vec(
          *this,
          std::vector<ss::sstring>{"GSSAPI"},
          "enterprise_str_vec",
          "An enterprise-only vector of strings")
    // , enterprise_enum(
    //     *this,
    //     test_e::baz,
    //     "enterprise_enum",
    //     "An enterprise-only enum property",
    //     meta{},
    //     test_e::foo,
    //     std::vector<test_e>{test_e::foo, test_e::bar, test_e::baz})
    {}
};
} // namespace

TEST(EnterprisePropertyTest, TestRestriction) {
    test_config cfg;

    EXPECT_FALSE(cfg.enterprise_bool.check_restricted(false));
    EXPECT_TRUE(cfg.enterprise_bool.check_restricted(true));
    // TODO(oren): type_name is all messed up. user error w/o a doubt
    // EXPECT_EQ(
    //   ss::sstring(cfg.enterprise_bool.type_name()), ss::sstring{"bool"});

    EXPECT_FALSE(cfg.enterprise_str_enum.check_restricted("foo"));
    EXPECT_TRUE(cfg.enterprise_str_enum.check_restricted("bar"));

    EXPECT_FALSE(
      cfg.enterprise_str_vec.check_restricted({"foo", "bar", "baz"}));
    EXPECT_TRUE(
      cfg.enterprise_str_vec.check_restricted({"foo", "bar", "baz", "GSSAPI"}));

    // EXPECT_FALSE(cfg.enterprise_enum.check_restricted(test_e::bar));
    // EXPECT_TRUE(cfg.enterprise_enum.check_restricted(test_e::baz));
}

} // namespace config
