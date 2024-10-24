/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "config/property.h"
#include "security/gssapi_principal_mapper.h"

#include <boost/test/data/test_case.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/test/unit_test_suite.hpp>

namespace security {

namespace bdata = boost::unit_test::data;

struct gssapi_test_record {
    gssapi_test_record(
      std::string_view gssapi_principal_name,
      std::string_view expected_primary,
      std::string_view expected_host_name,
      std::string_view expected_realm,
      std::string_view expected_name)
      : gssapi_principal_name(gssapi_principal_name)
      , expected_primary(expected_primary)
      , expected_host_name(expected_host_name)
      , expected_realm(expected_realm)
      , expected_name(expected_name) {}
    std::string_view gssapi_principal_name;
    std::string_view expected_primary;
    std::string_view expected_host_name;
    std::string_view expected_realm;
    std::string_view expected_name;

    friend std::ostream&
    operator<<(std::ostream& os, const gssapi_test_record& r) {
        fmt::print(
          os, "gssapi_principal_name: '{}', ", r.gssapi_principal_name);
        fmt::print(os, "expected_primary: '{}', ", r.expected_primary);
        fmt::print(os, "expected_host_name: '{}', ", r.expected_host_name);
        fmt::print(os, "expected_realm: '{}', ", r.expected_realm);
        fmt::print(os, "expected_name: '{}'", r.expected_name);

        return os;
    }
};

static std::array<gssapi_test_record, 4> gssapi_name_test_data{
  gssapi_test_record{
    "App.service-name/example.com@REALM.com",
    "App.service-name",
    "example.com",
    "REALM.com",
    "service-name"},
  {"App.service-name@REALM.com",
   "App.service-name",
   "",
   "REALM.com",
   "service-name"},
  {"user/host@REALM.com", "user", "host", "REALM.com", "user"},
  {"redpanda/example.com@REALM.com",
   "redpanda",
   "example.com",
   "REALM.com",
   "redpandadataexample.com"}};

BOOST_DATA_TEST_CASE(test_gssapi_name, bdata::make(gssapi_name_test_data), c) {
    static const std::vector<ss::sstring> rules = {
      "RULE:[1:$1](App\\..*)s/App\\.(.*)/$1/g",
      "RULE:[2:$1](App\\..*)s/App\\.(.*)/$1/g",
      "RULE:[2:$1data$2](redpanda.*)",
      "DEFAULT"};
    static constexpr std::string_view DEFAULT_REALM = "REALM.com";
    BOOST_REQUIRE_NO_THROW(
      auto mapper = gssapi_principal_mapper(
        config::mock_binding(std::vector<ss::sstring>{rules}));

      BOOST_REQUIRE_NO_THROW(
        auto name = gssapi_name::parse(c.gssapi_principal_name).value();
        BOOST_REQUIRE_EQUAL(c.expected_primary, name.primary());
        BOOST_REQUIRE_EQUAL(c.expected_host_name, name.host_name());
        BOOST_REQUIRE_EQUAL(c.expected_realm, name.realm());
        BOOST_REQUIRE_EQUAL(c.gssapi_principal_name, fmt::format("{}", name));
        auto result_name = mapper.apply(DEFAULT_REALM, name);
        BOOST_REQUIRE(result_name.has_value());
        BOOST_REQUIRE_EQUAL(c.expected_name, *result_name);););
}

static std::array<gssapi_test_record, 5> gssapi_lower_case_test_data{
  gssapi_test_record{"User@REALM.com", "User", "", "REALM.com", "user"},
  {"TestABC/host@FOO.COM", "TestABC", "host", "FOO.COM", "test"},
  {"ABC_User_ABC/host@FOO.COM",
   "ABC_User_ABC",
   "host",
   "FOO.COM",
   "xyz_user_xyz"},
  {"App.SERVICE-name/example.com@REALM.COM",
   "App.SERVICE-name",
   "example.com",
   "REALM.COM",
   "service-name"},
  {"User/root@REALM.COM", "User", "root", "REALM.COM", "user"}};

BOOST_DATA_TEST_CASE(
  test_gssapi_lower_case, bdata::make(gssapi_lower_case_test_data), c) {
    static const std::vector<ss::sstring> rules = {
      "RULE:[1:$1]/L",
      "RULE:[2:$1](Test.*)s/ABC///L",
      "RULE:[2:$1](ABC.*)s/ABC/XYZ/g/L",
      "RULE:[2:$1](App\\..*)s/App\\.(.*)/$1/g/L",
      "RULE:[2:$1]/L",
      "DEFAULT"};
    static constexpr std::string_view DEFAULT_REALM = "REALM.COM";
    BOOST_REQUIRE_NO_THROW(
      auto mapper = gssapi_principal_mapper(
        config::mock_binding(std::vector<ss::sstring>{rules}));

      BOOST_REQUIRE_NO_THROW(
        auto name = gssapi_name::parse(c.gssapi_principal_name).value();
        BOOST_REQUIRE_EQUAL(c.expected_primary, name.primary());
        BOOST_REQUIRE_EQUAL(c.expected_host_name, name.host_name());
        BOOST_REQUIRE_EQUAL(c.expected_realm, name.realm());
        BOOST_REQUIRE_EQUAL(c.gssapi_principal_name, fmt::format("{}", name));
        auto result_name = mapper.apply(DEFAULT_REALM, name);
        BOOST_REQUIRE(result_name.has_value());
        BOOST_REQUIRE_EQUAL(c.expected_name, *result_name);););
}

static std::array<gssapi_test_record, 5> gssapi_upper_case_test_data{
  gssapi_test_record{"User@REALM.com", "User", "", "REALM.com", "USER"},
  {"TestABC/host@FOO.COM", "TestABC", "host", "FOO.COM", "TEST"},
  {"ABC_User_ABC/host@FOO.COM",
   "ABC_User_ABC",
   "host",
   "FOO.COM",
   "XYZ_USER_XYZ"},
  {"App.SERVICE-name/example.com@REALM.COM",
   "App.SERVICE-name",
   "example.com",
   "REALM.COM",
   "SERVICE-NAME"},
  {"User/root@REALM.COM", "User", "root", "REALM.COM", "USER"}};

BOOST_DATA_TEST_CASE(
  test_gssapi_upper_case, bdata::make(gssapi_upper_case_test_data), c) {
    static const std::vector<ss::sstring> rules = {
      "RULE:[1:$1]/U",
      "RULE:[2:$1](Test.*)s/ABC///U",
      "RULE:[2:$1](ABC.*)s/ABC/XYZ/g/U",
      "RULE:[2:$1](App\\..*)s/App\\.(.*)/$1/g/U",
      "RULE:[2:$1]/U",
      "DEFAULT"};
    static constexpr std::string_view DEFAULT_REALM = "REALM.COM";
    BOOST_REQUIRE_NO_THROW(
      auto mapper = gssapi_principal_mapper(
        config::mock_binding(std::vector<ss::sstring>{rules}));

      BOOST_REQUIRE_NO_THROW(
        auto name = gssapi_name::parse(c.gssapi_principal_name).value();
        BOOST_REQUIRE_EQUAL(c.expected_primary, name.primary());
        BOOST_REQUIRE_EQUAL(c.expected_host_name, name.host_name());
        BOOST_REQUIRE_EQUAL(c.expected_realm, name.realm());
        BOOST_REQUIRE_EQUAL(c.gssapi_principal_name, fmt::format("{}", name));
        auto result_name = mapper.apply(DEFAULT_REALM, name);
        BOOST_REQUIRE(result_name.has_value());
        BOOST_REQUIRE_EQUAL(c.expected_name, *result_name);););
}

std::array<ss::sstring, 11> gssapi_invalid_rules{
  "default",
  "DEFAUL",
  "DEFAULT/L",
  "DEFAULT/g",
  "rule:[1:$1]",
  "rule:[1:$1]/L/U",
  "rule:[1:$1]/U/L",
  "rule:[1:$1]/LU",
  "RULE:[1:$1/L",
  "RULE:[1:$1]/l",
  "RULE:[2:$1](ABC.*)s/ABC/XYZ/L/g"};

BOOST_DATA_TEST_CASE(
  test_gssapi_invalid_rules, bdata::make(gssapi_invalid_rules), c) {
    BOOST_REQUIRE_THROW(
      gssapi_principal_mapper(
        config::mock_binding(std::vector<ss::sstring>{{c}})),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(test_invalid_index) {
    static const std::vector<ss::sstring> rules = {"RULE:[2:$3]"};
    static constexpr std::string_view DEFAULT_REALM = "REALM.com";
    static constexpr std::string_view TEST_NAME = "test/host@REALM.com";
    BOOST_REQUIRE_NO_THROW(
      auto mapper = gssapi_principal_mapper(
        config::mock_binding(std::vector<ss::sstring>{rules}));
      BOOST_REQUIRE_NO_THROW(auto name = gssapi_name::parse(TEST_NAME).value();
                             auto result = mapper.apply(DEFAULT_REALM, name);
                             BOOST_REQUIRE(!result.has_value());););
}

BOOST_AUTO_TEST_CASE(test_only_primary) {
    static const std::vector<ss::sstring> rules = {
      "RULE:[1:$1data](redpanda.*)", "RULE:[2:$3]"};
    static constexpr std::string_view DEFAULT_REALM = "REALM.com";
    static constexpr std::string_view TEST_NAME = "redpanda";
    BOOST_REQUIRE_NO_THROW(
      auto mapper = gssapi_principal_mapper(
        config::mock_binding(std::vector<ss::sstring>{rules}));
      BOOST_REQUIRE_NO_THROW(auto name = gssapi_name::parse(TEST_NAME).value();
                             BOOST_REQUIRE(name.host_name().empty());
                             BOOST_REQUIRE(name.realm().empty());
                             BOOST_REQUIRE_EQUAL(TEST_NAME, name.primary());
                             auto result = mapper.apply(DEFAULT_REALM, name);
                             BOOST_REQUIRE(result.has_value());
                             BOOST_REQUIRE_EQUAL(TEST_NAME, *result);););
}

struct gssapi_default_mapper_record {
    explicit gssapi_default_mapper_record(std::string_view kerberos_principal)
      : kerberos_principal(kerberos_principal) {}
    gssapi_default_mapper_record(
      std::string_view kerberos_principal, std::string_view mapped_name)
      : kerberos_principal(kerberos_principal)
      , mapped_name(mapped_name) {}

    std::string_view kerberos_principal;
    std::optional<std::string_view> mapped_name{std::nullopt};

    friend std::ostream&
    operator<<(std::ostream& os, const gssapi_default_mapper_record& r) {
        fmt::print(os, "kerberos_principal: '{}'", r.kerberos_principal);
        if (r.mapped_name) {
            fmt::print(os, ", mapped_name: '{}'", *(r.mapped_name));
        }
        return os;
    }
};

static std::array<gssapi_default_mapper_record, 2> gssapi_default_rule_test_set{
  gssapi_default_mapper_record{"test@REALM.com", "test"},
  gssapi_default_mapper_record{"test@test.com"}};

BOOST_DATA_TEST_CASE(
  test_no_rules, bdata::make(gssapi_default_rule_test_set), c) {
    static const std::string_view DEFAULT_REALM = "REALM.com";
    BOOST_REQUIRE_NO_THROW(
      auto mapper = gssapi_principal_mapper(
        config::mock_binding(std::vector<ss::sstring>{}));
      auto name = gssapi_name::parse(c.kerberos_principal).value();
      auto result = mapper.apply(DEFAULT_REALM, name);
      BOOST_REQUIRE_EQUAL(c.mapped_name, result););
}

struct gssapi_mapping_rules_test {
    explicit gssapi_mapping_rules_test(std::vector<ss::sstring> rules)
      : rules(std::move(rules)) {}
    gssapi_mapping_rules_test(
      std::vector<ss::sstring> rules, std::string_view error_message)
      : rules(std::move(rules))
      , error_message(error_message) {}

    std::vector<ss::sstring> rules;
    std::optional<std::string_view> error_message{std::nullopt};

    friend std::ostream&
    operator<<(std::ostream& os, const gssapi_mapping_rules_test& r) {
        fmt::print(os, "rules: '[{}]'", fmt::join(r.rules, ", "));
        if (r.error_message) {
            fmt::print(os, ", error_message: '{}'", *(r.error_message));
        }
        return os;
    }
};

static std::array<gssapi_mapping_rules_test, 3> gssapi_mapping_rules_test_data{
  gssapi_mapping_rules_test{{ss::sstring{"DEFAULT"}}},
  {{ss::sstring{"default"}}, "GSSAPI: Invalid rule: default"},
  {{ss::sstring{"RULE:[9999999999:$1]"}},
   "Invalid rule - Invalid value for number of components: 9999999999"}};

BOOST_DATA_TEST_CASE(
  test_validate_mapping_rules, bdata::make(gssapi_mapping_rules_test_data), c) {
    BOOST_REQUIRE_EQUAL(
      c.error_message, validate_kerberos_mapping_rules(c.rules));
}

BOOST_AUTO_TEST_CASE(invalid_names) {
    BOOST_REQUIRE(!gssapi_name::parse("test@@@test").has_value());
    BOOST_REQUIRE_THROW(gssapi_name("", "", ""), std::invalid_argument);
}
} // namespace security
