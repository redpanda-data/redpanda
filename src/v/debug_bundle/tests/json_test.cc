/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "debug_bundle/json.h"
#include "debug_bundle/types.h"
#include "json/document.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "security/types.h"
#include "utils/uuid.h"

#include <fmt/chrono.h>
#include <gtest/gtest-typed-test.h>
#include <gtest/gtest.h>
#include <rapidjson/error/en.h>

#include <chrono>
#include <optional>
#include <string>
#include <type_traits>

using namespace debug_bundle;
using namespace std::chrono_literals;

template<typename T>
class JsonTypeTest : public testing::Test {
public:
    ss::sstring json_input;
    T expected;
};

using optional_int = std::optional<int>;
using optional_str = std::optional<ss::sstring>;
using named_int = named_type<int, struct test_named_tag>;

using JsonTestTypes = ::testing::Types<
  int,
  optional_int,
  optional_str,
  named_int,
  uint64_t,
  std::chrono::seconds,
  ss::sstring,
  uuid_t,
  special_date,
  clock::time_point,
  time_variant,
  scram_creds,
  debug_bundle_authn_options,
  partition_selection,
  debug_bundle_parameters,
  std::vector<int>,
  absl::btree_set<int>,
  bool,
  label_selection>;
TYPED_TEST_SUITE(JsonTypeTest, JsonTestTypes);

TYPED_TEST(JsonTypeTest, BasicType) {
    if constexpr (std::is_same_v<TypeParam, int>) {
        this->json_input = R"(42)";
        this->expected = 42;
    } else if constexpr (std::is_same_v<TypeParam, optional_int>) {
        this->json_input = R"(42)";
        this->expected = optional_int{42};
    } else if constexpr (std::is_same_v<TypeParam, optional_str>) {
        this->json_input = R"(null)";
        this->expected = std::nullopt;
    } else if constexpr (std::is_same_v<TypeParam, named_int>) {
        this->json_input = R"(42)";
        this->expected = named_int{42};
    } else if constexpr (std::is_same_v<TypeParam, uint64_t>) {
        this->json_input = R"(42)";
        this->expected = 42;
    } else if constexpr (std::is_same_v<TypeParam, std::chrono::seconds>) {
        this->json_input = R"(42)";
        this->expected = std::chrono::seconds{42};
    } else if constexpr (std::is_same_v<TypeParam, ss::sstring>) {
        this->json_input = R"("42")";
        this->expected = "42";
    } else if constexpr (std::is_same_v<TypeParam, uuid_t>) {
        this->json_input = R"("e6fd84b8-f12b-407e-941f-e968d392e40d")";
        this->expected = uuid_t::from_string(
          "e6fd84b8-f12b-407e-941f-e968d392e40d");
    } else if constexpr (std::is_same_v<TypeParam, special_date>) {
        this->json_input = R"("yesterday")";
        this->expected = special_date::yesterday;
    } else if constexpr (std::is_same_v<TypeParam, clock::time_point>) {
        auto now = std::chrono::system_clock::now();
        this->json_input = fmt::format(R"("{:%FT%T}")", now);
        std::cout << this->json_input << std::endl;
        this->expected = clock::from_time_t(
          std::chrono::system_clock::to_time_t(now));
    } else if constexpr (std::is_same_v<TypeParam, time_variant>) {
        this->json_input = R"("tomorrow")";
        this->expected = special_date::tomorrow;
    } else if constexpr (std::is_same_v<TypeParam, scram_creds>) {
        this->json_input
          = R"({"username": "user", "password": "pass", "mechanism": "SCRAM-SHA-256"})";
        this->expected = scram_creds{
          .username{"user"}, .password{"pass"}, .mechanism{"SCRAM-SHA-256"}};
    } else if constexpr (std::
                           is_same_v<TypeParam, debug_bundle_authn_options>) {
        this->json_input
          = R"({"username": "user", "password": "pass", "mechanism": "SCRAM-SHA-256"})";
        this->expected = TypeParam{scram_creds{
          .username{"user"}, .password{"pass"}, .mechanism{"SCRAM-SHA-256"}}};
    } else if constexpr (std::is_same_v<TypeParam, partition_selection>) {
        this->json_input = R"("foo/bar/1")";
        this->expected = {
          {model::ns{"foo"}, model::topic{"bar"}}, {{model::partition_id{1}}}};
    } else if constexpr (std::is_same_v<TypeParam, debug_bundle_parameters>) {
        this->json_input = R"({
  "authentication": {
    "username": "user",
    "password": "pass",
    "mechanism": "SCRAM-SHA-256"
  },
  "controller_logs_size_limit_bytes": 42,
  "cpu_profiler_wait_seconds": 42,
  "logs_since": "yesterday",
  "logs_size_limit_bytes": 42,
  "logs_until": "2024-09-05T14:34:02",
  "metrics_interval_seconds": 42,
  "metrics_samples": 2,
  "partition": [
    "foo/bar/1,2",
    "baz/1,2,3"
   ],
  "tls_enabled": true,
  "tls_insecure_skip_verify": false,
  "namespace": "k8s-namespace",
  "label_selector": [
    {"key": "test/key1", "value": "value1"},
    {"key": "key2", "value": "value2"}
  ]
})";
        const std::string_view test_time = "2024-09-05T14:34:02";
        std::istringstream ss(test_time.data());
        std::tm tmp{};
        ASSERT_NO_THROW(ss >> std::get_time(&tmp, "%FT%T"));
        ASSERT_FALSE(ss.fail());
        // Forces `std::mktime` to "figure it out"
        tmp.tm_isdst = -1;
        std::time_t tt = std::mktime(&tmp);

        this->expected
          = debug_bundle_parameters{
            .authn_options
            = scram_creds{.username = security::credential_user{"user"}, .password = security::credential_password{"pass"}, .mechanism = "SCRAM-SHA-256"},
            .controller_logs_size_limit_bytes = 42,
            .cpu_profiler_wait_seconds = 42s,
            .logs_since = special_date::yesterday,
            .logs_size_limit_bytes = 42,
            .logs_until = clock::from_time_t(tt),
            .metrics_interval_seconds = 42s,
            .metrics_samples = 2,
            .partition = std::vector<partition_selection>{{{model::ns{"foo"}, model::topic{"bar"}}, {{model::partition_id{1}, model::partition_id{2}}}}, {{model::kafka_namespace, model::topic{"baz"}}, {{model::partition_id{1}, model::partition_id{2}, model::partition_id{3}}}}},
            .tls_enabled = true,
            .tls_insecure_skip_verify = false,
            .k8s_namespace = "k8s-namespace",
            .label_selector = std::vector<label_selection>{
              {"test/key1", "value1"}, {"key2", "value2"}}};
    } else if constexpr (detail::
                           is_specialization_of_v<TypeParam, std::vector>) {
        this->json_input = R"([1,2,3])";
        this->expected = {1, 2, 3};
    } else if constexpr (detail::
                           is_specialization_of_v<TypeParam, absl::btree_set>) {
        this->json_input = R"([1,2,3])";
        this->expected = {1, 2, 3};
    } else if constexpr (std::is_same_v<TypeParam, bool>) {
        this->json_input = R"(true)";
        this->expected = true;
    } else if constexpr (std::is_same_v<TypeParam, label_selection>) {
        this->json_input = R"({"key": "test/key1", "value": "value1"})";
        this->expected = label_selection{.key = "test/key1", .value = "value1"};
    } else {
        static_assert(always_false_v<TypeParam>, "not implemented");
    }

    json::Document doc;
    ASSERT_NO_THROW(doc.Parse(this->json_input));
    ASSERT_FALSE(doc.HasParseError()) << fmt::format(
      "Malformed json schema: {} at offset {}",
      rapidjson::GetParseError_En(doc.GetParseError()),
      doc.GetErrorOffset());

    debug_bundle::result<TypeParam> res{outcome::success()};

    ASSERT_NO_THROW(res = from_json<TypeParam>(doc));
    ASSERT_TRUE(res.has_value()) << res.assume_error().message();
    ASSERT_EQ(res.assume_value(), this->expected);
}

TYPED_TEST(JsonTypeTest, TypeIsInvalid) {
    if constexpr (std::is_same_v<TypeParam, int>) {
        this->json_input = R"("42")";
        this->expected = 42;
    } else if constexpr (std::is_same_v<TypeParam, optional_int>) {
        this->json_input = R"("42")";
        this->expected = optional_int{42};
    } else if constexpr (std::is_same_v<TypeParam, optional_str>) {
        this->json_input = R"(42)";
        this->expected = std::nullopt;
    } else if constexpr (std::is_same_v<TypeParam, named_int>) {
        this->json_input = R"("42")";
        this->expected = named_int{42};
    } else if constexpr (std::is_same_v<TypeParam, uint64_t>) {
        this->json_input = R"("42")";
        this->expected = 42;
    } else if constexpr (std::is_same_v<TypeParam, std::chrono::seconds>) {
        this->json_input = R"("42")";
        this->expected = std::chrono::seconds{42};
    } else if constexpr (std::is_same_v<TypeParam, ss::sstring>) {
        this->json_input = R"(42)";
        this->expected = "42";
    } else if constexpr (std::is_same_v<TypeParam, uuid_t>) {
        this->json_input = R"("1-2-3-4-5")";
        this->expected = uuid_t::from_string(
          "e6fd84b8-f12b-407e-941f-e968d392e40d");
    } else if constexpr (std::is_same_v<TypeParam, special_date>) {
        this->json_input = R"(42)";
        this->expected = special_date::yesterday;
    } else if constexpr (std::is_same_v<TypeParam, clock::time_point>) {
        auto now = std::chrono::system_clock::now();
        this->json_input = fmt::format(R"(42)", now);
        std::cout << this->json_input << std::endl;
        this->expected = clock::from_time_t(
          std::chrono::system_clock::to_time_t(now));
    } else if constexpr (std::is_same_v<TypeParam, time_variant>) {
        this->json_input = R"("last_year")";
        this->expected = special_date::tomorrow;
    } else if constexpr (std::is_same_v<TypeParam, scram_creds>) {
        this->json_input = R"({"credential": "user:pass:SCRAM-SHA-256"})";
        this->expected = scram_creds{
          .username{"user"}, .password{"pass"}, .mechanism{"SCRAM-SHA-256"}};
    } else if constexpr (std::
                           is_same_v<TypeParam, debug_bundle_authn_options>) {
        this->json_input = R"(42)";
        this->expected = TypeParam{scram_creds{
          .username{"user"}, .password{"pass"}, .mechanism{"SCRAM-SHA-256"}}};
    } else if constexpr (std::is_same_v<TypeParam, partition_selection>) {
        this->json_input = R"("invalid")";
        this->expected = {
          {model::ns{"foo"}, model::topic{"bar"}}, {{model::partition_id{1}}}};
    } else if constexpr (std::is_same_v<TypeParam, debug_bundle_parameters>) {
        this->json_input = R"("")";
        this->expected = debug_bundle_parameters{};
    } else if constexpr (detail::
                           is_specialization_of_v<TypeParam, std::vector>) {
        this->json_input = R"(42)";
        this->expected = {1, 2, 3};
    } else if constexpr (detail::
                           is_specialization_of_v<TypeParam, absl::btree_set>) {
        this->json_input = R"(42)";
        this->expected = {1, 2, 3};
    } else if constexpr (std::is_same_v<TypeParam, bool>) {
        this->json_input = R"("blergh")";
        this->expected = true;
    } else if constexpr (std::is_same_v<TypeParam, label_selection>) {
        this->json_input = R"("key")";
        this->expected = label_selection{.key = "test/key1", .value = "value1"};
    } else {
        static_assert(always_false_v<TypeParam>, "not implemented");
    }

    json::Document doc;
    ASSERT_NO_THROW(doc.Parse(this->json_input));
    ASSERT_FALSE(doc.HasParseError()) << fmt::format(
      "Malformed json schema: {} at offset {}",
      rapidjson::GetParseError_En(doc.GetParseError()),
      doc.GetErrorOffset());

    debug_bundle::result<TypeParam> res{outcome::success()};

    ASSERT_NO_THROW(res = from_json<TypeParam>(doc));
    ASSERT_TRUE(res.has_error());
    ASSERT_FALSE(res.has_exception());
    EXPECT_EQ(res.assume_error().code(), error_code::invalid_parameters);
    EXPECT_TRUE(res.assume_error().message().starts_with("Failed to parse"))
      << res.assume_error().message();
}

TYPED_TEST(JsonTypeTest, ValidateControlCharacters) {
    if constexpr (std::is_same_v<TypeParam, ss::sstring>) {
        this->json_input = R"("foo\nbar")";
        this->expected = "foo\nbar";
    } else if constexpr (std::is_same_v<TypeParam, scram_creds>) {
        this->json_input
          = R"({"username": "user\r", "password": "pass", "mechanism": "SCRAM-SHA-256"})";
        this->expected = scram_creds{
          .username{"user"}, .password{"pass"}, .mechanism{"SCRAM-SHA-256"}};
    } else if constexpr (std::
                           is_same_v<TypeParam, debug_bundle_authn_options>) {
        this->json_input
          = R"({"username": "user", "password": "\fpass", "mechanism": "SCRAM-SHA-256"})";
        this->expected = TypeParam{scram_creds{
          .username{"user"}, .password{"pass"}, .mechanism{"SCRAM-SHA-256"}}};
    } else {
        return;
    }

    json::Document doc;
    ASSERT_NO_THROW(doc.Parse(this->json_input));
    ASSERT_FALSE(doc.HasParseError()) << fmt::format(
      "Malformed json schema: {} at offset {}",
      rapidjson::GetParseError_En(doc.GetParseError()),
      doc.GetErrorOffset());

    debug_bundle::result<TypeParam> res{outcome::success()};

    ASSERT_NO_THROW(res = from_json<TypeParam>(doc));
    ASSERT_TRUE(res.has_error());
    ASSERT_FALSE(res.has_exception());
    EXPECT_EQ(res.assume_error().code(), error_code::invalid_parameters);
    EXPECT_TRUE(
      res.assume_error().message().find("invalid control character")
      != std::string::npos)
      << res.assume_error().message();
}
