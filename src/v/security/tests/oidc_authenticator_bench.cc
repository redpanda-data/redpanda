/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/vassert.h"
#include "json/document.h"
#include "security/jwt.h"
#include "security/oidc_authenticator.h"
#include "security/oidc_principal_mapping.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/testing/perf_tests.hh>

#include <boost/algorithm/string/join.hpp>
#include <container/chunked_vector.h>

namespace {

using namespace security::oidc;
namespace rj = rapidjson;

// Group representation format
enum class group_format : std::uint8_t {
    array, // JSON array: ["group1", "group2"]
    string // Comma-delimited string: "group1,group2"
};

constexpr const char* test_alg = "RS256";
constexpr const char* test_typ = "JWT";
constexpr const char* test_kid = "test-key-id";
constexpr const char* test_issuer = "https://issuer.example.com";
constexpr const char* test_user = "test-user";
constexpr const char* test_audience = "redpanda";
constexpr const char* test_groups_claim_key = "groups";
constexpr const char* test_groups_claim_pointer = "/groups";
constexpr std::string_view test_nested_group_prefix = "/a/b/";
constexpr std::time_t test_issued_at = 1695887942;
constexpr std::time_t test_expiration = 1695891542;
constexpr std::chrono::seconds clock_skew_tolerance{10};
const auto test_now = ss::lowres_system_clock::from_time_t(test_issued_at);

const principal_mapping_rule test_mapping_rule{};
const group_claim_policy test_group_policy_none{
  json::Pointer(test_groups_claim_pointer), nested_group_behavior::none};
const group_claim_policy test_group_policy_suffix{
  json::Pointer(test_groups_claim_pointer), nested_group_behavior::suffix};

json::Document make_jwt_header() {
    json::Document header;
    header.SetObject();
    auto& allocator = header.GetAllocator();

    header.AddMember("alg", rj::StringRef(test_alg), allocator);
    header.AddMember("typ", rj::StringRef(test_typ), allocator);
    header.AddMember("kid", rj::StringRef(test_kid), allocator);

    return header;
}

json::Document make_jwt_payload(
  size_t num_groups, group_format format, nested_group_behavior ngb) {
    json::Document payload;
    payload.SetObject();
    auto& allocator = payload.GetAllocator();

    // Add required claims
    payload.AddMember("iss", rj::StringRef(test_issuer), allocator);
    payload.AddMember("sub", rj::StringRef(test_user), allocator);
    payload.AddMember("aud", rj::StringRef(test_audience), allocator);
    payload.AddMember("exp", static_cast<int64_t>(test_expiration), allocator);
    payload.AddMember("iat", static_cast<int64_t>(test_issued_at), allocator);

    if (num_groups == 0) {
        return payload;
    }

    // Generate group names
    chunked_vector<ss::sstring> group_names;
    group_names.reserve(num_groups);
    for (size_t i = 0; i < num_groups; ++i) {
        group_names.emplace_back(
          ssx::sformat(
            "{}group-{}",
            ngb == nested_group_behavior::suffix ? test_nested_group_prefix
                                                 : "",
            i));
    }

    json::Value groups;

    if (format == group_format::array) {
        // Array format: ["group-0", "group-1", ...]
        groups.SetArray();
        groups.Reserve(num_groups, allocator);
        for (const auto& name : group_names) {
            json::Value group_name;
            group_name.SetString(name.data(), name.size(), allocator);
            groups.PushBack(std::move(group_name), allocator);
        }
    } else {
        // String format: "group-0,group-1,..."
        auto groups_str = boost::algorithm::join(group_names, ",");
        groups.SetString(groups_str.c_str(), groups_str.size(), allocator);
    }

    payload.AddMember(
      rj::StringRef(test_groups_claim_key), std::move(groups), allocator);

    return payload;
}

jwt make_test_jwt(
  size_t num_groups,
  group_format format = group_format::array,
  nested_group_behavior ngb = nested_group_behavior::none) {
    auto jwt_result = jwt::make(
      make_jwt_header(), make_jwt_payload(num_groups, format, ngb));
    vassert(jwt_result.has_value(), "Failed to create test JWT");
    return std::move(jwt_result).assume_value();
}

// JWT with 0 groups
const jwt jwt_0_groups = make_test_jwt(0);

// JWTs with groups as an array, not nested
const jwt jwt_1_group_as_array_not_nested = make_test_jwt(
  1, group_format::array, nested_group_behavior::none);
const jwt jwt_10_groups_as_array_not_nested = make_test_jwt(
  10, group_format::array, nested_group_behavior::none);
const jwt jwt_100_groups_as_array_not_nested = make_test_jwt(
  100, group_format::array, nested_group_behavior::none);
const jwt jwt_1000_groups_as_array_not_nested = make_test_jwt(
  1000, group_format::array, nested_group_behavior::none);

// JWTs with groups as a comma-delimited string, not nested
const jwt jwt_1_group_as_string_not_nested = make_test_jwt(
  1, group_format::string, nested_group_behavior::none);
const jwt jwt_10_groups_as_string_not_nested = make_test_jwt(
  10, group_format::string, nested_group_behavior::none);
const jwt jwt_100_groups_as_string_not_nested = make_test_jwt(
  100, group_format::string, nested_group_behavior::none);
const jwt jwt_1000_groups_as_string_not_nested = make_test_jwt(
  1000, group_format::string, nested_group_behavior::none);

// JWTs with groups as an array, nested
const jwt jwt_1_group_as_array_nested = make_test_jwt(
  1, group_format::array, nested_group_behavior::suffix);
const jwt jwt_10_groups_as_array_nested = make_test_jwt(
  10, group_format::array, nested_group_behavior::suffix);
const jwt jwt_100_groups_as_array_nested = make_test_jwt(
  100, group_format::array, nested_group_behavior::suffix);
const jwt jwt_1000_groups_as_array_nested = make_test_jwt(
  1000, group_format::array, nested_group_behavior::suffix);

// JWTs with groups as a comma-delimited string, nested
const jwt jwt_1_group_as_string_nested = make_test_jwt(
  1, group_format::string, nested_group_behavior::suffix);
const jwt jwt_10_groups_as_string_nested = make_test_jwt(
  10, group_format::string, nested_group_behavior::suffix);
const jwt jwt_100_groups_as_string_nested = make_test_jwt(
  100, group_format::string, nested_group_behavior::suffix);
const jwt jwt_1000_groups_as_string_nested = make_test_jwt(
  1000, group_format::string, nested_group_behavior::suffix);

void run_authenticate(
  const jwt& jwt_token, const group_claim_policy& group_policy) {
    result<authentication_data> result = authenticate(
      jwt_token,
      test_mapping_rule,
      group_policy,
      test_issuer,
      test_audience,
      clock_skew_tolerance,
      test_now);
    perf_tests::do_not_optimize(result);
}

} // namespace

PERF_TEST(oidc_authenticator_bench, 0_groups) {
    run_authenticate(jwt_0_groups, test_group_policy_none);
}

PERF_TEST(oidc_authenticator_bench, 1_group_as_array_not_nested) {
    run_authenticate(jwt_1_group_as_array_not_nested, test_group_policy_none);
}

PERF_TEST(oidc_authenticator_bench, 10_groups_as_array_not_nested) {
    run_authenticate(jwt_10_groups_as_array_not_nested, test_group_policy_none);
}

PERF_TEST(oidc_authenticator_bench, 100_groups_as_array_not_nested) {
    run_authenticate(
      jwt_100_groups_as_array_not_nested, test_group_policy_none);
}

PERF_TEST(oidc_authenticator_bench, 1000_groups_as_array_not_nested) {
    run_authenticate(
      jwt_1000_groups_as_array_not_nested, test_group_policy_none);
}

PERF_TEST(oidc_authenticator_bench, 1_group_as_string_not_nested) {
    run_authenticate(jwt_1_group_as_string_not_nested, test_group_policy_none);
}

PERF_TEST(oidc_authenticator_bench, 10_groups_as_string_not_nested) {
    run_authenticate(
      jwt_10_groups_as_string_not_nested, test_group_policy_none);
}

PERF_TEST(oidc_authenticator_bench, 100_groups_as_string_not_nested) {
    run_authenticate(
      jwt_100_groups_as_string_not_nested, test_group_policy_none);
}

PERF_TEST(oidc_authenticator_bench, 1000_groups_as_string_not_nested) {
    run_authenticate(
      jwt_1000_groups_as_string_not_nested, test_group_policy_none);
}

PERF_TEST(oidc_authenticator_bench, 1_group_as_array_nested) {
    run_authenticate(jwt_1_group_as_array_nested, test_group_policy_suffix);
}

PERF_TEST(oidc_authenticator_bench, 10_groups_as_array_nested) {
    run_authenticate(jwt_10_groups_as_array_nested, test_group_policy_suffix);
}

PERF_TEST(oidc_authenticator_bench, 100_groups_as_array_nested) {
    run_authenticate(jwt_100_groups_as_array_nested, test_group_policy_suffix);
}

PERF_TEST(oidc_authenticator_bench, 1000_groups_as_array_nested) {
    run_authenticate(jwt_1000_groups_as_array_nested, test_group_policy_suffix);
}

PERF_TEST(oidc_authenticator_bench, 1_group_as_string_nested) {
    run_authenticate(jwt_1_group_as_string_nested, test_group_policy_suffix);
}

PERF_TEST(oidc_authenticator_bench, 10_groups_as_string_nested) {
    run_authenticate(jwt_10_groups_as_string_nested, test_group_policy_suffix);
}

PERF_TEST(oidc_authenticator_bench, 100_groups_as_string_nested) {
    run_authenticate(jwt_100_groups_as_string_nested, test_group_policy_suffix);
}

PERF_TEST(oidc_authenticator_bench, 1000_groups_as_string_nested) {
    run_authenticate(
      jwt_1000_groups_as_string_nested, test_group_policy_suffix);
}
