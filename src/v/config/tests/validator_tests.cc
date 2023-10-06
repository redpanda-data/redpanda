// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/tests/constraints_utils.h"
#include "config/validators.h"

#include <seastar/testing/thread_test_case.hh>

SEASTAR_THREAD_TEST_CASE(test_client_groups_byte_rate_quota_invalid_config) {
    std::unordered_map<ss::sstring, config::client_group_quota> repeated_group{
      {"group1", {"group1", "group1", 1}},
      {"group2", {"group2", "group1", 1}},
      {"group3", {"group3", "group3", 1}}};
    auto result = config::validate_client_groups_byte_rate_quota(
      repeated_group);
    BOOST_TEST(result.has_value());
    BOOST_TEST(
      result->find("Group client prefix can not be prefix for another group")
      != ss::sstring::npos);

    std::unordered_map<ss::sstring, config::client_group_quota> prefix_group{
      {"group1", {"group1", "group1", 1}},
      {"special_group", {"special_group", "special_group", 1}},
      {"group", {"group", "group", 1}}};
    result = config::validate_client_groups_byte_rate_quota(prefix_group);
    BOOST_TEST(result.has_value());
    BOOST_TEST(
      result->find("Group client prefix can not be prefix for another group")
      != ss::sstring::npos);

    std::unordered_map<ss::sstring, config::client_group_quota> prefix_group_2{
      {"g", {"g", "g", 1}},
      {"group1", {"group1", "group1", 1}},
      {"group2", {"group2", "group2", 1}}};
    result = config::validate_client_groups_byte_rate_quota(prefix_group_2);
    BOOST_TEST(result.has_value());
    BOOST_TEST(
      result->find("Group client prefix can not be prefix for another group")
      != ss::sstring::npos);

    std::unordered_map<ss::sstring, config::client_group_quota> zero_rate{
      {"group1", {"group1", "group1", 1}}, {"group2", {"group2", "group2", 0}}};
    result = config::validate_client_groups_byte_rate_quota(zero_rate);
    BOOST_TEST(result.has_value());
    BOOST_TEST(
      result->find("Quota must be a non zero positive number")
      != ss::sstring::npos);

    std::unordered_map<ss::sstring, config::client_group_quota> negative_rate{
      {"group1", {"group1", "group1", 1}},
      {"group2", {"group2", "group2", -10}}};
    result = config::validate_client_groups_byte_rate_quota(negative_rate);
    BOOST_TEST(result.has_value());
    BOOST_TEST(
      result->find("Quota must be a non zero positive number")
      != ss::sstring::npos);

    std::unordered_map<ss::sstring, config::client_group_quota> valid_config{
      {"group1", {"group1", "group1", 9223372036854775807}},
      {"group2", {"group2", "group2", 1073741824}},
      {"another_group", {"another_group", "another_group", 1}}};
    result = config::validate_client_groups_byte_rate_quota(valid_config);
    BOOST_TEST(!result.has_value());
}

SEASTAR_THREAD_TEST_CASE(test_empty_string_vec) {
    using config::validate_non_empty_string_vec;
    BOOST_TEST(!(validate_non_empty_string_vec({"apple", "pear"}).has_value()));
    BOOST_TEST(validate_non_empty_string_vec({"apple", ""}).has_value());
    BOOST_TEST(validate_non_empty_string_vec({"", "pear"}).has_value());
    BOOST_TEST(
      validate_non_empty_string_vec({"apple", "", "pear"}).has_value());
}

SEASTAR_THREAD_TEST_CASE(test_empty_string_opt) {
    using config::validate_non_empty_string_opt;
    BOOST_TEST(!validate_non_empty_string_opt(std::nullopt).has_value());
    BOOST_TEST(!validate_non_empty_string_opt("apple").has_value());
    BOOST_TEST(validate_non_empty_string_opt("").has_value());
}

SEASTAR_THREAD_TEST_CASE(test_audit_event_types) {
    using config::validate_audit_event_types;
    BOOST_TEST(!validate_audit_event_types({"management",
                                            "produce",
                                            "consume",
                                            "describe",
                                            "heartbeat",
                                            "authenticate"})
                  .has_value());
    std::vector<ss::sstring> random_strings{"asdf", "fda", "hello", "world"};
    BOOST_TEST(validate_audit_event_types(random_strings).has_value());

    std::vector<ss::sstring> one_bad_apple{
      "management", "consume", "hello world", "heartbeat"};
    BOOST_TEST(validate_audit_event_types(one_bad_apple).has_value());
}

SEASTAR_THREAD_TEST_CASE(test_invalid_constraint_config) {
    // Constraint with min > max
    config::constraint_t invalid_integral_constraint(
      "default_topic_replications",
      config::constraint_type::restrikt,
      ss::make_shared<config::constraint_validator_range<int16_t>>(
        int16_t{9},
        int16_t{3},
        [](const auto&) { return int16_t{0}; },
        [](auto&, const auto&) {}));

    // Constraint with min < max
    config::constraint_t valid_integral_constraint(
      "default_topic_replications",
      config::constraint_type::restrikt,
      ss::make_shared<config::constraint_validator_range<int16_t>>(
        int16_t{3},
        int16_t{9},
        [](const auto&) { return int16_t{0}; },
        [](auto&, const auto&) {}));

    auto cfg = test_config();
    // Non-integral constraint
    config::constraint_t non_integral_constraint(
      "log_cleanup_policy",
      config::constraint_type::restrikt,
      ss::make_shared<
        config::constraint_validator_enabled<model::cleanup_policy_bitflags>>(
        config::constraint_enabled_t::yes,
        cfg.original_enum.bind(),
        [](const auto&) { return model::cleanup_policy_bitflags::none; },
        [](auto&, const auto&) {}));

    BOOST_TEST(
      config::validate_constraints({invalid_integral_constraint}).has_value());
    BOOST_TEST(!config::validate_constraints(
                  {valid_integral_constraint, non_integral_constraint})
                  .has_value());
}
