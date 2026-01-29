// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/validators.h"

#include <seastar/testing/thread_test_case.hh>

#include <array>

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

SEASTAR_THREAD_TEST_CASE(test_iceberg_default_catalog_namespace) {
    using config::validate_iceberg_default_catalog_namespace;
    BOOST_TEST(validate_iceberg_default_catalog_namespace({}).has_value());
    BOOST_TEST(validate_iceberg_default_catalog_namespace({""}).has_value());
    BOOST_TEST(
      !validate_iceberg_default_catalog_namespace({"redpanda"}).has_value());
    BOOST_TEST(
      !validate_iceberg_default_catalog_namespace({"abc"}).has_value());
    BOOST_TEST(
      !validate_iceberg_default_catalog_namespace({"org", "db"}).has_value());
}

SEASTAR_THREAD_TEST_CASE(test_cloud_storage_cluster_name_validation) {
    using config::validate_cloud_storage_cluster_name;

    // Test nullopt case separately
    BOOST_TEST(!validate_cloud_storage_cluster_name(std::nullopt).has_value());

    // Valid test cases
    constexpr std::array valid_names{
      "valid-name",
      "valid_name",
      "ValidName123",
      "a",
      "123",
      "cluster-name_123"};

    for (const auto& name : valid_names) {
        BOOST_TEST(!validate_cloud_storage_cluster_name(name).has_value());
    }

    // Test maximum length (64 characters)
    std::string max_length_name(64, 'a');
    BOOST_TEST(
      !validate_cloud_storage_cluster_name(max_length_name).has_value());

    // Invalid test cases
    constexpr std::array invalid_names{
      "",              // Empty string
      "invalid.name",  // Dot
      "invalid name",  // Space
      "invalid@name",  // At symbol
      "invalid#name",  // Hash
      "invalid$name",  // Dollar
      "invalid%name",  // Percent
      "invalid/name",  // Forward slash
      "invalid\\name", // Backslash
      "invalid:name"   // Colon
    };

    for (const auto& name : invalid_names) {
        BOOST_TEST(validate_cloud_storage_cluster_name(name).has_value());
    }

    // Too long (65 characters)
    std::string too_long_name(65, 'a');
    BOOST_TEST(validate_cloud_storage_cluster_name(too_long_name).has_value());
}
