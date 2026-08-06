// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/broker_authn_endpoint.h"
#include "config/endpoint_tls_config.h"
#include "config/tls_config.h"
#include "config/validators.h"
#include "model/metadata.h"

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

namespace {

config::broker_authn_endpoint make_uds_ep(ss::sstring name, ss::sstring path) {
    return config::broker_authn_endpoint{
      .name = std::move(name),
      .address = {},
      .authn_method = std::nullopt,
      .unix_path = std::move(path),
      .unix_socket_mode = std::nullopt};
}

config::broker_authn_endpoint
make_inet_ep(ss::sstring name, ss::sstring host, uint16_t port) {
    return config::broker_authn_endpoint{
      .name = std::move(name),
      .address = net::unresolved_address(std::move(host), port),
      .authn_method = std::nullopt};
}

config::endpoint_tls_config make_enabled_tls(ss::sstring name) {
    // Construct a minimally-valid enabled tls_config. File paths are not
    // dereferenced by validate_kafka_uds_constraints — only the `is_enabled()`
    // flag is inspected.
    return config::endpoint_tls_config{
      .name = std::move(name),
      .config = config::tls_config{/*enabled=*/true,
                                   /*key_cert=*/std::nullopt,
                                   /*truststore=*/std::nullopt,
                                   /*crl=*/std::nullopt,
                                   /*require_client_auth=*/false}};
}

} // namespace

SEASTAR_THREAD_TEST_CASE(uds_tls_conflict_rejected) {
    std::vector<config::broker_authn_endpoint> kafka_api{
      make_uds_ep("internal", "/tmp/rp.sock")};
    std::vector<config::endpoint_tls_config> kafka_api_tls{
      make_enabled_tls("internal")};
    std::vector<model::broker_endpoint> advertised;
    auto err = config::validate_kafka_uds_constraints(
      kafka_api, kafka_api_tls, advertised);
    BOOST_REQUIRE(err.has_value());
    BOOST_TEST(
      std::string_view{*err}.find("TLS is not supported on UDS")
      != std::string_view::npos);
}

SEASTAR_THREAD_TEST_CASE(uds_disabled_tls_entry_allowed) {
    // An endpoint_tls_config entry whose name matches a UDS listener but is
    // disabled (is_enabled() == false) must NOT be treated as a conflict.
    std::vector<config::broker_authn_endpoint> kafka_api{
      make_uds_ep("internal", "/tmp/rp.sock")};
    std::vector<config::endpoint_tls_config> kafka_api_tls{
      config::endpoint_tls_config{
        .name = "internal", .config = config::tls_config{}}};
    std::vector<model::broker_endpoint> advertised;
    auto err = config::validate_kafka_uds_constraints(
      kafka_api, kafka_api_tls, advertised);
    BOOST_TEST(!err.has_value());
}

SEASTAR_THREAD_TEST_CASE(uds_advertise_rejected) {
    std::vector<config::broker_authn_endpoint> kafka_api{
      make_uds_ep("internal", "/tmp/rp.sock")};
    std::vector<config::endpoint_tls_config> kafka_api_tls;
    std::vector<model::broker_endpoint> advertised{model::broker_endpoint{
      "internal", net::unresolved_address("host", 9092)}};
    auto err = config::validate_kafka_uds_constraints(
      kafka_api, kafka_api_tls, advertised);
    BOOST_REQUIRE(err.has_value());
    BOOST_TEST(
      std::string_view{*err}.find("cannot advertise UDS")
      != std::string_view::npos);
}

SEASTAR_THREAD_TEST_CASE(mixed_uds_and_tcp_listeners_ok) {
    // A TCP listener and a UDS listener with different names coexist
    // cleanly. TLS on the TCP name does not touch the UDS name.
    std::vector<config::broker_authn_endpoint> kafka_api{
      make_inet_ep("external", "0.0.0.0", 9092),
      make_uds_ep("internal", "/tmp/rp.sock")};
    std::vector<config::endpoint_tls_config> kafka_api_tls{
      make_enabled_tls("external")};
    std::vector<model::broker_endpoint> advertised{model::broker_endpoint{
      "external", net::unresolved_address("broker-0.example.com", 9092)}};
    auto err = config::validate_kafka_uds_constraints(
      kafka_api, kafka_api_tls, advertised);
    BOOST_TEST(!err.has_value());
}
