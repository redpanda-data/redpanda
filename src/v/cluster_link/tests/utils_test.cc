/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster_link/model/types.h"
#include "cluster_link/utils.h"

#include <gtest/gtest.h>

namespace cluster_link::tests {
TEST(cluster_link_utils_test, test_basic_config) {
    model::metadata md;
    md.connection.bootstrap_servers = {
      net::unresolved_address("localhost", 9092)};
    md.connection.client_id = "test-client";

    auto cfg = metadata_to_kafka_config(md);
    EXPECT_EQ(cfg.client_id, md.connection.client_id);
    EXPECT_EQ(cfg.initial_brokers, md.connection.bootstrap_servers);
    EXPECT_FALSE(cfg.broker_tls.has_value());
    EXPECT_FALSE(cfg.sasl_cfg.has_value());
}

TEST(cluster_link_utils_test, test_tls_file_config) {
    model::metadata md;
    md.connection.tls_enabled = model::connection_config::tls_enabled_t::yes;
    md.connection.ca = model::tls_file_path("/path/to/ca.crt");
    md.connection.cert = model::tls_file_path("/path/to/cert.crt");
    md.connection.key = model::tls_file_path("/path/to/key.key");

    auto cfg = metadata_to_kafka_config(md);
    EXPECT_FALSE(cfg.sasl_cfg.has_value());
    ASSERT_TRUE(cfg.broker_tls.has_value());
    auto& tls_cfg = cfg.broker_tls.value();
    EXPECT_TRUE(tls_cfg.provide_sni_hostname);
    ASSERT_TRUE(tls_cfg.truststore.has_value());
    ASSERT_TRUE(
      std::holds_alternative<std::filesystem::path>(
        tls_cfg.truststore.value()));
    EXPECT_EQ(
      std::get<std::filesystem::path>(tls_cfg.truststore.value()),
      std::filesystem::path("/path/to/ca.crt"));

    ASSERT_TRUE(tls_cfg.k_store.has_value());
    ASSERT_TRUE(
      std::holds_alternative<net::key_cert_path>(tls_cfg.k_store.value()));
    auto& k_store = std::get<net::key_cert_path>(tls_cfg.k_store.value());
    EXPECT_EQ(k_store.cert, std::filesystem::path("/path/to/cert.crt"));
    EXPECT_EQ(k_store.key, std::filesystem::path("/path/to/key.key"));
}

TEST(cluster_link_utils_test, test_tls_value_config) {
    model::metadata md;
    md.connection.tls_enabled = model::connection_config::tls_enabled_t::yes;
    md.connection.ca = model::tls_value("ca-cert");
    md.connection.cert = model::tls_value("cert-value");
    md.connection.key = model::tls_value("key-value");
    md.connection.tls_provide_sni
      = model::connection_config::tls_provide_sni_t::no;

    auto cfg = metadata_to_kafka_config(md);
    EXPECT_FALSE(cfg.sasl_cfg.has_value());

    ASSERT_TRUE(cfg.broker_tls.has_value());
    auto& tls_cfg = cfg.broker_tls.value();

    EXPECT_FALSE(tls_cfg.provide_sni_hostname);

    ASSERT_TRUE(tls_cfg.truststore.has_value());
    ASSERT_TRUE(
      std::holds_alternative<ss::sstring>(tls_cfg.truststore.value()));
    EXPECT_EQ(std::get<ss::sstring>(tls_cfg.truststore.value()), "ca-cert");

    ASSERT_TRUE(tls_cfg.k_store.has_value());
    ASSERT_TRUE(std::holds_alternative<net::key_cert>(tls_cfg.k_store.value()));
    auto& k_store = std::get<net::key_cert>(tls_cfg.k_store.value());
    EXPECT_EQ(k_store.cert, "cert-value");
    EXPECT_EQ(k_store.key, "key-value");
}

TEST(cluster_link_utils_test, test_tls_bad_combo) {
    GTEST_SKIP()
      << "TODO(death_tests): re-enable when death tests are made stable in CI.";
    model::metadata md;
    md.connection.tls_enabled = model::connection_config::tls_enabled_t::yes;
    md.connection.cert = model::tls_file_path("/path/to/cert.crt");
    md.connection.key = model::tls_value("key-value");
    EXPECT_DEATH(
      metadata_to_kafka_config(md),
      "TLS key and cert must be of the same type");
}

TEST(cluster_link_utils_test, test_sasl_config) {
    model::metadata md;
    md.connection.authn_config = model::scram_credentials{
      .username = "user", .password = "pass", .mechanism = "SCRAM-SHA-256"};

    auto cfg = metadata_to_kafka_config(md);
    ASSERT_TRUE(cfg.sasl_cfg.has_value());
    EXPECT_EQ(cfg.sasl_cfg->mechanism, "SCRAM-SHA-256");
    EXPECT_EQ(cfg.sasl_cfg->username, "user");
    EXPECT_EQ(cfg.sasl_cfg->password, "pass");
}
} // namespace cluster_link::tests
