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

#include <gtest/gtest.h>

namespace cluster_link::model::tests {

TEST(test_model, test_no_leak_private_data) {
    scram_credentials creds{
      .username = "user", .password = "pass", .mechanism = "SCRAM-SHA-256"};

    EXPECT_EQ(
      "{username: user, password: ****, mechanism: SCRAM-SHA-256}",
      fmt::format("{}", creds));

    connection_config config_files{
      .bootstrap_servers = {net::unresolved_address{"localhost", 9092}},
      .authn_config = creds,
      .cert = tls_file_path{"cert.pem"},
      .key = tls_file_path{"key.pem"},
      .ca = tls_file_path{"ca.pem"},
      .client_id = "client-id"};

    EXPECT_EQ(
      "{bootstrap_servers: [{host: localhost, port: 9092}], "
      "authn_config: {username: user, password: ****, mechanism: "
      "SCRAM-SHA-256}, tls_enabled: false, cert: {file: cert.pem}, key: {file: "
      "key.pem}, ca: {file: ca.pem}, client_id: client-id, "
      "metadata_max_age_ms: {nullopt}, connection_timeout_ms: {nullopt}, "
      "retry_backoff_ms: {nullopt}, fetch_wait_max_ms: {nullopt}, "
      "fetch_min_bytes: {nullopt}, fetch_max_bytes: {nullopt}}",
      fmt::format("{}", config_files));

    connection_config config_values{
      .bootstrap_servers = {net::unresolved_address{"localhost", 9092}},
      .authn_config = creds,
      .cert = tls_value{"cert.pem"},
      .key = tls_value{"key.pem"},
      .ca = tls_value{"ca.pem"},
      .client_id = "client-id"};
    EXPECT_EQ(
      "{bootstrap_servers: [{host: localhost, port: 9092}], "
      "authn_config: {username: user, password: ****, mechanism: "
      "SCRAM-SHA-256}, tls_enabled: false, cert: {value: cert.pem}, key: "
      "{value: ****}, ca: {value: ca.pem}, client_id: client-id, "
      "metadata_max_age_ms: {nullopt}, connection_timeout_ms: {nullopt}, "
      "retry_backoff_ms: {nullopt}, fetch_wait_max_ms: {nullopt}, "
      "fetch_min_bytes: {nullopt}, fetch_max_bytes: {nullopt}}",
      fmt::format("{}", config_values));
}
} // namespace cluster_link::model::tests
