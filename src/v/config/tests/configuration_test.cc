// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/base_property.h"
#include "config/configuration.h"

#include <gtest/gtest.h>

ss::logger lg("config_test"); // NOLINT

namespace config {

// Test that configuration can be round-tripped through YAML. This mirrors the
// config export/import workflow and enables quick iteration on the
// encoding/decoding/etc.
TEST(ConfigurationTest, Roundtrip) {
    auto& cfg = config::shard_local_cfg();
    YAML::Node root_out = to_yaml(cfg, redact_secrets::no);

    lg.debug("Configuration as YAML: {}", root_out);

    try {
        cfg.read_yaml(root_out);
        YAML::Node root_in = to_yaml(cfg, redact_secrets::no);

        // Compare the two YAML strings.
        EXPECT_EQ(fmt::format("{}", root_out), fmt::format("{}", root_in));
    } catch (const std::exception& e) {
        FAIL() << e.what();
    }
}

TEST(oidc_http_proxy_credentials, password_is_secret) {
    auto& cfg = config::shard_local_cfg();
    EXPECT_TRUE(cfg.oidc_http_proxy_password.is_secret());
    EXPECT_FALSE(cfg.oidc_http_proxy_username.is_secret());
}

TEST(oidc_http_proxy_credentials, username_rejects_colon_and_control) {
    auto& cfg = config::shard_local_cfg();
    EXPECT_EQ(
      cfg.oidc_http_proxy_username.validate(std::optional<ss::sstring>{}),
      std::nullopt);
    EXPECT_EQ(
      cfg.oidc_http_proxy_username.validate(std::optional<ss::sstring>{"svc"}),
      std::nullopt);
    EXPECT_TRUE(cfg.oidc_http_proxy_username
                  .validate(std::optional<ss::sstring>{"has:colon"})
                  .has_value());
    EXPECT_TRUE(cfg.oidc_http_proxy_username
                  .validate(std::optional<ss::sstring>{"has\nnewline"})
                  .has_value());
}

TEST(oidc_http_proxy_credentials, password_allows_colon_rejects_control) {
    auto& cfg = config::shard_local_cfg();
    EXPECT_EQ(
      cfg.oidc_http_proxy_password.validate(std::optional<ss::sstring>{}),
      std::nullopt);
    EXPECT_EQ(
      cfg.oidc_http_proxy_password.validate(
        std::optional<ss::sstring>{"p@ss:word/ok"}),
      std::nullopt);
    EXPECT_TRUE(cfg.oidc_http_proxy_password
                  .validate(std::optional<ss::sstring>{"bad\rcr"})
                  .has_value());
}

}; // namespace config
