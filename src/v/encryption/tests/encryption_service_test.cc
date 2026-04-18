/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "encryption/encryption_service.h"
#include "encryption/encryption_services.h"
#include "encryption/schema_resolver.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

TEST_CORO(encryption_service, disabled_when_not_started) {
    encryption::encryption_service svc;

    EXPECT_FALSE(svc.is_enabled());
    EXPECT_EQ(svc.get_encryption_services(), nullptr);
    co_return;
}

TEST_CORO(encryption_service, disabled_when_kms_type_empty) {
    encryption::encryption_service svc;

    co_await svc.start(
      encryption::schema_fetcher{}, "", "default-key", "aes256_gcm", 3600);

    EXPECT_FALSE(svc.is_enabled());
    EXPECT_EQ(svc.get_encryption_services(), nullptr);

    co_await svc.stop();
}

TEST_CORO(encryption_service, enabled_with_mock_kms) {
    encryption::encryption_service svc;

    co_await svc.start(
      encryption::schema_fetcher{}, "mock", "default-key", "aes256_gcm", 3600);

    EXPECT_TRUE(svc.is_enabled());
    EXPECT_NE(svc.get_encryption_services(), nullptr);

    co_await svc.stop();
}

TEST_CORO(encryption_service, unknown_kms_type_disables) {
    encryption::encryption_service svc;

    // Unknown KMS type silently disables encryption rather than crashing.
    co_await svc.start(
      encryption::schema_fetcher{}, "unknown", "key", "aes256_gcm", 3600);

    EXPECT_FALSE(svc.is_enabled());
    EXPECT_EQ(svc.get_encryption_services(), nullptr);
}

TEST_CORO(encryption_service, services_bundle_valid) {
    encryption::encryption_service svc;

    co_await svc.start(
      encryption::schema_fetcher{}, "mock", "default-key", "aes256_gcm", 3600);

    auto* services = svc.get_encryption_services();
    ASSERT_NE_CORO(services, nullptr);

    // Verify the bundle references are usable by calling methods on them.
    // resolver: resolve a topic with no rules should return nullopt
    auto result = co_await services->resolver.resolve(
      model::topic("no-such-topic"));
    EXPECT_FALSE(result.has_value());

    co_await svc.stop();
}
