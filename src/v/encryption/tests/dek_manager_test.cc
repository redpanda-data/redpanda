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

#include "encryption/dek_manager.h"
#include "encryption/mock_kms_provider.h"
#include "test_utils/test.h"

#include <seastar/core/sleep.hh>

#include <gtest/gtest.h>

#include <chrono>

using namespace std::chrono_literals;

TEST_CORO(dek_manager, first_produce_generates_dek) {
    encryption::mock_kms_provider kms;
    encryption::dek_manager mgr(kms);

    auto dek = co_await mgr.get_or_create_dek(
      "test-subject",
      "test-kek",
      "mock",
      "key-1",
      encryption::dek_algorithm::aes256_gcm,
      std::nullopt);

    EXPECT_EQ(dek.version, 1);
    EXPECT_FALSE(dek.plaintext_dek.empty());
    EXPECT_FALSE(dek.encrypted_dek.empty());
    // Plaintext and encrypted must differ
    EXPECT_NE(dek.plaintext_dek, dek.encrypted_dek);
    // AES-256 DEK should be 32 bytes
    EXPECT_EQ(dek.plaintext_dek.size(), 32);
    EXPECT_EQ(dek.algorithm, encryption::dek_algorithm::aes256_gcm);
    EXPECT_EQ(dek.kek_name, "test-kek");
    EXPECT_EQ(dek.kms_type, "mock");
    EXPECT_EQ(dek.kms_key_id, "key-1");
    EXPECT_FALSE(dek.created_at.is_missing());
}

TEST_CORO(dek_manager, first_produce_aes128_generates_16_byte_dek) {
    encryption::mock_kms_provider kms;
    encryption::dek_manager mgr(kms);

    auto dek = co_await mgr.get_or_create_dek(
      "test-subject",
      "test-kek",
      "mock",
      "key-1",
      encryption::dek_algorithm::aes128_gcm,
      std::nullopt);

    EXPECT_EQ(dek.version, 1);
    // AES-128 DEK should be 16 bytes
    EXPECT_EQ(dek.plaintext_dek.size(), 16);
}

TEST_CORO(dek_manager, second_produce_uses_cached_dek) {
    encryption::mock_kms_provider kms;
    encryption::dek_manager mgr(kms);

    auto dek1 = co_await mgr.get_or_create_dek(
      "test-subject",
      "test-kek",
      "mock",
      "key-1",
      encryption::dek_algorithm::aes256_gcm,
      std::nullopt);

    auto dek2 = co_await mgr.get_or_create_dek(
      "test-subject",
      "test-kek",
      "mock",
      "key-1",
      encryption::dek_algorithm::aes256_gcm,
      std::nullopt);

    EXPECT_EQ(dek1.version, dek2.version);
    EXPECT_EQ(dek1.plaintext_dek, dek2.plaintext_dek);
    EXPECT_EQ(dek1.encrypted_dek, dek2.encrypted_dek);
    EXPECT_EQ(dek1.created_at, dek2.created_at);
}

TEST_CORO(dek_manager, expired_dek_triggers_rotation) {
    encryption::mock_kms_provider kms;
    encryption::dek_manager mgr(kms);

    // Use 0-second expiry so the DEK expires immediately
    auto dek1 = co_await mgr.get_or_create_dek(
      "test-subject",
      "test-kek",
      "mock",
      "key-1",
      encryption::dek_algorithm::aes256_gcm,
      std::chrono::seconds(0));

    EXPECT_EQ(dek1.version, 1);

    // Sleep briefly to ensure wall-clock advances past expiry
    co_await ss::sleep(1ms);

    auto dek2 = co_await mgr.get_or_create_dek(
      "test-subject",
      "test-kek",
      "mock",
      "key-1",
      encryption::dek_algorithm::aes256_gcm,
      std::chrono::seconds(0));

    EXPECT_EQ(dek2.version, 2);
    // New DEK should have different plaintext (random)
    EXPECT_NE(dek1.plaintext_dek, dek2.plaintext_dek);
    EXPECT_NE(dek1.encrypted_dek, dek2.encrypted_dek);
}

TEST_CORO(dek_manager, different_subjects_get_different_deks) {
    encryption::mock_kms_provider kms;
    encryption::dek_manager mgr(kms);

    auto dek1 = co_await mgr.get_or_create_dek(
      "subject-a",
      "test-kek",
      "mock",
      "key-1",
      encryption::dek_algorithm::aes256_gcm,
      std::nullopt);

    auto dek2 = co_await mgr.get_or_create_dek(
      "subject-b",
      "test-kek",
      "mock",
      "key-1",
      encryption::dek_algorithm::aes256_gcm,
      std::nullopt);

    EXPECT_NE(dek1.plaintext_dek, dek2.plaintext_dek);
    // Both should be version 1 (independent entries)
    EXPECT_EQ(dek1.version, 1);
    EXPECT_EQ(dek2.version, 1);
}

TEST_CORO(dek_manager, different_kek_names_get_different_deks) {
    encryption::mock_kms_provider kms;
    encryption::dek_manager mgr(kms);

    auto dek1 = co_await mgr.get_or_create_dek(
      "test-subject",
      "kek-a",
      "mock",
      "key-1",
      encryption::dek_algorithm::aes256_gcm,
      std::nullopt);

    auto dek2 = co_await mgr.get_or_create_dek(
      "test-subject",
      "kek-b",
      "mock",
      "key-1",
      encryption::dek_algorithm::aes256_gcm,
      std::nullopt);

    EXPECT_NE(dek1.plaintext_dek, dek2.plaintext_dek);
    EXPECT_EQ(dek1.version, 1);
    EXPECT_EQ(dek2.version, 1);
}
