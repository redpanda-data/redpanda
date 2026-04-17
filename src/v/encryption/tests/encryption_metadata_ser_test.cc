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

#include "bytes/bytes.h"
#include "encryption/encryption_metadata_ser.h"
#include "encryption/types.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "storage/record_batch_builder.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

namespace {

encryption::dek_state make_dek_state(
  ss::sstring kek_name,
  ss::sstring kms_type,
  ss::sstring kms_key_id,
  bytes encrypted_dek,
  encryption::dek_algorithm algo,
  uint32_t version) {
    encryption::dek_state state;
    state.kek_name = std::move(kek_name);
    state.kms_type = std::move(kms_type);
    state.kms_key_id = std::move(kms_key_id);
    state.encrypted_dek = std::move(encrypted_dek);
    state.algorithm = algo;
    state.version = version;
    return state;
}

model::record_batch make_test_batch(int num_records) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset{0});
    for (int i = 0; i < num_records; ++i) {
        iobuf value;
        auto msg = fmt::format("value-{}", i);
        value.append(msg.data(), msg.size());
        builder.add_raw_kv(std::nullopt, std::move(value));
    }
    return std::move(builder).build();
}

void verify_dek_state(
  const encryption::dek_state& actual, const encryption::dek_state& expected) {
    EXPECT_EQ(actual.kek_name, expected.kek_name);
    EXPECT_EQ(actual.kms_type, expected.kms_type);
    EXPECT_EQ(actual.kms_key_id, expected.kms_key_id);
    EXPECT_EQ(actual.encrypted_dek, expected.encrypted_dek);
    EXPECT_EQ(actual.algorithm, expected.algorithm);
    EXPECT_EQ(actual.version, expected.version);
}

} // namespace

TEST_CORO(encryption_metadata_ser, serialize_single_dek) {
    encryption::dek_set deks;
    auto state = make_dek_state(
      "test-kek",
      "aws-kms",
      "arn:aws:kms:us-east-1:123:key/abc",
      bytes::from_string("encrypted-key-material"),
      encryption::dek_algorithm::aes256_gcm,
      1);
    deks.emplace("test-kek", state);

    auto serialized = co_await encryption::serialize_encryption_metadata(deks);
    EXPECT_GT(serialized.size_bytes(), 0);

    auto deserialized = co_await encryption::deserialize_encryption_metadata(
      std::move(serialized));
    EXPECT_EQ(deserialized.size(), 1);
    EXPECT_TRUE(deserialized.contains("test-kek"));
    verify_dek_state(deserialized.at("test-kek"), state);
}

TEST_CORO(encryption_metadata_ser, serialize_multiple_deks) {
    encryption::dek_set deks;
    auto state1 = make_dek_state(
      "kek-alpha",
      "aws-kms",
      "arn:aws:kms:us-east-1:123:key/alpha",
      bytes::from_string("encrypted-alpha"),
      encryption::dek_algorithm::aes256_gcm,
      1);
    auto state2 = make_dek_state(
      "kek-beta",
      "gcp-kms",
      "projects/p/locations/l/keyRings/r/cryptoKeys/k",
      bytes::from_string("encrypted-beta"),
      encryption::dek_algorithm::aes128_gcm,
      3);
    deks.emplace("kek-alpha", state1);
    deks.emplace("kek-beta", state2);

    auto serialized = co_await encryption::serialize_encryption_metadata(deks);
    auto deserialized = co_await encryption::deserialize_encryption_metadata(
      std::move(serialized));

    EXPECT_EQ(deserialized.size(), 2);
    EXPECT_TRUE(deserialized.contains("kek-alpha"));
    EXPECT_TRUE(deserialized.contains("kek-beta"));
    verify_dek_state(deserialized.at("kek-alpha"), state1);
    verify_dek_state(deserialized.at("kek-beta"), state2);
}

TEST_CORO(encryption_metadata_ser, inject_header_into_batch) {
    auto batch = make_test_batch(3);
    encryption::dek_set deks;
    deks.emplace(
      "inject-kek",
      make_dek_state(
        "inject-kek",
        "mock",
        "key-1",
        bytes::from_string("wrapped-dek"),
        encryption::dek_algorithm::aes256_gcm,
        1));

    auto injected = co_await encryption::inject_encryption_headers(
      std::move(batch), deks);

    int record_idx = 0;
    injected.for_each_record([&](model::record rec) {
        bool found = false;
        for (const auto& h : rec.headers()) {
            auto key_str = h.key().linearize_to_string();
            if (key_str == ss::sstring{encryption::encryption_header_key}) {
                found = true;
                if (record_idx == 0) {
                    EXPECT_GT(h.value_size(), 0)
                      << "first record should have non-empty value";
                } else {
                    EXPECT_EQ(h.value_size(), 0)
                      << "record " << record_idx
                      << " should have empty sentinel value";
                }
            }
        }
        EXPECT_TRUE(found) << "record " << record_idx
                           << " should have rp.encryption header";
        ++record_idx;
    });
    EXPECT_EQ(record_idx, 3);
}

TEST_CORO(encryption_metadata_ser, extract_header_from_batch) {
    auto batch = make_test_batch(3);
    encryption::dek_set deks;
    auto state = make_dek_state(
      "extract-kek",
      "aws-kms",
      "key-42",
      bytes::from_string("secret-material"),
      encryption::dek_algorithm::aes256_siv,
      7);
    deks.emplace("extract-kek", state);

    auto injected = co_await encryption::inject_encryption_headers(
      std::move(batch), deks);
    auto extracted = co_await encryption::extract_encryption_metadata(injected);

    EXPECT_TRUE(extracted.has_value());
    EXPECT_EQ(extracted->size(), 1);
    EXPECT_TRUE(extracted->contains("extract-kek"));
    verify_dek_state(extracted->at("extract-kek"), state);
}

TEST_CORO(encryption_metadata_ser, dedup_within_batch) {
    auto batch = make_test_batch(5);
    encryption::dek_set deks;
    deks.emplace(
      "dedup-kek",
      make_dek_state(
        "dedup-kek",
        "mock",
        "key-9",
        bytes::from_string("dedup-material"),
        encryption::dek_algorithm::aes128_gcm,
        2));

    auto injected = co_await encryption::inject_encryption_headers(
      std::move(batch), deks);

    int header_count = 0;
    int non_empty_count = 0;
    injected.for_each_record([&](model::record rec) {
        for (const auto& h : rec.headers()) {
            if (
              h.key().linearize_to_string()
              == ss::sstring{encryption::encryption_header_key}) {
                ++header_count;
                if (h.value_size() > 0) {
                    ++non_empty_count;
                }
            }
        }
    });
    EXPECT_EQ(header_count, 5)
      << "all records should have rp.encryption header";
    EXPECT_EQ(non_empty_count, 1)
      << "only first record should have non-empty value";
}

TEST_CORO(encryption_metadata_ser, sentinel_has_empty_value) {
    auto batch = make_test_batch(3);
    encryption::dek_set deks;
    deks.emplace(
      "sentinel-kek",
      make_dek_state(
        "sentinel-kek",
        "mock",
        "key-5",
        bytes::from_string("sentinel-material"),
        encryption::dek_algorithm::aes256_gcm,
        1));

    auto injected = co_await encryption::inject_encryption_headers(
      std::move(batch), deks);

    int record_idx = 0;
    injected.for_each_record([&](model::record rec) {
        bool found = false;
        for (const auto& h : rec.headers()) {
            if (
              h.key().linearize_to_string()
              == ss::sstring{encryption::encryption_header_key}) {
                found = true;
                if (record_idx == 0) {
                    EXPECT_GT(h.value_size(), 0)
                      << "record 0 should have non-empty encryption metadata";
                } else {
                    EXPECT_EQ(h.value_size(), 0)
                      << "record " << record_idx
                      << " should have empty sentinel value";
                }
            }
        }
        EXPECT_TRUE(found) << "record " << record_idx
                           << " should have rp.encryption header";
        ++record_idx;
    });
    EXPECT_EQ(record_idx, 3);
}

TEST_CORO(encryption_metadata_ser, no_encryption_header_returns_nullopt) {
    auto batch = make_test_batch(3);
    auto extracted = co_await encryption::extract_encryption_metadata(batch);
    EXPECT_FALSE(extracted.has_value());
}
