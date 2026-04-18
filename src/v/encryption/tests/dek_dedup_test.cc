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
#include "encryption/dek_dedup.h"
#include "encryption/encryption_metadata_ser.h"
#include "encryption/types.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "storage/record_batch_builder.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

namespace {

encryption::dek_state make_dek_state(
  ss::sstring kek_name, uint32_t version, ss::sstring kms_type = "mock") {
    encryption::dek_state state;
    state.kek_name = kek_name;
    state.kms_type = kms_type;
    state.kms_key_id = "key-1";
    state.encrypted_dek = bytes::from_string("encrypted-" + kek_name);
    state.algorithm = encryption::dek_algorithm::aes256_gcm;
    state.version = version;
    return state;
}

model::record_batch make_plain_batch(int num_records) {
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

ss::future<model::record_batch>
make_encrypted_batch(int num_records, const encryption::dek_set& deks) {
    auto batch = make_plain_batch(num_records);
    co_return co_await encryption::inject_encryption_headers(
      std::move(batch), deks);
}

bool has_encryption_header(const model::record_batch& batch) {
    bool found = false;
    batch.for_each_record([&](model::record rec) {
        for (const auto& h : rec.headers()) {
            if (
              h.key().linearize_to_string()
              == ss::sstring{encryption::encryption_header_key}) {
                found = true;
            }
        }
    });
    return found;
}

/// Return true when the first record of the batch carries an rp.encryption
/// header whose value is the empty sentinel (zero-length iobuf).
bool first_record_has_sentinel(const model::record_batch& batch) {
    bool found = false;
    batch.for_each_record([&](model::record rec) {
        // Only inspect the first record.
        for (const auto& h : rec.headers()) {
            if (
              h.key().linearize_to_string()
                == ss::sstring{encryption::encryption_header_key}
              && h.value_size() == 0) {
                found = true;
            }
        }
        return ss::stop_iteration::yes;
    });
    return found;
}

ss::future<std::optional<encryption::dek_set>>
get_batch_deks(const model::record_batch& batch) {
    co_return co_await encryption::extract_encryption_metadata(batch);
}

} // namespace

TEST_CORO(dek_dedup, first_batch_keeps_dek) {
    encryption::dek_set deks;
    auto state_a = make_dek_state("kek-a", 1);
    deks.emplace("kek-a", state_a);

    auto batch = co_await make_encrypted_batch(3, deks);
    encryption::seen_dek_set seen;

    auto result = co_await encryption::strip_duplicate_dek_headers(
      std::move(batch), seen);

    // DEK A should be kept (it was new)
    EXPECT_TRUE(has_encryption_header(result));
    auto extracted = co_await get_batch_deks(result);
    EXPECT_TRUE(extracted.has_value());
    EXPECT_EQ(extracted->size(), 1);
    EXPECT_TRUE(extracted->contains("kek-a"));
    EXPECT_EQ(extracted->at("kek-a").version, 1u);

    // Seen set should now contain DEK A
    EXPECT_EQ(seen.size(), 1);
    EXPECT_TRUE(
      seen.contains(encryption::dek_id{.kek_name = "kek-a", .dek_version = 1}));
}

TEST_CORO(dek_dedup, duplicate_dek_stripped) {
    encryption::dek_set deks;
    auto state_a = make_dek_state("kek-a", 1);
    deks.emplace("kek-a", state_a);

    auto batch = co_await make_encrypted_batch(3, deks);

    // Pre-populate the seen set with DEK A
    encryption::seen_dek_set seen;
    seen.emplace(encryption::dek_id{.kek_name = "kek-a", .dek_version = 1});

    auto result = co_await encryption::strip_duplicate_dek_headers(
      std::move(batch), seen);

    // DEK A was already seen, so the encryption header should be a sentinel
    // (key present, empty value)
    EXPECT_TRUE(has_encryption_header(result));
    EXPECT_TRUE(first_record_has_sentinel(result));

    // Seen set should still have just one entry
    EXPECT_EQ(seen.size(), 1);
}

TEST_CORO(dek_dedup, sentinel_preserved_on_full_strip) {
    encryption::dek_set deks;
    auto state_a = make_dek_state("kek-a", 1);
    deks.emplace("kek-a", state_a);

    auto batch = co_await make_encrypted_batch(3, deks);

    // Pre-populate the seen set with DEK A
    encryption::seen_dek_set seen;
    seen.emplace(encryption::dek_id{.kek_name = "kek-a", .dek_version = 1});

    auto result = co_await encryption::strip_duplicate_dek_headers(
      std::move(batch), seen);

    // The rp.encryption key must be present with an empty value sentinel
    EXPECT_TRUE(has_encryption_header(result));
    EXPECT_TRUE(first_record_has_sentinel(result));

    // Extracting DEK metadata from a sentinel should yield an empty DEK set
    auto extracted = co_await get_batch_deks(result);
    EXPECT_TRUE(extracted.has_value());
    EXPECT_EQ(extracted->size(), 0);
}

TEST_CORO(dek_dedup, mixed_new_and_duplicate) {
    encryption::dek_set deks;
    auto state_a = make_dek_state("kek-a", 1);
    auto state_b = make_dek_state("kek-b", 2);
    deks.emplace("kek-a", state_a);
    deks.emplace("kek-b", state_b);

    auto batch = co_await make_encrypted_batch(3, deks);

    // Pre-populate with DEK A only
    encryption::seen_dek_set seen;
    seen.emplace(encryption::dek_id{.kek_name = "kek-a", .dek_version = 1});

    auto result = co_await encryption::strip_duplicate_dek_headers(
      std::move(batch), seen);

    // DEK A should be stripped, DEK B should be kept with a non-empty value
    // (not a sentinel)
    EXPECT_TRUE(has_encryption_header(result));
    EXPECT_FALSE(first_record_has_sentinel(result));
    auto extracted = co_await get_batch_deks(result);
    EXPECT_TRUE(extracted.has_value());
    EXPECT_EQ(extracted->size(), 1);
    EXPECT_TRUE(extracted->contains("kek-b"));
    EXPECT_EQ(extracted->at("kek-b").version, 2u);

    // Seen set should now have both
    EXPECT_EQ(seen.size(), 2);
    EXPECT_TRUE(
      seen.contains(encryption::dek_id{.kek_name = "kek-a", .dek_version = 1}));
    EXPECT_TRUE(
      seen.contains(encryption::dek_id{.kek_name = "kek-b", .dek_version = 2}));
}

TEST_CORO(dek_dedup, reset_after_index_entry) {
    encryption::dek_set deks;
    auto state_a = make_dek_state("kek-a", 1);
    deks.emplace("kek-a", state_a);

    encryption::seen_dek_set seen;

    // First batch: DEK A is new
    auto batch1 = co_await make_encrypted_batch(3, deks);
    auto result1 = co_await encryption::strip_duplicate_dek_headers(
      std::move(batch1), seen);
    EXPECT_TRUE(has_encryption_header(result1));

    // Second batch: DEK A is duplicate -- sentinel expected
    auto batch2 = co_await make_encrypted_batch(3, deks);
    auto result2 = co_await encryption::strip_duplicate_dek_headers(
      std::move(batch2), seen);
    EXPECT_TRUE(has_encryption_header(result2));
    EXPECT_TRUE(first_record_has_sentinel(result2));

    // Simulate index entry creation by clearing the seen set
    seen.clear();

    // Third batch: DEK A should appear again after reset
    auto batch3 = co_await make_encrypted_batch(3, deks);
    auto result3 = co_await encryption::strip_duplicate_dek_headers(
      std::move(batch3), seen);
    EXPECT_TRUE(has_encryption_header(result3));
    auto extracted = co_await get_batch_deks(result3);
    EXPECT_TRUE(extracted.has_value());
    EXPECT_EQ(extracted->size(), 1);
    EXPECT_TRUE(extracted->contains("kek-a"));
}

TEST_CORO(dek_dedup, no_encryption_header_passthrough) {
    auto batch = make_plain_batch(3);
    encryption::seen_dek_set seen;

    auto result = co_await encryption::strip_duplicate_dek_headers(
      std::move(batch), seen);

    // No encryption header on input -- no modification
    EXPECT_FALSE(has_encryption_header(result));

    // Seen set should remain empty
    EXPECT_EQ(seen.size(), 0);
}

TEST_CORO(dek_dedup, same_kek_different_version_kept) {
    encryption::dek_set deks1;
    deks1.emplace("kek-a", make_dek_state("kek-a", 1));

    encryption::dek_set deks2;
    deks2.emplace("kek-a", make_dek_state("kek-a", 2));

    encryption::seen_dek_set seen;

    // First batch with version 1
    auto batch1 = co_await make_encrypted_batch(3, deks1);
    auto result1 = co_await encryption::strip_duplicate_dek_headers(
      std::move(batch1), seen);
    EXPECT_TRUE(has_encryption_header(result1));

    // Second batch with version 2 (same kek_name, different version)
    auto batch2 = co_await make_encrypted_batch(3, deks2);
    auto result2 = co_await encryption::strip_duplicate_dek_headers(
      std::move(batch2), seen);
    // Should be kept since (kek-a, 2) differs from (kek-a, 1)
    EXPECT_TRUE(has_encryption_header(result2));
    auto extracted = co_await get_batch_deks(result2);
    EXPECT_TRUE(extracted.has_value());
    EXPECT_EQ(extracted->at("kek-a").version, 2u);

    // Both versions should be in seen set
    EXPECT_EQ(seen.size(), 2);
}
