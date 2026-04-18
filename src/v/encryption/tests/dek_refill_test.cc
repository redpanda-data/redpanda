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
#include "encryption/dek_refill.h"
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

/// Build a batch where record 0 has a full rp.encryption header and
/// records 1..N-1 have sentinel (empty) headers.
ss::future<model::record_batch>
make_batch_with_sentinels(int num_records, const encryption::dek_set& deks) {
    auto serialized = co_await encryption::serialize_encryption_metadata(deks);

    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset{0});

    for (int i = 0; i < num_records; ++i) {
        iobuf value;
        auto msg = fmt::format("value-{}", i);
        value.append(msg.data(), msg.size());

        chunked_vector<model::record_header> hdrs;
        iobuf hdr_key;
        hdr_key.append(
          encryption::encryption_header_key.data(),
          encryption::encryption_header_key.size());
        if (i == 0) {
            hdrs.emplace_back(std::move(hdr_key), serialized.copy());
        } else {
            hdrs.emplace_back(std::move(hdr_key), iobuf{});
        }

        builder.add_raw_kw(std::nullopt, std::move(value), std::move(hdrs));
    }

    co_return std::move(builder).build();
}

/// Build a batch where all records have sentinel (empty) rp.encryption
/// headers.
model::record_batch make_all_sentinel_batch(int num_records) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset{0});

    for (int i = 0; i < num_records; ++i) {
        iobuf value;
        auto msg = fmt::format("value-{}", i);
        value.append(msg.data(), msg.size());

        chunked_vector<model::record_header> hdrs;
        iobuf hdr_key;
        hdr_key.append(
          encryption::encryption_header_key.data(),
          encryption::encryption_header_key.size());
        hdrs.emplace_back(std::move(hdr_key), iobuf{});

        builder.add_raw_kw(std::nullopt, std::move(value), std::move(hdrs));
    }

    return std::move(builder).build();
}

/// Build a batch with no rp.encryption headers.
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

/// Build a batch where all records have full (non-empty) rp.encryption
/// headers.
ss::future<model::record_batch>
make_all_full_batch(int num_records, const encryption::dek_set& deks) {
    auto serialized = co_await encryption::serialize_encryption_metadata(deks);

    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset{0});

    for (int i = 0; i < num_records; ++i) {
        iobuf value;
        auto msg = fmt::format("value-{}", i);
        value.append(msg.data(), msg.size());

        chunked_vector<model::record_header> hdrs;
        iobuf hdr_key;
        hdr_key.append(
          encryption::encryption_header_key.data(),
          encryption::encryption_header_key.size());
        hdrs.emplace_back(std::move(hdr_key), serialized.copy());

        builder.add_raw_kw(std::nullopt, std::move(value), std::move(hdrs));
    }

    co_return std::move(builder).build();
}

/// Get the rp.encryption header value from a specific record index.
std::optional<iobuf>
get_encryption_header(const model::record_batch& batch, int record_idx) {
    std::optional<iobuf> result;
    int idx = 0;
    batch.for_each_record([&](model::record rec) {
        if (idx == record_idx) {
            for (const auto& h : rec.headers()) {
                if (
                  h.key().linearize_to_string()
                  == ss::sstring{encryption::encryption_header_key}) {
                    result = h.value().copy();
                }
            }
        }
        ++idx;
    });
    return result;
}

} // namespace

TEST_CORO(dek_refill, first_record_full_others_sentinel) {
    encryption::dek_set deks;
    deks.emplace("kek-a", make_dek_state("kek-a", 1));

    auto batch = co_await make_batch_with_sentinels(3, deks);
    auto result = co_await encryption::refill_dek_sentinels(
      std::move(batch), std::nullopt);

    // All three records should have the same non-empty header value.
    auto h0 = get_encryption_header(result.batch, 0);
    auto h1 = get_encryption_header(result.batch, 1);
    auto h2 = get_encryption_header(result.batch, 2);
    EXPECT_TRUE(h0.has_value());
    EXPECT_TRUE(h1.has_value());
    EXPECT_TRUE(h2.has_value());
    EXPECT_GT(h0->size_bytes(), 0u);
    EXPECT_GT(h1->size_bytes(), 0u);
    EXPECT_GT(h2->size_bytes(), 0u);

    // Verify all records have identical header content.
    EXPECT_EQ(*h0, *h1);
    EXPECT_EQ(*h0, *h2);

    // last_dek_metadata should be set.
    EXPECT_TRUE(result.last_dek_metadata.has_value());
    EXPECT_EQ(*result.last_dek_metadata, *h0);
}

TEST_CORO(dek_refill, all_sentinels_with_fallback) {
    // Create a fallback DEK metadata from a known dek_set.
    encryption::dek_set deks;
    deks.emplace("kek-b", make_dek_state("kek-b", 2));
    auto fallback = co_await encryption::serialize_encryption_metadata(deks);

    auto batch = make_all_sentinel_batch(3);
    auto result = co_await encryption::refill_dek_sentinels(
      std::move(batch), fallback.copy());

    // All three records should now have the fallback value.
    for (int i = 0; i < 3; ++i) {
        auto hdr = get_encryption_header(result.batch, i);
        EXPECT_TRUE(hdr.has_value());
        EXPECT_EQ(*hdr, fallback);
    }

    EXPECT_TRUE(result.last_dek_metadata.has_value());
    EXPECT_EQ(*result.last_dek_metadata, fallback);
}

TEST_CORO(dek_refill, no_encryption_headers_passthrough) {
    auto batch = make_plain_batch(3);
    auto original_data = batch.data().copy();

    auto result = co_await encryption::refill_dek_sentinels(
      std::move(batch), std::nullopt);

    // Batch should be returned unchanged.
    EXPECT_EQ(result.batch.data(), original_data);

    // last_dek_metadata should be nullopt.
    EXPECT_FALSE(result.last_dek_metadata.has_value());
}

TEST_CORO(dek_refill, new_full_header_updates_state) {
    encryption::dek_set deks;
    deks.emplace("kek-a", make_dek_state("kek-a", 1));
    auto expected = co_await encryption::serialize_encryption_metadata(deks);

    auto batch = co_await make_batch_with_sentinels(1, deks);
    auto result = co_await encryption::refill_dek_sentinels(
      std::move(batch), std::nullopt);

    // last_dek_metadata should contain DEK_A's serialized form.
    EXPECT_TRUE(result.last_dek_metadata.has_value());
    EXPECT_EQ(*result.last_dek_metadata, expected);
}

TEST_CORO(dek_refill, no_sentinels_no_rebuild) {
    encryption::dek_set deks;
    deks.emplace("kek-a", make_dek_state("kek-a", 1));

    auto batch = co_await make_all_full_batch(3, deks);
    auto original_data = batch.data().copy();

    auto result = co_await encryption::refill_dek_sentinels(
      std::move(batch), std::nullopt);

    // Batch data should be identical (no rebuild).
    EXPECT_EQ(result.batch.data(), original_data);

    // last_dek_metadata should be set.
    EXPECT_TRUE(result.last_dek_metadata.has_value());
    EXPECT_GT(result.last_dek_metadata->size_bytes(), 0u);
}
