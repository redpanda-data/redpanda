/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "crypto/crypto.h"
#include "crypto_test_utils.h"
#include "test_utils/test.h"
#include "test_values.h"

#include <absl/container/flat_hash_map.h>
#include <gtest/gtest.h>

#include <iterator>
#include <type_traits>

using namespace test_values;

// NOLINTBEGIN
static const absl::flat_hash_map<crypto::digest_type, size_t> expected_sizes = {
  {crypto::digest_type::MD5, 16},
  {crypto::digest_type::SHA256, 32},
  {crypto::digest_type::SHA512, 64}};
// NOLINTEND

TEST(crypto_hashing, length) {
    for (auto&& [k, v] : expected_sizes) {
        EXPECT_EQ(crypto::digest_ctx::size(k), v)
          << "Mismatch on size for digest type " << k;
        crypto::digest_ctx ctx(k);
        EXPECT_EQ(ctx.size(), v)
          << "Mismatch on size for context using digest type " << k;
    }
}

TEST(crypto_hashing, md5_one_shot) {
    auto result = crypto::digest(crypto::digest_type::MD5, md5_test_val);
    EXPECT_EQ(result, md5_expected_val);
}

TEST(crypto_hashing, sha256_one_shot) {
    auto result = crypto::digest(crypto::digest_type::SHA256, sha256_test_val);
    EXPECT_EQ(result, sha256_expected_val);
}

TEST(crypto_hashing, sha512_one_shot) {
    auto result = crypto::digest(crypto::digest_type::SHA512, sha512_test_val);
    EXPECT_EQ(result, sha512_expected_val);
}

TEST(crypto_hashing, sha256_multipart_bytes) {
    crypto::digest_ctx ctx(crypto::digest_type::SHA256);
    ctx.update(sha256_test_val);
    bytes dgst(bytes::initialized_later{}, ctx.size());
    ASSERT_NO_THROW(std::move(ctx).final(dgst));
    EXPECT_EQ(dgst, sha256_expected_val);
}

TEST(crypto_hashing, sha256_multipart_string) {
    crypto::digest_ctx ctx(crypto::digest_type::SHA256);
    ctx.update(bytes_view_to_string_view(sha256_test_val));
    bytes dgst(bytes::initialized_later{}, ctx.size());
    ASSERT_NO_THROW(std::move(ctx).final(bytes_span_to_char_span(dgst)));
    EXPECT_EQ(dgst, sha256_expected_val);
}

TEST(crypto_hashing, sha256_bad_len) {
    crypto::digest_ctx ctx(crypto::digest_type::SHA256);
    ctx.update(sha256_test_val);
    bytes dgst(bytes::initialized_later{}, ctx.size() - 1);
    EXPECT_THROW(std::move(ctx).final(dgst), crypto::exception);
}

TEST(crypto_hashing, sha256_reset) {
    crypto::digest_ctx ctx(crypto::digest_type::SHA256);
    ctx.update(bytes_view_to_string_view(sha256_test_val));
    bytes dgst(bytes::initialized_later{}, ctx.size());
    ctx.reset(bytes_span_to_char_span(dgst));
    EXPECT_EQ(dgst, sha256_expected_val);
    ctx.update(sha256_test_val);
    bytes dgst2(bytes::initialized_later{}, ctx.size());
    ctx.reset(dgst2);
    EXPECT_EQ(dgst2, sha256_expected_val);
}
