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
#include "crypto/types.h"
#include "crypto_test_utils.h"
#include "test_utils/test.h"
#include "test_values.h"

#include <absl/container/flat_hash_map.h>
#include <gtest/gtest.h>

using namespace test_values;

// NOLINTBEGIN
static const absl::flat_hash_map<crypto::digest_type, size_t> expected_sizes = {
  {crypto::digest_type::SHA256, 32}, {crypto::digest_type::SHA512, 64}};
// NOLINTEND

TEST(crypto_hmac, length) {
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
    bytes fake_key(bytes::initialized_later{}, 16);
    std::fill(fake_key.begin(), fake_key.end(), 'k');
    for (auto&& [k, v] : expected_sizes) {
        EXPECT_EQ(crypto::hmac_ctx::size(k), v)
          << "Mismatch on HMAC size for digest type " << k;
        crypto::hmac_ctx ctx(k, fake_key);
        EXPECT_EQ(ctx.size(), v)
          << "Mismatch on HMAC size for context using digest type " << k;
    }
}

TEST(crypto_hmac, hmac_sha256_one_shot) {
    EXPECT_EQ(
      crypto::hmac(
        crypto::digest_type::SHA256, hmac_sha256_key, hmac_sha256_msg),
      hmac_sha256_expected);
}

TEST(crypto_hmac, hmac_sha512_one_shot) {
    EXPECT_EQ(
      crypto::hmac(
        crypto::digest_type::SHA512,
        bytes_view_to_string_view(hmac_sha512_key),
        bytes_view_to_string_view(hmac_sha512_msg)),
      hmac_sha512_expected);
}

TEST(crypto_hmac, hmac_sha256_multipart_bytes) {
    crypto::hmac_ctx ctx(crypto::digest_type::SHA256, hmac_sha256_key);
    ctx.update(hmac_sha256_msg);
    bytes sig(bytes::initialized_later{}, ctx.size());
    ASSERT_NO_THROW(std::move(ctx).final(sig));
    EXPECT_EQ(sig, hmac_sha256_expected);
}

TEST(crypto_hmac, hmac_sha256_multipart_string) {
    crypto::hmac_ctx ctx(
      crypto::digest_type::SHA256, bytes_view_to_string_view(hmac_sha256_key));
    ctx.update(bytes_view_to_string_view(hmac_sha256_msg));
    bytes sig(bytes::initialized_later{}, ctx.size());
    ASSERT_NO_THROW(std::move(ctx).final(bytes_span_to_char_span(sig)));
    EXPECT_EQ(sig, hmac_sha256_expected);
}

TEST(crypto_hmac, hmac_sha256_multipart_bad_len) {
    crypto::hmac_ctx ctx(crypto::digest_type::SHA256, hmac_sha256_key);
    ctx.update(hmac_sha256_msg);
    bytes sig(bytes::initialized_later{}, ctx.size() - 1);
    EXPECT_THROW(std::move(ctx).final(sig), crypto::exception);
}

TEST(crypto_hmac, hmac_sha256_reset) {
    crypto::hmac_ctx ctx(crypto::digest_type::SHA256, hmac_sha256_key);
    ctx.update(bytes_view_to_string_view(hmac_sha256_msg));
    bytes sig(bytes::initialized_later{}, ctx.size());
    ctx.reset(bytes_span_to_char_span(sig));
    EXPECT_EQ(sig, hmac_sha256_expected);
    ctx.update(hmac_sha256_msg);
    bytes sig2(bytes::initialized_later{}, ctx.size());
    ctx.reset(sig2);
    EXPECT_EQ(sig2, hmac_sha256_expected);
}
