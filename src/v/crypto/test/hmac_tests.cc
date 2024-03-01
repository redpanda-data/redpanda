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

#include <absl/container/flat_hash_map.h>
#include <gtest/gtest.h>

// NOLINTBEGIN
const auto hmac_sha256_key = convert_from_hex(
  "9779d9120642797f1747025d5b22b7ac607cab08e1758f2f3a46c8be1e25c53b8c6a8f58ffef"
  "a176");

const auto hmac_sha256_msg = convert_from_hex(
  "b1689c2591eaf3c9e66070f8a77954ffb81749f1b00346f9dfe0b2ee905dcc288baf4a92de3f"
  "4001dd9f44c468c3d07d6c6ee82faceafc97c2fc0fc0601719d2dcd0aa2aec92d1b0ae933c65"
  "eb06a03c9c935c2bad0459810241347ab87e9f11adb30415424c6c7f5f22a003b8ab8de54f6d"
  "ed0e3ab9245fa79568451dfa258e");

const auto hmac_sha256_expected = convert_from_hex(
  "769f00d3e6a6cc1fb426a14a4f76c6462e6149726e0dee0ec0cf97a16605ac8b");

const auto hmac_sha512_key = convert_from_hex(
  "57c2eb677b5093b9e829ea4babb50bde55d0ad59fec34a618973802b2ad9b78e26b2045dda78"
  "4df3ff90ae0f2cc51ce39cf54867320ac6f3ba2c6f0d72360480c96614ae66581f266c35fb79"
  "fd28774afd113fa5187eff9206d7cbe90dd8bf67c844e202");

const auto hmac_sha512_msg = convert_from_hex(
  "2423dff48b312be864cb3490641f793d2b9fb68a7763b8e298c86f42245e4540eb01ae4d2d45"
  "00370b1886f23ca2cf9701704cad5bd21ba87b811daf7a854ea24a56565ced425b35e40e1acb"
  "ebe03603e35dcf4a100e57218408a1d8dbcc3b99296cfea931efe3ebd8f719a6d9a15487b9ad"
  "67eafedf15559ca42445b0f9b42e");

const auto hmac_sha512_expected = convert_from_hex(
  "33c511e9bc2307c62758df61125a980ee64cefebd90931cb91c13742d4714c06de4003faf3c4"
  "1c06aefc638ad47b21906e6b104816b72de6269e045a1f4429d4");

static const absl::flat_hash_map<crypto::digest_type, size_t> expected_sizes = {
  {crypto::digest_type::SHA256, 32}, {crypto::digest_type::SHA512, 64}};
// NOLINTEND

TEST(crypto_hmac, length) {
    // NOLINTNEXTLINE
    bytes fake_key('k', 16);
    for (auto&& [k, v] : expected_sizes) {
        ASSERT_EQ(crypto::hmac_ctx::len(k), v)
          << "Mismatch on HMAC size for digest type " << k;
        crypto::hmac_ctx ctx(k, fake_key);
        ASSERT_EQ(ctx.len(), v)
          << "Mismatch on HMAC size for context using digest type " << k;
    }
}

TEST(crypto_hmac, hmac_sha256_one_shot) {
    ASSERT_EQ(
      crypto::hmac(
        crypto::digest_type::SHA256, hmac_sha256_key, hmac_sha256_msg),
      hmac_sha256_expected);
}

TEST(crypto_hmac, hmac_sha512_one_shot) {
    ASSERT_EQ(
      crypto::hmac(
        crypto::digest_type::SHA512,
        bytes_view_to_string_view(hmac_sha512_key),
        bytes_view_to_string_view(hmac_sha512_msg)),
      hmac_sha512_expected);
}

TEST(crypto_hmac, hmac_sha256_multipart_bytes) {
    crypto::hmac_ctx ctx(crypto::digest_type::SHA256, hmac_sha256_key);
    ctx.update(hmac_sha256_msg);
    bytes sig(bytes::initialized_later{}, ctx.len());
    ASSERT_NO_THROW(std::move(ctx).final(sig));
    ASSERT_EQ(sig, hmac_sha256_expected);
}

TEST(crypto_hmac, hmac_sha256_multipart_string) {
    crypto::hmac_ctx ctx(
      crypto::digest_type::SHA256, bytes_view_to_string_view(hmac_sha256_key));
    ctx.update(bytes_view_to_string_view(hmac_sha256_msg));
    bytes sig(bytes::initialized_later{}, ctx.len());
    ASSERT_NO_THROW(std::move(ctx).final(bytes_span_to_char_span(sig)));
    ASSERT_EQ(sig, hmac_sha256_expected);
}

TEST(crypto_hmac, hmac_sha256_multipart_bad_len) {
    crypto::hmac_ctx ctx(crypto::digest_type::SHA256, hmac_sha256_key);
    ctx.update(hmac_sha256_msg);
    bytes sig(bytes::initialized_later{}, ctx.len() - 1);
    ASSERT_THROW(std::move(ctx).final(sig), crypto::exception);
}
