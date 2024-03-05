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

#include <absl/container/flat_hash_map.h>
#include <gtest/gtest.h>

#include <iterator>
#include <type_traits>

static constexpr std::string_view md5_test_val
  = "The quick brown fox jumps over the lazy dog";

// NOLINTBEGIN
const auto md5_expected_val = convert_from_hex(
  "9e107d9d372bb6826bd81d3542a419d6");

const auto sha256_test_val = convert_from_hex(
  "09fc1accc230a205e4a208e64a8f204291f581a12756392da4b8c0cf5ef02b95");

const auto sha256_expected_val = convert_from_hex(
  "4f44c1c7fbebb6f9601829f3897bfd650c56fa07844be76489076356ac1886a4");

const auto sha512_test_val = convert_from_hex(
  "8ccb08d2a1a282aa8cc99902ecaf0f67a9f21cffe28005cb27fcf129e963f99d");

const auto sha512_expected_val = convert_from_hex(
  "4551def2f9127386eea8d4dae1ea8d8e49b2add0509f27ccbce7d9e950ac7db01d5bca579c27"
  "1b9f2d806730d88f58252fd0c2587851c3ac8a0e72b4e1dc0da6");

static const absl::flat_hash_map<crypto::digest_type, size_t> expected_sizes = {
  {crypto::digest_type::MD5, 16},
  {crypto::digest_type::SHA256, 32},
  {crypto::digest_type::SHA512, 64}};
// NOLINTEND

TEST(crypto_hashing, length) {
    for (auto&& [k, v] : expected_sizes) {
        ASSERT_EQ(crypto::digest_ctx::size(k), v)
          << "Mismatch on size for digest type " << k;
        crypto::digest_ctx ctx(k);
        ASSERT_EQ(ctx.size(), v)
          << "Mismatch on size for context using digest type " << k;
    }
}

TEST(crypto_hashing, md5_one_shot) {
    auto result = crypto::digest(crypto::digest_type::MD5, md5_test_val);
    ASSERT_EQ(result, md5_expected_val);
}

TEST(crypto_hashing, sha256_one_shot) {
    auto result = crypto::digest(crypto::digest_type::SHA256, sha256_test_val);
    ASSERT_EQ(result, sha256_expected_val);
}

TEST(crypto_hashing, sha512_one_shot) {
    auto result = crypto::digest(crypto::digest_type::SHA512, sha512_test_val);
    ASSERT_EQ(result, sha512_expected_val);
}

TEST(crypto_hashing, sha256_multipart_bytes) {
    crypto::digest_ctx ctx(crypto::digest_type::SHA256);
    ctx.update(sha256_test_val);
    bytes dgst(bytes::initialized_later{}, ctx.size());
    ASSERT_NO_THROW(std::move(ctx).final(dgst));
    ASSERT_EQ(dgst, sha256_expected_val);
}

TEST(crypto_hashing, sha256_multipart_string) {
    crypto::digest_ctx ctx(crypto::digest_type::SHA256);
    ctx.update(bytes_view_to_string_view(sha256_test_val));
    bytes dgst(bytes::initialized_later{}, ctx.size());
    ASSERT_NO_THROW(std::move(ctx).final(bytes_span_to_char_span(dgst)));
    ASSERT_EQ(dgst, sha256_expected_val);
}

TEST(crypto_hashing, sha256_bad_len) {
    crypto::digest_ctx ctx(crypto::digest_type::SHA256);
    ctx.update(sha256_test_val);
    bytes dgst(bytes::initialized_later{}, ctx.size() - 1);
    ASSERT_THROW(std::move(ctx).final(dgst), crypto::exception);
}

TEST(crypto_hashing, sha256_reset) {
    crypto::digest_ctx ctx(crypto::digest_type::SHA256);
    ctx.update(bytes_view_to_string_view(sha256_test_val));
    bytes dgst(bytes::initialized_later{}, ctx.size());
    ctx.reset(bytes_span_to_char_span(dgst));
    ASSERT_EQ(dgst, sha256_expected_val);
    ctx.update(sha256_test_val);
    bytes dgst2(bytes::initialized_later{}, ctx.size());
    ctx.reset(dgst2);
    ASSERT_EQ(dgst2, sha256_expected_val);
}
