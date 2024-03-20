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

#include "bytes/bytes.h"
#include "crypto/crypto.h"
#include "crypto/types.h"
#include "crypto_test_utils.h"
#include "test_utils/test.h"
#include "test_values.h"

#include <gtest/gtest.h>

using namespace test_values;

TEST(crypto_sig_validation, rsa_2k_sha256_good) {
    auto key = crypto::key::load_rsa_public_key(
      sig_test_rsa_pub_key_n, sig_test_rsa_pub_key_e);

    EXPECT_TRUE(crypto::verify_signature(
      crypto::digest_type::SHA256, key, sig_good_msg, sig_good_sig));
}

TEST(crypto_sig_validation, rsa_2k_sha256_bad) {
    auto key = crypto::key::load_rsa_public_key(
      sig_test_rsa_pub_key_n, sig_test_rsa_pub_key_e);

    EXPECT_FALSE(crypto::verify_signature(
      crypto::digest_type::SHA256,
      key,
      bytes_view_to_string_view(sig_bad_msg),
      bytes_view_to_string_view(sig_bad_sig)));
}

TEST(crypto_sig_validation, rsa_2k_sha256_good_multi_string) {
    auto key = crypto::key::load_rsa_public_key(
      sig_test_rsa_pub_key_n, sig_test_rsa_pub_key_e);

    crypto::verify_ctx ctx(crypto::digest_type::SHA256, key);
    ctx.update(bytes_view_to_string_view(sig_good_msg));
    EXPECT_TRUE(std::move(ctx).final(bytes_view_to_string_view(sig_good_sig)));
}

TEST(crypto_sig_validation, rsa_2k_sha256_reset) {
    auto key = crypto::key::load_rsa_public_key(
      sig_test_rsa_pub_key_n, sig_test_rsa_pub_key_e);

    crypto::verify_ctx ctx(crypto::digest_type::SHA256, key);
    ctx.update(sig_good_msg);
    EXPECT_TRUE(ctx.reset(sig_good_sig));
    ctx.update(bytes_view_to_string_view(sig_good_msg));
    EXPECT_TRUE(ctx.reset(bytes_view_to_string_view(sig_good_sig)));
}
