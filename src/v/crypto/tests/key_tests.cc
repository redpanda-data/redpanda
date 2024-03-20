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
#include "crypto/exceptions.h"
#include "crypto/types.h"
#include "crypto_test_utils.h"
#include "gtest/gtest.h"
#include "test_utils/test.h"
#include "test_values.h"

#include <gtest/gtest.h>

#include <stdexcept>

using namespace test_values;

TEST(crypto_key, load_pem_private_key) {
    auto key = crypto::key::load_key(
      example_pem_rsa_private_key,
      crypto::format_type::PEM,
      crypto::is_private_key_t::yes);

    EXPECT_EQ(key.get_type(), crypto::key_type::RSA);
    EXPECT_TRUE(key.is_private_key());
}

TEST(crypto_key, load_pem_public_key) {
    auto key = crypto::key::load_key(
      example_pem_rsa_public_key,
      crypto::format_type::PEM,
      crypto::is_private_key_t::no);

    EXPECT_EQ(key.get_type(), crypto::key_type::RSA);
    EXPECT_FALSE(key.is_private_key());
}

TEST(crypto_key, load_ec_key) {
    EXPECT_THROW(
      crypto::key::load_key(
        example_pem_ec_public_key,
        crypto::format_type::PEM,
        crypto::is_private_key_t::no),
      crypto::exception);
}

TEST(crypto_key, load_rsa_pub_key_components) {
    EXPECT_NO_THROW(
      crypto::key::load_rsa_public_key(rsa_pub_key_n, rsa_pub_key_e));
}
