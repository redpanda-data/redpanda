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
#include "test_utils/test.h"

#include <gtest/gtest.h>

TEST(crypto_rand, generate_rand) {
    const size_t len = 32;
    auto ret = crypto::generate_random(len, crypto::use_private_rng::no);
    EXPECT_EQ(ret.size(), len);
}

TEST(crypto_rand, generate_rand_priv) {
    const size_t len = 32;
    auto ret = crypto::generate_random(len, crypto::use_private_rng::yes);
    EXPECT_EQ(ret.size(), len);
}

TEST(crypto_rand, generate_rand_span) {
    std::vector<uint8_t> rand_data(32);
    auto copy_of_orig = rand_data;
    crypto::generate_random(rand_data, crypto::use_private_rng::no);
    EXPECT_NE(copy_of_orig, rand_data);
}

TEST(crypto_rand, generate_rand_span_private) {
    std::vector<uint8_t> rand_data(32);
    auto copy_of_orig = rand_data;
    crypto::generate_random(rand_data, crypto::use_private_rng::yes);
    EXPECT_NE(copy_of_orig, rand_data);
}
