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
#include "crypto/crypto.h"

#include <gtest/gtest.h>

#include <array>

TEST(crypto_secure_erase, zeroes_buffer) {
    constexpr size_t buf_size = 64;
    constexpr uint8_t fill_byte = 0xab;
    std::array<uint8_t, buf_size> buf{};
    buf.fill(fill_byte);
    crypto::secure_erase(buf);
    for (auto b : buf) {
        EXPECT_EQ(b, 0);
    }
}

TEST(crypto_secure_erase, empty_buffer_is_noop) {
    crypto::secure_erase(bytes_span<>{});
}
