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

#include "encryption/mock_kms_provider.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

namespace {

// 32-byte DEK for testing (AES key wrap requires >= 16 bytes, multiple of 8)
bytes make_test_dek() {
    bytes dek(bytes::initialized_later(), 32);
    for (size_t i = 0; i < dek.size(); ++i) {
        dek[i] = static_cast<uint8_t>(i);
    }
    return dek;
}

bytes make_alt_dek() {
    bytes dek(bytes::initialized_later(), 32);
    for (size_t i = 0; i < dek.size(); ++i) {
        dek[i] = static_cast<uint8_t>(0xff - i);
    }
    return dek;
}

bytes make_garbage(size_t len) {
    bytes garbage(bytes::initialized_later(), len);
    for (size_t i = 0; i < len; ++i) {
        garbage[i] = static_cast<uint8_t>(0xde);
    }
    return garbage;
}

} // namespace

TEST(mock_kms, wrap_then_unwrap_roundtrip) {
    encryption::mock_kms_provider kms;
    auto dek = make_test_dek();

    auto wrapped = kms.wrap_dek("test-key", dek).get();
    // Wrapped output is 8 bytes longer than input
    EXPECT_EQ(wrapped.size(), dek.size() + 8);
    // Wrapped bytes differ from original
    EXPECT_NE(
      bytes_view(wrapped.data(), dek.size()),
      bytes_view(dek.data(), dek.size()));

    auto unwrapped = kms.unwrap_dek("test-key", wrapped).get();
    EXPECT_EQ(unwrapped, dek);
}

TEST(mock_kms, deterministic_output) {
    encryption::mock_kms_provider kms;
    auto dek = make_test_dek();

    auto wrapped1 = kms.wrap_dek("key-1", dek).get();
    auto wrapped2 = kms.wrap_dek("key-2", dek).get();

    EXPECT_EQ(wrapped1, wrapped2);
}

TEST(mock_kms, different_deks_different_output) {
    encryption::mock_kms_provider kms;
    auto dek1 = make_test_dek();
    auto dek2 = make_alt_dek();

    auto wrapped1 = kms.wrap_dek("key", dek1).get();
    auto wrapped2 = kms.wrap_dek("key", dek2).get();

    EXPECT_NE(wrapped1, wrapped2);
}

TEST(mock_kms, unwrap_garbage_fails) {
    encryption::mock_kms_provider kms;
    // 40 bytes: valid size (multiple of 8, >= 24) but invalid content
    auto garbage = make_garbage(40);
    EXPECT_THROW(kms.unwrap_dek("key", garbage).get(), std::exception);
}
