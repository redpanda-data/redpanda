/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "gtest/gtest.h"
#include "serde/protobuf/wire_format.h"

#include <type_traits>

namespace serde::pb {

template<typename T>
class ProtobufVarintSupport : public testing::Test {
protected:
    std::vector<T> values() {
        if constexpr (std::is_same_v<T, int32_t>) {
            return {
              -1,
              0,
              1,
              -2,
              2,
              std::numeric_limits<int16_t>::max(),
              -std::numeric_limits<int16_t>::max(),
              std::numeric_limits<int16_t>::min(),
              -std::numeric_limits<int16_t>::min(),
              std::numeric_limits<int16_t>::max() + 1,
              std::numeric_limits<int16_t>::min() - 1,
              std::numeric_limits<int32_t>::max(),
              -std::numeric_limits<int32_t>::max(),
              std::numeric_limits<int32_t>::min(),
            };
        } else if constexpr (std::is_same_v<T, uint32_t>) {
            return {
              0,
              1,
              2,
              3,
              std::numeric_limits<int8_t>::max(),
              std::numeric_limits<uint8_t>::max(),
              std::numeric_limits<uint16_t>::max(),
              std::numeric_limits<int32_t>::max(),
              std::numeric_limits<uint32_t>::max(),
            };
        } else if constexpr (std::is_same_v<T, int64_t>) {
            return {
              -1,
              0,
              1,
              -2,
              2,
              std::numeric_limits<int16_t>::max(),
              -std::numeric_limits<int16_t>::max(),
              std::numeric_limits<int16_t>::min(),
              -std::numeric_limits<int16_t>::min(),
              std::numeric_limits<int16_t>::max() + 1,
              std::numeric_limits<int16_t>::min() - 1,
              std::numeric_limits<int32_t>::max(),
              -std::numeric_limits<int32_t>::max(),
              std::numeric_limits<int32_t>::min(),
              static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1,
              static_cast<int64_t>(std::numeric_limits<int32_t>::min()) - 1,
              std::numeric_limits<uint32_t>::max(),
              std::numeric_limits<int64_t>::max(),
              -std::numeric_limits<int64_t>::max(),
              std::numeric_limits<int64_t>::min(),
            };
        } else if constexpr (std::is_same_v<T, uint64_t>) {
            return {
              0,
              1,
              2,
              3,
              std::numeric_limits<int8_t>::max(),
              std::numeric_limits<uint8_t>::max(),
              std::numeric_limits<uint16_t>::max(),
              std::numeric_limits<int32_t>::max(),
              std::numeric_limits<uint32_t>::max(),
              std::numeric_limits<int64_t>::max(),
              std::numeric_limits<uint64_t>::max(),
            };
        } else {
            static_assert(false, "Unsupported type");
        }
    }
};

using VarintTypes = testing::Types<int32_t, uint32_t, int64_t, uint64_t>;
TYPED_TEST_SUITE(ProtobufVarintSupport, VarintTypes);

TYPED_TEST(ProtobufVarintSupport, Roundtrip) {
    std::vector<TypeParam> nums = this->values();
    for (TypeParam n : nums) {
        iobuf out;
        write_varint<TypeParam, zigzag::no>(n, &out);
        iobuf_parser parser(std::move(out));
        TypeParam v = read_varint<TypeParam, zigzag::no>(&parser);
        EXPECT_EQ(parser.bytes_left(), 0);
        EXPECT_EQ(v, n);
    }
    if constexpr (std::is_signed_v<TypeParam>) {
        for (TypeParam n : nums) {
            iobuf out;
            write_varint<TypeParam, zigzag::yes>(n, &out);
            iobuf_parser parser(std::move(out));
            TypeParam v = read_varint<TypeParam, zigzag::yes>(&parser);
            EXPECT_EQ(parser.bytes_left(), 0);
            EXPECT_EQ(v, n);
        }
    }
}

} // namespace serde::pb
