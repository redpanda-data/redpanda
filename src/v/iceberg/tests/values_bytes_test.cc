// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf.h"
#include "iceberg/values.h"
#include "iceberg/values_bytes.h"

#include <gtest/gtest.h>

using namespace iceberg;

TEST(ValueBytesTest, TestBool) {
    {
        boolean_value v{false};
        iobuf buf;
        buf.append("\0", 1);
        auto v_bytes = value_to_bytes(v);
        ASSERT_EQ(iobuf_to_bytes(buf), value_to_bytes(v));
        ASSERT_EQ(value_from_bytes(boolean_type{}, v_bytes), value{v});
    }
    {
        boolean_value v{true};
        iobuf buf;
        buf.append("\1", 1);
        auto v_bytes = value_to_bytes(v);
        ASSERT_EQ(iobuf_to_bytes(buf), v_bytes);
        ASSERT_EQ(value_from_bytes(boolean_type{}, v_bytes), value{v});
    }
}

TEST(ValueBytesTest, TestInt) {
    {
        int_value v{2};
        iobuf buf;
        auto bytes = std::vector<uint8_t>{0x02, 0x00, 0x00, 0x00};
        buf.append(bytes.data(), 4);
        auto v_bytes = value_to_bytes(v);
        ASSERT_EQ(iobuf_to_bytes(buf), value_to_bytes(v));
        ASSERT_EQ(value_from_bytes(int_type{}, v_bytes), value{v});
    }
    {
        int_value v{-2};
        iobuf buf;
        auto bytes = std::vector<uint8_t>{0xfe, 0xff, 0xff, 0xff};
        buf.append(bytes.data(), 4);
        auto v_bytes = value_to_bytes(v);
        ASSERT_EQ(iobuf_to_bytes(buf), v_bytes);
        ASSERT_EQ(value_from_bytes(int_type{}, v_bytes), value{v});
    }
}

TEST(ValueBytesTest, TestLong) {
    {
        long_value v{2};
        iobuf buf;
        auto bytes = std::vector<uint8_t>{
          0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
        buf.append(bytes.data(), 8);
        auto v_bytes = value_to_bytes(v);
        ASSERT_EQ(iobuf_to_bytes(buf), v_bytes);
        ASSERT_EQ(value_from_bytes(long_type{}, v_bytes), value{v});
    }
    {
        long_value v{-2};
        iobuf buf;
        auto bytes = std::vector<uint8_t>{
          0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};
        buf.append(bytes.data(), 8);
        auto v_bytes = value_to_bytes(v);
        ASSERT_EQ(iobuf_to_bytes(buf), v_bytes);
        ASSERT_EQ(value_from_bytes(long_type{}, v_bytes), value{v});
    }
}

TEST(ValueBytesTest, TestFloat) {
    {
        float_value v{2};
        iobuf buf;
        auto bytes = std::vector<uint8_t>{0x00, 0x00, 0x00, 0x40};
        buf.append(bytes.data(), 4);
        auto v_bytes = value_to_bytes(v);
        ASSERT_EQ(iobuf_to_bytes(buf), value_to_bytes(v));
        ASSERT_EQ(value_from_bytes(float_type{}, v_bytes), value{v});
    }
    {
        float_value v{-2};
        iobuf buf;
        auto bytes = std::vector<uint8_t>{0x00, 0x00, 0x00, 0xc0};
        buf.append(bytes.data(), 4);
        auto v_bytes = value_to_bytes(v);
        ASSERT_EQ(iobuf_to_bytes(buf), v_bytes);
        ASSERT_EQ(value_from_bytes(float_type{}, v_bytes), value{v});
    }
}

TEST(ValueBytesTest, TestDouble) {
    {
        double_value v{2};
        iobuf buf;
        auto bytes = std::vector<uint8_t>{
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40};
        buf.append(bytes.data(), 8);
        auto v_bytes = value_to_bytes(v);
        ASSERT_EQ(iobuf_to_bytes(buf), v_bytes);
        ASSERT_EQ(value_from_bytes(double_type{}, v_bytes), value{v});
    }
    {
        double_value v{-2};
        iobuf buf;
        auto bytes = std::vector<uint8_t>{
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xc0};
        buf.append(bytes.data(), 8);
        auto v_bytes = value_to_bytes(v);
        ASSERT_EQ(iobuf_to_bytes(buf), v_bytes);
        ASSERT_EQ(value_from_bytes(double_type{}, v_bytes), value{v});
    }
}

TEST(ValueBytesTest, TestUuid) {
    uuid_value v{uuid_t::from_string("deadbeef-0000-0000-0000-000000000000")};
    iobuf buf;
    // clang-format off
    auto bytes = std::vector<uint8_t>{
      0xde, 0xad, 0xbe, 0xef, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    // clang-format on
    buf.append(bytes.data(), 16);
    auto v_bytes = value_to_bytes(v);
    ASSERT_EQ(iobuf_to_bytes(buf), v_bytes);
    ASSERT_EQ(value_from_bytes(uuid_type{}, v_bytes), value{v});
}

TEST(ValueBytesTest, TestString) {
    string_value v{iobuf::from("abcd")};
    iobuf buf;
    auto bytes = std::vector<uint8_t>{0x61, 0x62, 0x63, 0x64};
    buf.append(bytes.data(), 4);
    auto v_bytes = value_to_bytes(std::move(v));
    ASSERT_EQ(iobuf_to_bytes(buf), v_bytes);
    ASSERT_EQ(
      value_from_bytes(string_type{}, v_bytes),
      value{string_value{iobuf::from("abcd")}});
}
