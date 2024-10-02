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

#include "bytes/iobuf.h"
#include "schema/utils.h"

#include <gtest/gtest.h>

#include <cstdint>

TEST(SchemaUtils, SchemaIdForValidRecord) {
    iobuf buf;
    uint8_t magic = 0;
    uint32_t schema_id = 12;
    ss::sstring msg = "Hello world";
    buf.append(&magic, 1);
    buf.append(reinterpret_cast<uint8_t*>(&schema_id), sizeof(schema_id));
    buf.append(msg.data(), msg.size());

    auto res = schema::parse_schema_id(buf);

    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res.value()(), schema_id);
}

TEST(SchemaUtils, SchemaIdForShortRecord) {
    iobuf buf;

    uint8_t magic = 0;
    uint32_t schema_id = 12;
    buf.append(&magic, 1);
    // Only adding 3 bytes here instead of 4 to generate an invalid record.
    buf.append(reinterpret_cast<uint8_t*>(&schema_id), 3);

    auto res = schema::parse_schema_id(buf);
    ASSERT_FALSE(res.has_value());
    ASSERT_TRUE(res.has_error());

    EXPECT_EQ(res.error(), schema::schema_id_error::not_enough_bytes);
}

TEST(SchemaUtils, SchemaIdForBadMagic) {
    iobuf buf;
    uint8_t magic = 5; // Bad magic byte
    uint32_t schema_id = 12;
    ss::sstring msg = "Hello world";
    buf.append(&magic, 1);
    buf.append(reinterpret_cast<uint8_t*>(&schema_id), sizeof(schema_id));
    buf.append(msg.data(), msg.size());

    auto res = schema::parse_schema_id(buf);
    ASSERT_FALSE(res.has_value());
    ASSERT_TRUE(res.has_error());
    EXPECT_EQ(res.error(), schema::schema_id_error::invalid_magic);
}
