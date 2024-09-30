/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "datalake/schema_registry.h"
#include "gmock/gmock.h"
#include "model/record.h"
#include "utils/vint.h"

#include <gtest/gtest.h>

#include <variant>

namespace {
void buf_append(iobuf& b, uint8_t byte) { b.append(&byte, 1); }
void buf_append(iobuf& b, int32_t val) {
    b.append(reinterpret_cast<uint8_t*>(&val), 4);
}
void buf_append(iobuf& b, const bytes& byte) {
    b.append(byte.data(), byte.size());
}
void buf_append(iobuf& b, const std::string& str) {
    b.append(str.data(), str.size());
}

template<typename... Args>
iobuf buf_from(const Args&... args) {
    iobuf b;
    (buf_append(b, args), ...);
    return b;
}
} // namespace

TEST(DatalakeSchemaRegistry, SchemaIdForValidRecord) {
    uint8_t magic = 0;
    int32_t schema_id = 12;
    int32_t schema_id_encoded = ss::cpu_to_be(schema_id);
    std::string payload = "Hello world";

    iobuf value = buf_from(magic, schema_id_encoded, payload);

    auto res = datalake::get_value_schema_id(value);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res.value().schema_id(), schema_id);
    EXPECT_EQ(res.value().shared_message_data.size_bytes(), payload.size());
}

TEST(DatalakeSchemaRegistry, SchemaIdForShortRecord) {
    iobuf value;

    uint8_t magic = 0;
    int32_t schema_id = 12;
    int32_t schema_id_encoded = ss::cpu_to_be(schema_id);
    value.append(&magic, 1);
    // Only adding 3 bytes here instead of 4 to generate an invalid record.
    value.append(reinterpret_cast<uint8_t*>(&schema_id_encoded), 3);

    auto res = datalake::get_value_schema_id(value);
    ASSERT_TRUE(res.has_error());
    EXPECT_EQ(res.error(), datalake::get_schema_error::not_enough_bytes);
}

TEST(DatalakeSchemaRegistry, SchemaIdForBadMagic) {
    uint8_t magic = 5; // Invalid magic
    int32_t schema_id = 12;
    int32_t schema_id_encoded = ss::cpu_to_be(schema_id);

    iobuf value = buf_from(magic, schema_id_encoded);

    auto res = datalake::get_value_schema_id(value);
    ASSERT_TRUE(res.has_error());
    EXPECT_EQ(res.error(), datalake::get_schema_error::no_schema_id);
}

TEST(DatalakeSchemaRegistry, GetProtoOffsetsOk) {
    uint8_t magic = 0;
    uint32_t schema_id = 12;
    int32_t schema_id_encoded = ss::cpu_to_be(schema_id);
    std::string payload = "Hello world";

    uint8_t proto_msg_count = 5;
    auto encoded = vint::to_bytes(proto_msg_count);
    iobuf value = buf_from(magic, schema_id_encoded, encoded);

    for (uint8_t i = 0; i < proto_msg_count; i++) {
        encoded = vint::to_bytes(i);
        value.append(encoded.data(), encoded.size());
    }
    value.append(payload.data(), payload.size());

    auto res = datalake::get_proto_offsets(value);
    ASSERT_TRUE(res.has_value());
    const auto& offsets = res.value().protobuf_offsets;
    EXPECT_THAT(offsets, testing::ElementsAre(0, 1, 2, 3, 4));
    EXPECT_EQ(res.value().schema_id(), schema_id);
    EXPECT_EQ(res.value().shared_message_data.size_bytes(), payload.size());
}

TEST(DatalakeSchemaRegistry, GetProtoOffsetsDefaultZero) {
    // This tests a special case where the offset count is 0, we should assume
    // that the message is the first one defined in the schema and return {0}.

    uint8_t magic = 0;
    uint32_t schema_id = 12;
    int32_t schema_id_encoded = ss::cpu_to_be(schema_id);
    std::string payload = "Hello world";

    uint8_t proto_msg_count = 0;
    auto encoded = vint::to_bytes(proto_msg_count);

    iobuf value = buf_from(magic, schema_id_encoded, encoded, payload);

    auto res = datalake::get_proto_offsets(value);
    ASSERT_TRUE(res.has_value());
    const auto& offsets = res.value().protobuf_offsets;
    EXPECT_EQ(offsets.size(), 1);
    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(res.value().schema_id(), schema_id);
    EXPECT_EQ(res.value().shared_message_data.size_bytes(), payload.size());
}

TEST(DatalakeSchemaRegistry, GetProtoOffsetsNotEnoughData) {
    uint8_t magic = 0;
    uint32_t schema_id = 12;
    int32_t schema_id_encoded = ss::cpu_to_be(schema_id);

    uint8_t proto_msg_count = 9;
    auto encoded = vint::to_bytes(proto_msg_count);

    iobuf value = buf_from(magic, schema_id_encoded, encoded);

    for (uint8_t i = 0; i < proto_msg_count - 1; i++) {
        encoded = vint::to_bytes(i);
        value.append(encoded.data(), encoded.size());
    }

    auto res = datalake::get_proto_offsets(value);
    ASSERT_TRUE(res.has_error());
    EXPECT_EQ(res.error(), datalake::get_schema_error::not_enough_bytes);
}
