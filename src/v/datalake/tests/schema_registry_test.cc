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
#include "model/record.h"

#include <gtest/gtest.h>

#include <variant>

TEST(DatalakeSchemaRegistry, SchemaIdForValidRecord) {
    iobuf value;

    uint8_t magic = 0;
    int32_t schema_id = 12;
    int32_t schema_id_encoded = htobe32(schema_id);
    value.append(&magic, 1);
    value.append(reinterpret_cast<uint8_t*>(&schema_id_encoded), 4);

    auto res = datalake::get_value_schema_id(value);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res.value()(), schema_id);
}

TEST(DatalakeSchemaRegistry, SchemaIdForShortRecord) {
    iobuf value;

    uint8_t magic = 0;
    int32_t schema_id = 12;
    int32_t schema_id_encoded = htobe32(schema_id);
    value.append(&magic, 1);
    // Only adding 3 bytes here instead of 4 to generate an invalid record.
    value.append(reinterpret_cast<uint8_t*>(&schema_id_encoded), 3);

    auto res = datalake::get_value_schema_id(value);
    ASSERT_TRUE(res.has_error());
    EXPECT_EQ(res.error(), datalake::get_schema_error::not_enough_bytes);
}

TEST(DatalakeSchemaRegistry, SchemaIdForBadMagic) {
    iobuf value;

    uint8_t magic = 5; // Invalid magic
    int32_t schema_id = 12;
    int32_t schema_id_encoded = htobe32(schema_id);
    value.append(&magic, 1);
    value.append(reinterpret_cast<uint8_t*>(&schema_id_encoded), 4);
    auto res = datalake::get_value_schema_id(value);
    ASSERT_TRUE(res.has_error());
    EXPECT_EQ(res.error(), datalake::get_schema_error::no_schema_id);
}
