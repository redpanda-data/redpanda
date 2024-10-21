/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/json_writer.h"
#include "iceberg/partition_json.h"
#include "iceberg/schema_json.h"
#include "iceberg/table_requests.h"
#include "iceberg/table_requests_json.h"
#include "iceberg/tests/test_schemas.h"
#include "json/document.h"

#include <gtest/gtest.h>

namespace {
template<typename T>
void assert_type_and_value(
  const json::Document& d, const T& expected, const std::string& name) {
    ASSERT_TRUE(d.HasMember(name));
    ASSERT_TRUE(d[name].Is<T>());
    ASSERT_EQ(d[name].Get<T>(), expected);
}
} // namespace

using namespace iceberg;

TEST(table_requests, serialize_create_table) {
    schema schema{
      .schema_struct = std::get<struct_type>(test_nested_schema_type()),
      .schema_id = schema::id_t{0},
      .identifier_field_ids = {}};
    partition_spec spec{
      .spec_id = partition_spec::id_t{0},
      .fields = chunked_vector<partition_field>{
        partition_field{.field_id = partition_field::id_t{0}, .name = "foo"}}};
    chunked_hash_map<ss::sstring, ss::sstring> properties{{"a", "b"}};
    const create_table_request r{
      .name = "table",
      .schema = schema.copy(),
      .location = "loc",
      .partition_spec = spec.copy(),
      .stage_create = true,
      .properties = std::move(properties)};
    auto serialized = to_json_str(r);

    json::Document d;
    d.Parse(serialized);

    ASSERT_FALSE(d.HasParseError());

    assert_type_and_value(d, std::string{r.name}, "name");

    ASSERT_TRUE(d.HasMember("schema"));
    ASSERT_TRUE(d["schema"].IsObject());
    ASSERT_EQ(parse_schema(d["schema"].GetObject()), schema);

    assert_type_and_value(d, std::string{r.location.value()}, "location");

    ASSERT_TRUE(d.HasMember("partition-spec"));
    ASSERT_TRUE(d["partition-spec"].IsObject());
    ASSERT_EQ(
      parse_partition_spec(d["partition-spec"].GetObject()), r.partition_spec);

    assert_type_and_value(d, r.stage_create.value(), "stage-create");

    ASSERT_TRUE(d.HasMember("properties"));
    ASSERT_TRUE(d["properties"].IsObject());
    ASSERT_STREQ(d["properties"]["a"].GetString(), "b");
}
