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

#include <gmock/gmock.h>
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
using namespace testing;

TEST(table_requests, serialize_commit_table_request) {
    commit_table_request req;
    req.identifier = iceberg::table_identifier{
      .ns = {"foo", "bar"},
      .table = "panda_table",
    };
    // add all possible updates and requirements
    schema schema{
      .schema_struct = std::get<struct_type>(test_nested_schema_type()),
      .schema_id = schema::id_t{0},
      .identifier_field_ids = {}};
    req.updates.push_back(table_update::add_schema{});
    req.updates.push_back(table_update::set_current_schema{});
    req.updates.push_back(table_update::add_spec{});
    req.updates.push_back(table_update::add_snapshot{});
    req.updates.push_back(table_update::remove_snapshots{});
    req.updates.push_back(table_update::set_snapshot_ref{});
    req.requirements.push_back(table_requirement::assert_create{});
    req.requirements.push_back(table_requirement::assert_current_schema_id{});
    req.requirements.push_back(table_requirement::assert_ref_snapshot_id{});
    req.requirements.push_back(table_requirement::assert_table_uuid{});
    req.requirements.push_back(table_requirement::last_assigned_field_match{});
    req.requirements.push_back(
      table_requirement::assert_last_assigned_partition_id{});

    auto json_str = to_json_str(req);

    json::Document d;
    d.Parse(json_str);

    ASSERT_TRUE(d["identifier"].HasMember("namespace"));
    ASSERT_TRUE(d["identifier"]["namespace"].IsArray());
    ASSERT_EQ(d["identifier"]["name"].GetString(), req.identifier.table);

    ASSERT_TRUE(d["updates"].IsArray());
    ASSERT_EQ(d["updates"].GetArray().Size(), 6);
    ASSERT_EQ(d["updates"].GetArray()[0]["action"], "add-schema");
    ASSERT_EQ(d["updates"].GetArray()[1]["action"], "set-current-schema");
    ASSERT_EQ(d["updates"].GetArray()[2]["action"], "add-spec");
    ASSERT_EQ(d["updates"].GetArray()[3]["action"], "add-snapshot");
    ASSERT_EQ(d["updates"].GetArray()[4]["action"], "remove-snapshots");
    ASSERT_EQ(d["updates"].GetArray()[5]["action"], "set-snapshot-ref");

    ASSERT_TRUE(d["requirements"].IsArray());
    ASSERT_EQ(d["requirements"].GetArray().Size(), 6);
    ASSERT_EQ(d["requirements"].GetArray()[0]["type"], "assert-create");
    ASSERT_EQ(
      d["requirements"].GetArray()[1]["type"], "assert-current-schema-id");
    ASSERT_EQ(
      d["requirements"].GetArray()[2]["type"], "assert-ref-snapshot-id");
    ASSERT_EQ(d["requirements"].GetArray()[3]["type"], "assert-table-uuid");
    ASSERT_EQ(
      d["requirements"].GetArray()[4]["type"], "assert-last-assigned-field-id");
    ASSERT_EQ(
      d["requirements"].GetArray()[5]["type"],
      "assert-last-assigned-partition-id");
}

TEST(table_requests, parsing_load_table_result) {
    static constexpr auto json = R"J(
  {
  "metadata-location": "string",
  "metadata": {
    "format-version": 2,
    "table-uuid": "56c3ec0c-3e3b-4369-bfe0-59e74e37f560",
    "location": "string",
    "last-updated-ms": 0,
    "properties": {
      "additionalProp1": "string",
      "additionalProp2": "string",
      "additionalProp3": "string"
    },
    "schemas": [
      {
        "type": "struct",
        "fields": [
          {
            "id": 0,
            "name": "string",
            "type": "string",
            "required": true,
            "doc": "string"
          }
        ],
        "schema-id": 0,
        "identifier-field-ids": [
          0
        ]
      }
    ],
    "current-schema-id": 0,
    "last-column-id": 0,
    "partition-specs": [
      {
        "spec-id": 0,
        "fields": [
          {
            "field-id": 0,
            "source-id": 0,
            "name": "string",
            "transform": "identity"
          }
        ]
      }
    ],
    "default-spec-id": 0,
    "last-partition-id": 0,
    "sort-orders": [
      {
        "order-id": 0,
        "fields": [
          {
            "source-ids": [0],
            "transform":"hour",
            "direction": "asc",
            "null-order": "nulls-first"
          }
        ]
      }
    ],
    "default-sort-order-id": 0,
    "snapshots": [
      {
        "snapshot-id": 0,
        "parent-snapshot-id": 0,
        "sequence-number": 0,
        "timestamp-ms": 0,
        "manifest-list": "string",
        "summary": {
          "operation": "append",
          "additionalProp1": "string",
          "additionalProp2": "string",
          "additionalProp3": "string"
        },
        "schema-id": 0
      }
    ],
    "refs": {
      "additionalProp1": {
        "type": "tag",
        "snapshot-id": 0,
        "max-ref-age-ms": 0,
        "max-snapshot-age-ms": 0,
        "min-snapshots-to-keep": 0
      },
      "additionalProp2": {
        "type": "tag",
        "snapshot-id": 0,
        "max-ref-age-ms": 0,
        "max-snapshot-age-ms": 0,
        "min-snapshots-to-keep": 0
      },
      "additionalProp3": {
        "type": "tag",
        "snapshot-id": 0,
        "max-ref-age-ms": 0,
        "max-snapshot-age-ms": 0,
        "min-snapshots-to-keep": 0
      }
    },
    "current-snapshot-id": 0,
    "last-sequence-number": 0,
    "snapshot-log": [
      {
        "snapshot-id": 0,
        "timestamp-ms": 0
      }
    ],
    "metadata-log": [
      {
        "metadata-file": "string",
        "timestamp-ms": 0
      }
    ],
    "statistics": [
      {
        "snapshot-id": 0,
        "statistics-path": "string",
        "file-size-in-bytes": 0,
        "file-footer-size-in-bytes": 0,
        "blob-metadata": [
          {
            "type": "string",
            "snapshot-id": 0,
            "sequence-number": 0,
            "fields": [
              0
            ],
            "properties": {
              "additionalProp1": "string",
              "additionalProp2": "string",
              "additionalProp3": "string"
            }
          }
        ]
      }
    ],
    "partition-statistics": [
      {
        "snapshot-id": 0,
        "statistics-path": "string",
        "file-size-in-bytes": 0
      }
    ]
  },
  "config": {
    "additionalProp1": "string",
    "additionalProp2": "string",
    "additionalProp3": "string"
  },
  "storage-credentials": [
    {
      "prefix": "string",
      "config": {
        "additionalProp1": "string",
        "additionalProp2": "string",
        "additionalProp3": "string"
      }
    }
  ]
}
)J";

    json::Document d;
    d.Parse(json);
    auto result = iceberg::parse_load_table_result(d);
    // validate that table metadata were decoded
    ASSERT_EQ(
      result.metadata.table_uuid,
      uuid_t::from_string("56c3ec0c-3e3b-4369-bfe0-59e74e37f560"));
    ASSERT_EQ(result.metadata_location, "string");
    ASSERT_TRUE(result.config.has_value());
    ASSERT_EQ(result.config->size(), 3);
    ASSERT_TRUE(result.storage_credentials.has_value());
    ASSERT_THAT(
      *result.storage_credentials,
      ElementsAre(AllOf(
        Field(&iceberg::storage_credentials::prefix, Eq("string")),
        Field(
          &iceberg::storage_credentials::config,
          UnorderedElementsAre(_, _, _)))));
}

TEST(table_requests, parsing_commit_table_response) {
    static constexpr auto json = R"J(
{
  "metadata-location": "string",
  "metadata": {
    "format-version": 2,
    "table-uuid": "56c3ec0c-3e3b-4369-bfe0-59e74e37f560",
    "location": "string",
    "last-updated-ms": 0,
    "properties": {
      "additionalProp1": "string",
      "additionalProp2": "string",
      "additionalProp3": "string"
    },
    "schemas": [
      {
        "type": "struct",
        "fields": [
          {
            "id": 0,
            "name": "string",
            "type": "long",
            "required": true,
            "doc": "string"
          }
        ],
        "schema-id": 0,
        "identifier-field-ids": [
          0
        ]
      }
    ],
    "current-schema-id": 0,
    "last-column-id": 0,
    "partition-specs": [
      {
        "spec-id": 0,
        "fields": [
          {
            "field-id": 0,
            "source-id": 0,
            "name": "string",
            "transform": "identity"
          }
        ]
      }
    ],
    "default-spec-id": 0,
    "last-partition-id": 0,
    "sort-orders": [
      {
        "order-id": 0,
        "fields": [
          {
            "source-ids": [0],
            "transform": "hour",
            "direction": "asc",
            "null-order": "nulls-first"
          }
        ]
      }
    ],
    "default-sort-order-id": 0,
    "snapshots": [
      {
        "snapshot-id": 0,
        "parent-snapshot-id": 0,
        "sequence-number": 0,
        "timestamp-ms": 0,
        "manifest-list": "string",
        "summary": {
          "operation": "append",
          "additionalProp1": "string",
          "additionalProp2": "string",
          "additionalProp3": "string"
        },
        "schema-id": 0
      }
    ],
    "refs": {
      "additionalProp1": {
        "type": "tag",
        "snapshot-id": 0,
        "max-ref-age-ms": 0,
        "max-snapshot-age-ms": 0,
        "min-snapshots-to-keep": 0
      },
      "additionalProp2": {
        "type": "tag",
        "snapshot-id": 0,
        "max-ref-age-ms": 0,
        "max-snapshot-age-ms": 0,
        "min-snapshots-to-keep": 0
      },
      "additionalProp3": {
        "type": "tag",
        "snapshot-id": 0,
        "max-ref-age-ms": 0,
        "max-snapshot-age-ms": 0,
        "min-snapshots-to-keep": 0
      }
    },
    "current-snapshot-id": 0,
    "last-sequence-number": 0,
    "snapshot-log": [
      {
        "snapshot-id": 0,
        "timestamp-ms": 0
      }
    ],
    "metadata-log": [
      {
        "metadata-file": "string",
        "timestamp-ms": 0
      }
    ],
    "statistics": [
      {
        "snapshot-id": 0,
        "statistics-path": "string",
        "file-size-in-bytes": 0,
        "file-footer-size-in-bytes": 0,
        "blob-metadata": [
          {
            "type": "string",
            "snapshot-id": 0,
            "sequence-number": 0,
            "fields": [
              "int"
            ],
            "properties": {
              "additionalProp1": "string",
              "additionalProp2": "string",
              "additionalProp3": "string"
            }
          }
        ]
      }
    ],
    "partition-statistics": [
      {
        "snapshot-id": 0,
        "statistics-path": "string",
        "file-size-in-bytes": 0
      }
    ]
  }
}
)J";

    json::Document d;
    d.Parse(json);
    auto result = iceberg::parse_commit_table_response(d);

    ASSERT_EQ(result.metadata_location, "string");
    ASSERT_EQ(
      result.table_metadata.table_uuid,
      uuid_t::from_string("56c3ec0c-3e3b-4369-bfe0-59e74e37f560"));
}
