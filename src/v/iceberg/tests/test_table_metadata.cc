// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

namespace iceberg {

// This metadata was mostly taken from the Iceberg Rust project.
// https://github.com/apache/iceberg-rust/blob/7aa8bddeb09420b2f81a50112603de28aeaf3be7/crates/iceberg/testdata/example_table_metadata_v2.json
const char* test_table_meta_json = R"({
  "format-version": 2,
  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "location": "s3://foo/bar/baz",
  "last-sequence-number": 34,
  "last-updated-ms": 1602638573590,
  "last-column-id": 3,
  "current-schema-id": 1,
  "schemas": [
    {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]},
    {
      "type": "struct",
      "schema-id": 1,
      "identifier-field-ids": [1, 2],
      "fields": [
        {"id": 1, "name": "x", "required": true, "type": "long"},
        {"id": 2, "name": "y", "required": true, "type": "long", "doc": "comment"},
        {"id": 3, "name": "z", "required": true, "type": "long"},
        {"id": 4, "name": "a", "required": true, "type": "string"}
      ]
    }
  ],
  "default-spec-id": 0,
  "partition-specs": [{"spec-id": 0, "fields": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]}],
  "last-partition-id": 1000,
  "default-sort-order-id": 3,
  "sort-orders": [
    {
      "order-id": 3,
      "fields": [
        {"transform": "identity", "source-ids": [2], "direction": "asc", "null-order": "nulls-first"},
        {"transform": "bucket[4]", "source-ids": [3], "direction": "desc", "null-order": "nulls-last"}
      ]
    }
  ],
  "properties": {"read.split.target.size": "134217728"},
  "current-snapshot-id": 3055729675574597004,
  "snapshots": [
    {
      "snapshot-id": 3051729675574597004,
      "timestamp-ms": 1515100955770,
      "sequence-number": 0,
      "summary": {"operation": "append"},
      "manifest-list": "s3://foo/bar/baz/1"
    },
    {
      "snapshot-id": 3055729675574597004,
      "parent-snapshot-id": 3051729675574597004,
      "timestamp-ms": 1555100955770,
      "sequence-number": 1,
      "summary": {"operation": "append"},
      "manifest-list": "s3://foo/bar/baz/2",
      "schema-id": 1
    }
  ],
  "refs": {
    "main": {
      "snapshot-id": 5937117119577207000,
      "type": "branch",
      "max-ref-age-ms": 255486129308,
      "max-snapshot-age-ms": 255486129307,
      "min-snapshots-to-keep": 12345
    },
    "foo": {
      "snapshot-id": 5937117119577207000,
      "type": "tag"
    }
  }
})";

// Same schema as above but with optional fields removed.
const char* test_table_meta_no_optionals_json = R"({
  "format-version": 2,
  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "location": "s3://foo/bar/baz",
  "last-sequence-number": 34,
  "last-updated-ms": 1602638573590,
  "last-column-id": 3,
  "current-schema-id": 1,
  "schemas": [
    {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]},
    {
      "type": "struct",
      "schema-id": 1,
      "identifier-field-ids": [1, 2],
      "fields": [
        {"id": 1, "name": "x", "required": true, "type": "long"},
        {"id": 2, "name": "y", "required": true, "type": "long", "doc": "comment"},
        {"id": 3, "name": "z", "required": true, "type": "long"},
        {"id": 4, "name": "a", "required": true, "type": "string"}
      ]
    }
  ],
  "default-spec-id": 0,
  "partition-specs": [{"spec-id": 0, "fields": [{"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}]}],
  "last-partition-id": 1000,
  "default-sort-order-id": 3,
  "sort-orders": [
    {
      "order-id": 3,
      "fields": [
        {"transform": "identity", "source-ids": [2], "direction": "asc", "null-order": "nulls-first"},
        {"transform": "bucket[4]", "source-ids": [3], "direction": "desc", "null-order": "nulls-last"}
      ]
    }
  ]
})";

} // namespace iceberg
