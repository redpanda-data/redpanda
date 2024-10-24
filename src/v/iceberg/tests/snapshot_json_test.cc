// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/json_writer.h"
#include "iceberg/snapshot.h"
#include "iceberg/snapshot_json.h"
#include "json/document.h"

#include <gtest/gtest.h>

using namespace iceberg;

TEST(SnapshotJsonSerde, TestSnapshot) {
    const auto test_snapshot_str = R"(
        {
            "snapshot-id": 3051729675574597004,
            "parent-snapshot-id": 3051729675574597004,
            "sequence-number": 10,
            "timestamp-ms": 1515100955770,
            "summary": {
                "operation": "append",
                "foo": "bar"
            },
            "manifest-list": "s3://b/wh/.../s1.avro",
            "schema-id": 0
        }
)";
    json::Document parsed_orig_json;
    parsed_orig_json.Parse(test_snapshot_str);
    const auto snap = parse_snapshot(parsed_orig_json);
    ASSERT_EQ(3051729675574597004, snap.id());
    ASSERT_TRUE(snap.parent_snapshot_id.has_value());
    ASSERT_EQ(3051729675574597004, snap.parent_snapshot_id.value());
    ASSERT_EQ(10, snap.sequence_number());
    ASSERT_EQ(1515100955770, snap.timestamp_ms.value());
    ASSERT_EQ(snapshot_operation::append, snap.summary.operation);
    ASSERT_EQ(1, snap.summary.other.size());
    ASSERT_EQ("bar", snap.summary.other.at("foo"));
    ASSERT_STREQ("s3://b/wh/.../s1.avro", snap.manifest_list_path.c_str());
    ASSERT_TRUE(snap.schema_id.has_value());
    ASSERT_EQ(0, snap.schema_id.value());

    const auto parsed_orig_as_str = iceberg::to_json_str(snap);
    json::Document parsed_roundtrip_json;
    parsed_roundtrip_json.Parse(parsed_orig_as_str);
    const auto roundtrip_snap = parse_snapshot(parsed_roundtrip_json);

    ASSERT_EQ(roundtrip_snap, snap);
}

TEST(SnapshotJsonSerde, TestSnapshotMissingOptionals) {
    const auto test_snapshot_str = R"(
        {
            "snapshot-id": 3051729675574597004,
            "timestamp-ms": 1515100955770,
            "sequence-number": 10,
            "summary": {
                "operation": "append",
                "foo": "bar"
            },
            "manifest-list": "s3://b/wh/.../s1.avro"
        }
)";
    json::Document parsed_orig_json;
    parsed_orig_json.Parse(test_snapshot_str);
    const auto snap = parse_snapshot(parsed_orig_json);
    ASSERT_EQ(3051729675574597004, snap.id());
    ASSERT_FALSE(snap.parent_snapshot_id.has_value());
    ASSERT_EQ(10, snap.sequence_number());
    ASSERT_EQ(1515100955770, snap.timestamp_ms.value());
    ASSERT_EQ(snapshot_operation::append, snap.summary.operation);
    ASSERT_EQ(1, snap.summary.other.size());
    ASSERT_EQ("bar", snap.summary.other.at("foo"));
    ASSERT_STREQ("s3://b/wh/.../s1.avro", snap.manifest_list_path.c_str());
    ASSERT_FALSE(snap.schema_id.has_value());

    const auto parsed_orig_as_str = iceberg::to_json_str(snap);
    json::Document parsed_roundtrip_json;
    parsed_roundtrip_json.Parse(parsed_orig_as_str);
    const auto roundtrip_snap = parse_snapshot(parsed_roundtrip_json);

    ASSERT_EQ(roundtrip_snap, snap);
}

TEST(SnapshotJsonSerde, TestSnapshotReferences) {
    const auto test_ref_str = R"({
        "snapshot-id": 5937117119577207000,
        "type": "branch",
        "max-ref-age-ms": 255486129308,
        "max-snapshot-age-ms": 255486129307,
        "min-snapshots-to-keep": 12345
      })";
    json::Document parsed_orig_json;
    parsed_orig_json.Parse(test_ref_str);
    const auto ref = parse_snapshot_ref(parsed_orig_json);
    ASSERT_EQ(ref.snapshot_id(), 5937117119577207000);
    ASSERT_EQ(ref.type, snapshot_ref_type::branch);
    ASSERT_TRUE(ref.max_snapshot_age_ms.has_value());
    ASSERT_EQ(ref.max_snapshot_age_ms.value(), 255486129307);
    ASSERT_TRUE(ref.max_ref_age_ms.has_value());
    ASSERT_EQ(ref.max_ref_age_ms.value(), 255486129308);
    ASSERT_TRUE(ref.min_snapshots_to_keep.has_value());
    ASSERT_EQ(ref.min_snapshots_to_keep.value(), 12345);

    const auto parsed_orig_as_str = iceberg::to_json_str(ref);
    json::Document parsed_roundtrip_json;
    parsed_roundtrip_json.Parse(parsed_orig_as_str);
    const auto roundtrip_ref = parse_snapshot_ref(parsed_roundtrip_json);

    ASSERT_EQ(roundtrip_ref, ref);
}

TEST(SnapshotJsonSerde, TestSnapshotReferencesMissingOptionals) {
    const auto test_ref_str = R"({
        "snapshot-id": 5937117119577207000,
        "type": "tag"
      })";
    json::Document parsed_orig_json;
    parsed_orig_json.Parse(test_ref_str);
    const auto ref = parse_snapshot_ref(parsed_orig_json);
    ASSERT_EQ(ref.snapshot_id(), 5937117119577207000);
    ASSERT_EQ(ref.type, snapshot_ref_type::tag);
    ASSERT_FALSE(ref.max_snapshot_age_ms.has_value());
    ASSERT_FALSE(ref.max_ref_age_ms.has_value());
    ASSERT_FALSE(ref.min_snapshots_to_keep.has_value());

    const auto parsed_orig_as_str = iceberg::to_json_str(ref);
    json::Document parsed_roundtrip_json;
    parsed_roundtrip_json.Parse(parsed_orig_as_str);
    const auto roundtrip_ref = parse_snapshot_ref(parsed_roundtrip_json);

    ASSERT_EQ(roundtrip_ref, ref);
}
