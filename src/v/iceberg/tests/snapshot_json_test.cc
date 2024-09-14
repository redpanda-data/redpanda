// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/snapshot.h"
#include "iceberg/snapshot_json.h"
#include "json/document.h"
#include "json/stringbuffer.h"

#include <gtest/gtest.h>

using namespace iceberg;

namespace {
ss::sstring snapshot_to_json_str(const snapshot& s) {
    json::StringBuffer buf;
    json::Writer<json::StringBuffer> w(buf);
    rjson_serialize(w, s);
    return buf.GetString();
}
} // namespace

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

    const auto parsed_orig_as_str = snapshot_to_json_str(snap);
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

    const auto parsed_orig_as_str = snapshot_to_json_str(snap);
    json::Document parsed_roundtrip_json;
    parsed_roundtrip_json.Parse(parsed_orig_as_str);
    const auto roundtrip_snap = parse_snapshot(parsed_roundtrip_json);

    ASSERT_EQ(roundtrip_snap, snap);
}
