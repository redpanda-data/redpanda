// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/partition.h"
#include "iceberg/partition_json.h"

#include <gtest/gtest.h>

using namespace iceberg;

TEST(PartitionJsonSerde, TestEmptyPartitionSpec) {
    const auto test_str = R"(
        {
        "spec-id": 1,
        "fields": []
        }
)";
    json::Document parsed_orig_json;
    parsed_orig_json.Parse(test_str);
    const auto parsed = parse_partition_spec(parsed_orig_json);
    ASSERT_EQ(1, parsed.spec_id());
    ASSERT_TRUE(parsed.fields.empty());

    const auto parsed_orig_as_str = iceberg::to_json_str(parsed);
    json::Document parsed_roundtrip_json;
    parsed_roundtrip_json.Parse(parsed_orig_as_str);
    const auto roundtrip = parse_partition_spec(parsed_roundtrip_json);

    ASSERT_EQ(roundtrip, parsed);
}

TEST(PartitionJsonSerde, TestPartitionSpec) {
    const auto test_str = R"(
        {
        "spec-id": 1,
        "fields": [ {
            "source-id": 4,
            "field-id": 1000,
            "name": "ts_day",
            "transform": "day"
            }, {
            "source-id": 1,
            "field-id": 1001,
            "name": "id_bucket",
            "transform": "bucket[16]"
            }, {
            "source-id": 2,
            "field-id": 1002,
            "name": "id_truncate",
            "transform": "truncate[4]"
            } ]
        }
)";
    json::Document parsed_orig_json;
    parsed_orig_json.Parse(test_str);
    const auto parsed = parse_partition_spec(parsed_orig_json);
    ASSERT_EQ(1, parsed.spec_id());
    ASSERT_EQ(3, parsed.fields.size());
    ASSERT_EQ(4, parsed.fields[0].source_id());
    ASSERT_EQ(1, parsed.fields[1].source_id());
    ASSERT_EQ(2, parsed.fields[2].source_id());
    ASSERT_EQ(1000, parsed.fields[0].field_id());
    ASSERT_EQ(1001, parsed.fields[1].field_id());
    ASSERT_EQ(1002, parsed.fields[2].field_id());
    ASSERT_STREQ("ts_day", parsed.fields[0].name.c_str());
    ASSERT_STREQ("id_bucket", parsed.fields[1].name.c_str());
    ASSERT_STREQ("id_truncate", parsed.fields[2].name.c_str());
    ASSERT_EQ(day_transform{}, parsed.fields[0].transform);
    ASSERT_EQ(bucket_transform{16}, parsed.fields[1].transform);
    ASSERT_EQ(truncate_transform{4}, parsed.fields[2].transform);

    const auto parsed_orig_as_str = to_json_str(parsed);
    json::Document parsed_roundtrip_json;
    parsed_roundtrip_json.Parse(parsed_orig_as_str);
    const auto roundtrip = parse_partition_spec(parsed_roundtrip_json);

    ASSERT_EQ(roundtrip, parsed);
}
