// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "container/fragmented_vector.h"
#include "iceberg/partition.h"

#include <gtest/gtest.h>

using namespace iceberg;

chunked_vector<transform> all_transforms() {
    chunked_vector<transform> all_transforms;

    all_transforms.emplace_back(identity_transform{});
    all_transforms.emplace_back(bucket_transform{.n = 1024});
    all_transforms.emplace_back(truncate_transform{.length = 2048});
    all_transforms.emplace_back(year_transform{});
    all_transforms.emplace_back(month_transform{});
    all_transforms.emplace_back(hour_transform{});
    all_transforms.emplace_back(void_transform{});
    return all_transforms;
}

void check_single_transform_exists(
  const transform& expected_type,
  const chunked_vector<transform>& all_transforms) {
    size_t num_eq = 0;
    size_t num_ne = 0;
    for (const auto& t : all_transforms) {
        if (t == expected_type) {
            ++num_eq;
        }
        if (t != expected_type) {
            ++num_ne;
        }
    }
    ASSERT_EQ(num_eq, 1);
    ASSERT_EQ(num_ne, all_transforms.size() - 1);
}

TEST(PartitionTest, TestTransformsEquality) {
    auto transforms = all_transforms();
    for (const auto& t : transforms) {
        ASSERT_NO_FATAL_FAILURE(
          check_single_transform_exists(t, all_transforms()));
    }
}

TEST(PartitionTest, TestBucketTransform) {
    bucket_transform t1{.n = 1024};
    auto t1_copy = t1;
    auto t2 = t1_copy;
    t2.n = 1025;
    ASSERT_EQ(t1, t1_copy);
    ASSERT_NE(t1, t2);
}

TEST(PartitionTest, TestTruncateTransform) {
    truncate_transform t1{.length = 1024};
    auto t1_copy = t1;
    auto t2 = t1_copy;
    t2.length = 1025;
    ASSERT_EQ(t1, t1_copy);
    ASSERT_NE(t1, t2);
}

TEST(PartitionTest, TestPartitionField) {
    auto make_field = [](int32_t i, const ss::sstring& name, transform t) {
        return partition_field{
          .field_id = partition_field::id_t{i},
          .name = name,
          .transform = t,
        };
    };
    auto t1 = make_field(0, "foo", identity_transform{});
    auto t1_copy = t1;
    auto t2 = make_field(1, "foo", identity_transform{});
    auto t3 = make_field(0, "fo", identity_transform{});
    auto t4 = make_field(0, "foo", void_transform{});
    ASSERT_EQ(t1, t1_copy);
    ASSERT_NE(t1, t2);
    ASSERT_NE(t1, t3);
    ASSERT_NE(t1, t4);
}

TEST(PartitionTest, TestPartitionSpec) {
    auto make_spec_single = [](
                              int32_t spec_id,
                              int32_t field_id,
                              const ss::sstring& name,
                              transform t) {
        return partition_spec{
          .spec_id = partition_spec::id_t{spec_id},
          .fields = chunked_vector<partition_field>{partition_field{
            .field_id = partition_field::id_t{field_id},
            .name = name,
            .transform = t,
          }}};
    };
    auto t1 = make_spec_single(0, 1, "foo", identity_transform{});
    auto t1_dup = t1.copy();
    ASSERT_EQ(t1, t1_dup);
    auto t2 = make_spec_single(1, 1, "foo", identity_transform{});
    auto t3 = make_spec_single(0, 0, "foo", identity_transform{});
    auto t4 = make_spec_single(0, 1, "fo", identity_transform{});
    auto t5 = make_spec_single(0, 1, "foo", void_transform{});
    auto t6 = make_spec_single(0, 1, "foo", void_transform{});
    t6.fields.pop_back();
    auto t7 = make_spec_single(0, 1, "foo", void_transform{});
    t7.fields.emplace_back(t1.fields[0]);

    ASSERT_EQ(t1, t1_dup);
    ASSERT_NE(t1, t2);
    ASSERT_NE(t1, t3);
    ASSERT_NE(t1, t4);
    ASSERT_NE(t1, t5);
    ASSERT_NE(t1, t6);
    ASSERT_NE(t1, t7);
}
