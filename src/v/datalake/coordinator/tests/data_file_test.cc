/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "bytes/bytes.h"
#include "datalake/coordinator/data_file.h"
#include "datalake/coordinator/translated_offset_range.h"

#include <gtest/gtest.h>

#include <string_view>

using namespace datalake::coordinator;

namespace {
constexpr std::string_view test_path = "path.parquet";

data_file make_file_with_stats(size_t bound_bytes) {
    data_file f;
    f.remote_path = ss::sstring(test_path);
    datalake::per_column_stats cs;
    cs.field_id = 1;
    cs.lower_bound = bytes::from_string(std::string(bound_bytes, 'a'));
    cs.upper_bound = bytes::from_string(std::string(bound_bytes, 'z'));
    f.column_stats.push_back(std::move(cs));
    return f;
}
} // namespace

// Exactly the struct plus the heap it points at, for a narrow and a wide stat
// payload: the bounds are the term that varies across schemas.
TEST(EstimatedMemoryBytes, CountsStructAndHeap) {
    auto expected = [](size_t bound_bytes) {
        return sizeof(data_file) + test_path.size()
               + sizeof(datalake::per_column_stats) + 2 * bound_bytes;
    };
    EXPECT_EQ(estimated_memory_bytes(make_file_with_stats(8)), expected(8));
    EXPECT_EQ(
      estimated_memory_bytes(make_file_with_stats(4096)), expected(4096));
}

// A range costs its own struct plus every file it holds, main and DLQ.
TEST(EstimatedMemoryBytes, RangeSumsMainAndDlq) {
    translated_offset_range r;
    r.files.push_back(make_file_with_stats(16));
    r.dlq_files.push_back(make_file_with_stats(16));
    EXPECT_EQ(
      estimated_memory_bytes(r),
      sizeof(translated_offset_range)
        + 2 * estimated_memory_bytes(make_file_with_stats(16)));
}
