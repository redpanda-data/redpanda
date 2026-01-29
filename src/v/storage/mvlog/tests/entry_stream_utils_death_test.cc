// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/mvlog/entry.h"
#include "storage/mvlog/entry_stream_utils.h"

#include <gtest/gtest.h>

using namespace storage::experimental::mvlog;

TEST(EntryHeaderDeathTest, TestDeserializeTooSmall) {
    GTEST_SKIP()
      << "TODO(death_tests): re-enable when death tests are made stable in CI.";
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    entry_header orig_h{0, 0, entry_type::record_batch};
    auto buf = entry_header_to_iobuf(orig_h);
    ASSERT_EQ(buf.size_bytes(), packed_entry_header_size);

    // For these lower level utilities, callers are expected to pass in
    // appropriately sized buffers.
    buf.pop_back();
    ASSERT_DEATH(
      entry_header_from_iobuf(std::move(buf)), "Expected buf of size 9");
}
