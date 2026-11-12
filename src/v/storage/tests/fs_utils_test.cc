/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "storage/fs_utils.h"
#include "storage/version.h"

#include <gtest/gtest.h>

#include <stdexcept>

namespace storage {

TEST(segment_path, parse_filename_versions) {
    for (auto [name, version] :
         {std::pair{"100-3-v1.log", record_version_type::v1},
          std::pair{"100-3-v2.log", record_version_type::v2}}) {
        auto meta = segment_path::parse_segment_filename(name);
        ASSERT_TRUE(meta.has_value()) << name;
        EXPECT_EQ(meta->base_offset, model::offset(100));
        EXPECT_EQ(meta->term, model::term_id(3));
        EXPECT_EQ(meta->version, version);
    }
}

TEST(segment_path, parse_filename_unknown_version) {
    EXPECT_THROW(
      segment_path::parse_segment_filename("100-3-v3.log"),
      std::invalid_argument);
    EXPECT_THROW(
      segment_path::parse_segment_filename("100-3-vx.log"),
      std::invalid_argument);
}

TEST(record_version, string_roundtrip) {
    for (auto version : {record_version_type::v1, record_version_type::v2}) {
        EXPECT_EQ(from_string(to_string(version)), version);
    }
}

} // namespace storage
