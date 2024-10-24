/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "base/seastarx.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "storage/fs_utils.h"
#include "storage/segment.h"
#include "storage/storage_resources.h"

#include <gtest/gtest.h>

namespace storage {

struct segment_offset_tracker_fixture : public testing::Test {
    segment make_segment(model::offset base_offset) {
        segment_index idx(
          segment_full_path::mock("mocked"),
          base_offset,
          1024,
          features,
          std::nullopt);
        return segment(
          segment::offset_tracker(model::term_id(0), base_offset),
          nullptr,
          std::move(idx),
          nullptr,
          std::nullopt,
          std::nullopt,
          resources);
    }

    ss::sharded<features::feature_table> features;
    storage_resources resources;
};

TEST_F(segment_offset_tracker_fixture, get_and_set_offsets) {
    auto segment = make_segment(model::offset{1});
    auto& offsets = const_cast<segment::offset_tracker&>(segment.offsets());
    EXPECT_EQ(offsets.get_term(), model::term_id{0});
    EXPECT_EQ(offsets.get_base_offset(), model::offset{1});

    EXPECT_EQ(offsets.get_committed_offset(), model::offset{0});
    EXPECT_EQ(offsets.get_stable_offset(), model::offset{0});
    EXPECT_EQ(offsets.get_dirty_offset(), model::offset{0});

    using committed_offset_t = segment::offset_tracker::committed_offset_t;
    using stable_offset_t = segment::offset_tracker::stable_offset_t;
    using dirty_offset_t = segment::offset_tracker::dirty_offset_t;

    offsets.set_offsets(
      committed_offset_t{1}, stable_offset_t{2}, dirty_offset_t{3});

    EXPECT_EQ(offsets.get_committed_offset(), model::offset{1});
    EXPECT_EQ(offsets.get_stable_offset(), model::offset{2});
    EXPECT_EQ(offsets.get_dirty_offset(), model::offset{3});

    offsets.set_offset(dirty_offset_t{6});
    EXPECT_EQ(offsets.get_dirty_offset(), model::offset{6});

    offsets.set_offset(stable_offset_t{5});
    EXPECT_EQ(offsets.get_stable_offset(), model::offset{5});

    offsets.set_offset(committed_offset_t{4});
    EXPECT_EQ(offsets.get_committed_offset(), model::offset{4});
}

}; // namespace storage
