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
    EXPECT_EQ(offsets.get_base_term(), model::term_id{0});
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

TEST_F(segment_offset_tracker_fixture, term_spans_single) {
    auto segment = make_segment(model::offset{10});
    auto& offsets = const_cast<segment::offset_tracker&>(segment.offsets());
    using dirty_offset_t = segment::offset_tracker::dirty_offset_t;

    EXPECT_EQ(offsets.get_base_term(), model::term_id{0});
    EXPECT_EQ(offsets.last_term(), model::term_id{0});
    EXPECT_EQ(offsets.last_term_base_offset(), model::offset{10});
    // a segment with no appended data covers no offsets
    EXPECT_EQ(offsets.term_at(model::offset{10}), std::nullopt);

    offsets.set_offset(dirty_offset_t{15});
    EXPECT_EQ(offsets.term_at(model::offset{10}), model::term_id{0});
    EXPECT_EQ(offsets.term_at(model::offset{15}), model::term_id{0});
    // offsets outside [base, dirty] have no term
    EXPECT_EQ(offsets.term_at(model::offset{0}), std::nullopt);
    EXPECT_EQ(offsets.term_at(model::offset{16}), std::nullopt);
}

TEST_F(segment_offset_tracker_fixture, term_spans_multi) {
    auto segment = make_segment(model::offset{10});
    auto& offsets = const_cast<segment::offset_tracker&>(segment.offsets());
    using dirty_offset_t = segment::offset_tracker::dirty_offset_t;

    // spans: [10, 20) -> 0, [20, 50) -> 2, [50, dirty] -> 5
    offsets.add_term_span(model::term_id{2}, model::offset{20});
    offsets.add_term_span(model::term_id{5}, model::offset{50});
    offsets.set_offset(dirty_offset_t{59});

    EXPECT_EQ(offsets.get_base_term(), model::term_id{0});
    EXPECT_EQ(offsets.last_term(), model::term_id{5});
    EXPECT_EQ(offsets.last_term_base_offset(), model::offset{50});

    EXPECT_EQ(offsets.term_at(model::offset{10}), model::term_id{0});
    EXPECT_EQ(offsets.term_at(model::offset{19}), model::term_id{0});
    EXPECT_EQ(offsets.term_at(model::offset{20}), model::term_id{2});
    EXPECT_EQ(offsets.term_at(model::offset{49}), model::term_id{2});
    EXPECT_EQ(offsets.term_at(model::offset{50}), model::term_id{5});
    // the last span is bounded by the dirty offset
    EXPECT_EQ(offsets.term_at(model::offset{59}), model::term_id{5});
    EXPECT_EQ(offsets.term_at(model::offset{100}), std::nullopt);

    EXPECT_EQ(offsets.term_last_offset(model::term_id{0}), model::offset{19});
    EXPECT_EQ(offsets.term_last_offset(model::term_id{2}), model::offset{49});
    // last term span is open: bounded by the dirty offset
    EXPECT_EQ(offsets.term_last_offset(model::term_id{5}), model::offset{59});
    // terms may be skipped (failed elections)
    EXPECT_EQ(offsets.term_last_offset(model::term_id{1}), std::nullopt);
    EXPECT_EQ(offsets.term_last_offset(model::term_id{6}), std::nullopt);
}

TEST_F(segment_offset_tracker_fixture, term_spans_truncate) {
    auto segment = make_segment(model::offset{10});
    auto& offsets = const_cast<segment::offset_tracker&>(segment.offsets());
    using dirty_offset_t = segment::offset_tracker::dirty_offset_t;

    offsets.add_term_span(model::term_id{2}, model::offset{20});
    offsets.add_term_span(model::term_id{5}, model::offset{50});
    offsets.set_offset(dirty_offset_t{59});

    // truncate into the middle span: the later span is dropped and the
    // middle span re-opens (segment truncation shrinks the tracked
    // offsets alongside)
    offsets.set_offset(dirty_offset_t{30});
    offsets.truncate_term_spans(model::offset{30});
    EXPECT_EQ(offsets.last_term(), model::term_id{2});
    EXPECT_EQ(offsets.last_term_base_offset(), model::offset{20});
    EXPECT_EQ(offsets.term_at(model::offset{30}), model::term_id{2});
    EXPECT_EQ(offsets.term_at(model::offset{55}), std::nullopt);
    EXPECT_EQ(offsets.term_last_offset(model::term_id{5}), std::nullopt);

    // spans can be re-added after truncation
    offsets.add_term_span(model::term_id{3}, model::offset{31});
    offsets.set_offset(dirty_offset_t{31});
    EXPECT_EQ(offsets.term_at(model::offset{31}), model::term_id{3});
    EXPECT_EQ(offsets.term_last_offset(model::term_id{2}), model::offset{30});

    // truncating below the base retains the base span
    offsets.truncate_term_spans(model::offset{0});
    EXPECT_EQ(offsets.get_base_term(), model::term_id{0});
    EXPECT_EQ(offsets.last_term(), model::term_id{0});
}

TEST_F(segment_offset_tracker_fixture, term_spans_truncate_boundaries) {
    auto segment = make_segment(model::offset{10});
    auto& offsets = const_cast<segment::offset_tracker&>(segment.offsets());
    using dirty_offset_t = segment::offset_tracker::dirty_offset_t;

    // spans: [10, 20) -> 0, [20, 50) -> 2, [50, dirty] -> 5
    offsets.add_term_span(model::term_id{2}, model::offset{20});
    offsets.add_term_span(model::term_id{5}, model::offset{50});
    offsets.set_offset(dirty_offset_t{59});

    // truncating to the last offset before a span's base drops the span
    offsets.set_offset(dirty_offset_t{49});
    offsets.truncate_term_spans(model::offset{49});
    EXPECT_EQ(offsets.last_term(), model::term_id{2});
    EXPECT_EQ(offsets.term_at(model::offset{49}), model::term_id{2});
    EXPECT_EQ(offsets.term_last_offset(model::term_id{5}), std::nullopt);

    // truncating to exactly a span's base offset keeps the span
    offsets.add_term_span(model::term_id{5}, model::offset{50});
    offsets.set_offset(dirty_offset_t{50});
    offsets.truncate_term_spans(model::offset{50});
    EXPECT_EQ(offsets.last_term(), model::term_id{5});
    EXPECT_EQ(offsets.term_at(model::offset{50}), model::term_id{5});
    EXPECT_EQ(offsets.term_last_offset(model::term_id{5}), model::offset{50});

    // same for a middle span's base offset: only later spans are dropped
    offsets.set_offset(dirty_offset_t{20});
    offsets.truncate_term_spans(model::offset{20});
    EXPECT_EQ(offsets.last_term(), model::term_id{2});
    EXPECT_EQ(offsets.term_at(model::offset{20}), model::term_id{2});
    EXPECT_EQ(offsets.term_last_offset(model::term_id{2}), model::offset{20});
}

TEST_F(segment_offset_tracker_fixture, copy_snapshot) {
    auto segment = make_segment(model::offset{10});
    auto& offsets = const_cast<segment::offset_tracker&>(segment.offsets());
    using committed_offset_t = segment::offset_tracker::committed_offset_t;
    using stable_offset_t = segment::offset_tracker::stable_offset_t;
    using dirty_offset_t = segment::offset_tracker::dirty_offset_t;

    offsets.add_term_span(model::term_id{2}, model::offset{20});
    offsets.set_offsets(
      committed_offset_t{15}, stable_offset_t{17}, dirty_offset_t{25});

    const auto snap = offsets.copy();

    // mutating the original does not affect the snapshot
    offsets.add_term_span(model::term_id{5}, model::offset{30});
    offsets.set_offset(dirty_offset_t{35});
    EXPECT_EQ(offsets.term_at(model::offset{30}), model::term_id{5});

    EXPECT_EQ(snap.get_base_offset(), model::offset{10});
    EXPECT_EQ(snap.get_base_term(), model::term_id{0});
    EXPECT_EQ(snap.get_committed_offset(), model::offset{15});
    EXPECT_EQ(snap.get_stable_offset(), model::offset{17});
    EXPECT_EQ(snap.get_dirty_offset(), model::offset{25});
    EXPECT_EQ(snap.last_term(), model::term_id{2});
    EXPECT_EQ(snap.term_at(model::offset{25}), model::term_id{2});
    EXPECT_EQ(snap.term_at(model::offset{30}), std::nullopt);
}

TEST_F(segment_offset_tracker_fixture, term_last_offset_below_base_term) {
    segment::offset_tracker offsets(model::term_id{3}, model::offset{10});
    using dirty_offset_t = segment::offset_tracker::dirty_offset_t;

    offsets.add_term_span(model::term_id{5}, model::offset{20});
    offsets.set_offset(dirty_offset_t{29});

    // terms before the base term are not covered by this segment
    EXPECT_EQ(offsets.term_last_offset(model::term_id{1}), std::nullopt);
    EXPECT_EQ(offsets.term_last_offset(model::term_id{3}), model::offset{19});
}

TEST_F(segment_offset_tracker_fixture, term_last_offset_empty_segment) {
    // an empty segment reports its base term as ending at base - 1, the
    // log's tail before this segment. The log-level term scan
    // (disk_log_impl::get_term_last_offset) directs a query to the newest
    // segment whose base term is <= the queried term, so a freshly rolled
    // empty active segment must answer with the previous segment's tail.
    auto segment = make_segment(model::offset{10});
    const auto& offsets = segment.offsets();

    EXPECT_EQ(offsets.term_last_offset(model::term_id{0}), model::offset{9});
    EXPECT_EQ(offsets.term_last_offset(model::term_id{1}), std::nullopt);
}

}; // namespace storage
