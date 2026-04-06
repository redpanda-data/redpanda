/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/lsm/keys.h"
#include "cloud_topics/level_one/metastore/lsm/values.h"
#include "cloud_topics/level_one/metastore/partition_validator.h"
#include "lsm/io/memory_persistence.h"
#include "lsm/lsm.h"
#include "serde/rw/rw.h"

#include <gtest/gtest.h>

using namespace cloud_topics;
using namespace cloud_topics::l1;

namespace {
kafka::offset operator""_o(unsigned long long o) {
    return kafka::offset{static_cast<int64_t>(o)};
}
model::timestamp operator""_t(unsigned long long t) {
    return model::timestamp{static_cast<int64_t>(t)};
}
} // namespace

class PartitionValidatorTest : public ::testing::Test {
protected:
    void SetUp() override {
        db_ = lsm::database::open(
                {.database_epoch = 0},
                lsm::io::persistence{
                  .data = lsm::io::make_memory_data_persistence(),
                  .metadata = lsm::io::make_memory_metadata_persistence()})
                .get();
    }
    void TearDown() override {
        if (db_) {
            db_->close().get();
        }
    }

    void write_metadata(
      const model::topic_id_partition& tp,
      kafka::offset start,
      kafka::offset next,
      partition_state::compaction_epoch_t epoch
      = partition_state::compaction_epoch_t{0}) {
        metadata_row_value md;
        md.start_offset = start;
        md.next_offset = next;
        md.compaction_epoch = epoch;
        auto wb = db_->create_write_batch();
        wb.put(metadata_row_key::encode(tp), serde::to_iobuf(md), next_seqno());
        db_->apply(std::move(wb)).get();
    }
    void write_extent(
      const model::topic_id_partition& tp,
      kafka::offset base,
      kafka::offset last,
      object_id oid) {
        extent_row_value ext;
        ext.last_offset = last;
        ext.len = 100;
        ext.oid = oid;
        auto wb = db_->create_write_batch();
        wb.put(
          extent_row_key::encode(tp, base), serde::to_iobuf(ext), next_seqno());
        db_->apply(std::move(wb)).get();
    }
    void write_term(
      const model::topic_id_partition& tp,
      model::term_id term,
      kafka::offset start_offset) {
        term_row_value trv;
        trv.term_start_offset = start_offset;
        auto wb = db_->create_write_batch();
        wb.put(
          term_row_key::encode(tp, term), serde::to_iobuf(trv), next_seqno());
        db_->apply(std::move(wb)).get();
    }
    void write_object(object_id oid, bool is_preregistration = false) {
        object_row_value orv;
        orv.object = object_entry{
          .total_data_size = 1000,
          .object_size = 1000,
          .is_preregistration = is_preregistration};
        auto wb = db_->create_write_batch();
        wb.put(object_row_key::encode(oid), serde::to_iobuf(orv), next_seqno());
        db_->apply(std::move(wb)).get();
    }
    void write_compaction(
      const model::topic_id_partition& tp, compaction_state comp) {
        auto wb = db_->create_write_batch();
        wb.put(
          compaction_row_key::encode(tp),
          serde::to_iobuf(compaction_row_value{.state = std::move(comp)}),
          next_seqno());
        db_->apply(std::move(wb)).get();
    }
    void add_extent(
      const model::topic_id_partition& tp,
      object_id oid,
      kafka::offset base,
      kafka::offset last) {
        write_extent(tp, base, last, oid);
        write_object(oid);
        if (base == 0_o) {
            write_term(tp, model::term_id{1}, 0_o);
        }
        write_metadata(tp, 0_o, kafka::next_offset(last));
    }
    partition_validation_result validate(validate_partition_options opts) {
        auto reader = state_reader(db_->create_snapshot());
        partition_validator validator(reader);
        auto result = validator.validate(std::move(opts)).get();
        EXPECT_TRUE(result.has_value()) << fmt::format("{}", result.error());
        return std::move(result.value());
    }
    void expect_clean(const partition_validation_result& r) {
        for (const auto& a : r.anomalies) {
            ADD_FAILURE() << fmt::format("[{}]: {}", a.type, a.description);
        }
    }
    void expect_anomalies(
      const partition_validation_result& r,
      std::initializer_list<anomaly_type> expected) {
        std::set<anomaly_type> es(expected);
        std::set<anomaly_type> actual;
        for (const auto& a : r.anomalies) {
            actual.insert(a.type);
        }
        for (auto t : es) {
            EXPECT_TRUE(actual.contains(t)) << "missing: " << to_string_view(t);
        }
        for (auto t : actual) {
            EXPECT_TRUE(es.contains(t)) << "unexpected: " << to_string_view(t);
        }
    }
    lsm::sequence_number next_seqno() {
        auto max = db_->max_applied_seqno();
        return max ? lsm::sequence_number{max.value()() + 1}
                   : lsm::sequence_number(1);
    }
    const model::topic_id_partition tp_a = model::topic_id_partition::from(
      "deadbeef-aaaa-0000-0000-000000000000/0");
    std::optional<lsm::database> db_;
};

TEST_F(PartitionValidatorTest, ValidPartition) {
    auto o1 = create_object_id(), o2 = create_object_id();
    add_extent(tp_a, o1, 0_o, 99_o);
    add_extent(tp_a, o2, 100_o, 199_o);
    auto r = validate({.tidp = tp_a});
    expect_clean(r);
    EXPECT_EQ(r.extents_validated, 2u);
    EXPECT_FALSE(r.resume_at_offset.has_value());
}
TEST_F(PartitionValidatorTest, ValidWithObjectCheck) {
    auto o = create_object_id();
    add_extent(tp_a, o, 0_o, 99_o);
    expect_clean(validate({.tidp = tp_a, .check_object_metadata = true}));
}
TEST_F(PartitionValidatorTest, EmptyPartition) {
    auto r = validate({.tidp = tp_a});
    expect_clean(r);
    EXPECT_EQ(r.extents_validated, 0u);
}
TEST_F(PartitionValidatorTest, MidExtentStartOffset) {
    auto o = create_object_id();
    write_metadata(tp_a, 50_o, 100_o);
    write_extent(tp_a, 0_o, 99_o, o);
    write_object(o);
    write_term(tp_a, model::term_id{1}, 0_o);
    expect_clean(validate({.tidp = tp_a}));
}
TEST_F(PartitionValidatorTest, ExtentGap) {
    auto o1 = create_object_id(), o2 = create_object_id();
    write_metadata(tp_a, 0_o, 300_o);
    write_extent(tp_a, 0_o, 99_o, o1);
    write_extent(tp_a, 200_o, 299_o, o2);
    write_object(o1);
    write_object(o2);
    write_term(tp_a, model::term_id{1}, 0_o);
    expect_anomalies(validate({.tidp = tp_a}), {anomaly_type::extent_gap});
}
TEST_F(PartitionValidatorTest, ExtentOverlap) {
    auto o1 = create_object_id(), o2 = create_object_id();
    write_metadata(tp_a, 0_o, 200_o);
    write_extent(tp_a, 0_o, 99_o, o1);
    write_extent(tp_a, 50_o, 199_o, o2);
    write_object(o1);
    write_object(o2);
    write_term(tp_a, model::term_id{1}, 0_o);
    expect_anomalies(validate({.tidp = tp_a}), {anomaly_type::extent_overlap});
}
TEST_F(PartitionValidatorTest, GapFromStartOffset) {
    auto o = create_object_id();
    write_metadata(tp_a, 0_o, 200_o);
    write_extent(tp_a, 100_o, 199_o, o);
    write_object(o);
    write_term(tp_a, model::term_id{1}, 0_o);
    expect_anomalies(validate({.tidp = tp_a}), {anomaly_type::extent_gap});
}
TEST_F(PartitionValidatorTest, StartOffsetMismatch) {
    auto o = create_object_id();
    write_metadata(tp_a, 200_o, 300_o);
    write_extent(tp_a, 0_o, 99_o, o);
    write_object(o);
    write_term(tp_a, model::term_id{1}, 0_o);
    auto r = validate({.tidp = tp_a});
    EXPECT_TRUE(std::ranges::any_of(r.anomalies, [](const auto& a) {
        return a.type == anomaly_type::start_offset_mismatch;
    }));
}
TEST_F(PartitionValidatorTest, NextOffsetMismatch) {
    auto o = create_object_id();
    add_extent(tp_a, o, 0_o, 99_o);
    write_metadata(tp_a, 0_o, 50_o);
    expect_anomalies(
      validate({.tidp = tp_a}), {anomaly_type::next_offset_mismatch});
}
TEST_F(PartitionValidatorTest, NoExtentsNonEmptyPartition) {
    write_metadata(tp_a, 0_o, 100_o);
    write_term(tp_a, model::term_id{1}, 0_o);
    expect_anomalies(
      validate({.tidp = tp_a}), {anomaly_type::next_offset_mismatch});
}
TEST_F(PartitionValidatorTest, PaginationBasic) {
    auto o1 = create_object_id(), o2 = create_object_id(),
         o3 = create_object_id();
    add_extent(tp_a, o1, 0_o, 99_o);
    add_extent(tp_a, o2, 100_o, 199_o);
    add_extent(tp_a, o3, 200_o, 299_o);
    auto p1 = validate({.tidp = tp_a, .max_extents = 2});
    expect_clean(p1);
    EXPECT_EQ(p1.extents_validated, 2u);
    ASSERT_TRUE(p1.resume_at_offset.has_value());
    EXPECT_EQ(*p1.resume_at_offset, 100_o);
    auto p2 = validate(
      {.tidp = tp_a,
       .resume_at_offset = p1.resume_at_offset,
       .max_extents = 2});
    expect_clean(p2);
    EXPECT_EQ(p2.extents_validated, 1u);
    EXPECT_FALSE(p2.resume_at_offset.has_value());
}
TEST_F(PartitionValidatorTest, ContinuationNoExtents) {
    auto o = create_object_id();
    add_extent(tp_a, o, 0_o, 99_o);
    expect_anomalies(
      validate({.tidp = tp_a, .resume_at_offset = 199_o}),
      {anomaly_type::extent_gap});
}
TEST_F(PartitionValidatorTest, NoTermsWithMetadata) {
    write_metadata(tp_a, 0_o, 0_o);
    expect_anomalies(validate({.tidp = tp_a}), {anomaly_type::term_ordering});
}
TEST_F(PartitionValidatorTest, FirstTermDoesNotCoverStart) {
    auto o = create_object_id();
    write_metadata(tp_a, 0_o, 100_o);
    write_extent(tp_a, 0_o, 99_o, o);
    write_object(o);
    write_term(tp_a, model::term_id{1}, 50_o);
    expect_anomalies(validate({.tidp = tp_a}), {anomaly_type::term_ordering});
}
TEST_F(PartitionValidatorTest, LastTermPastNextOffset) {
    auto o = create_object_id();
    write_metadata(tp_a, 0_o, 100_o);
    write_extent(tp_a, 0_o, 99_o, o);
    write_object(o);
    write_term(tp_a, model::term_id{1}, 0_o);
    write_term(tp_a, model::term_id{2}, 100_o);
    expect_anomalies(validate({.tidp = tp_a}), {anomaly_type::term_ordering});
}
TEST_F(PartitionValidatorTest, TermIdNotIncreasing) {
    auto o = create_object_id();
    add_extent(tp_a, o, 0_o, 99_o);
    write_term(tp_a, model::term_id{1}, 50_o);
    expect_anomalies(validate({.tidp = tp_a}), {anomaly_type::term_ordering});
}
TEST_F(PartitionValidatorTest, TermStartOffsetNotIncreasing) {
    auto o = create_object_id();
    add_extent(tp_a, o, 0_o, 99_o);
    write_term(tp_a, model::term_id{2}, 0_o);
    expect_anomalies(validate({.tidp = tp_a}), {anomaly_type::term_ordering});
}
TEST_F(PartitionValidatorTest, ObjectNotFound) {
    auto o = create_object_id();
    write_metadata(tp_a, 0_o, 100_o);
    write_extent(tp_a, 0_o, 99_o, o);
    write_term(tp_a, model::term_id{1}, 0_o);
    expect_anomalies(
      validate({.tidp = tp_a, .check_object_metadata = true}),
      {anomaly_type::object_not_found});
}
TEST_F(PartitionValidatorTest, ObjectPreregistered) {
    auto o = create_object_id();
    write_metadata(tp_a, 0_o, 100_o);
    write_extent(tp_a, 0_o, 99_o, o);
    write_object(o, true);
    write_term(tp_a, model::term_id{1}, 0_o);
    expect_anomalies(
      validate({.tidp = tp_a, .check_object_metadata = true}),
      {anomaly_type::object_preregistered});
}
TEST_F(PartitionValidatorTest, CompactionValid) {
    auto o = create_object_id();
    add_extent(tp_a, o, 0_o, 99_o);
    write_metadata(tp_a, 0_o, 100_o, partition_state::compaction_epoch_t{1});
    compaction_state c;
    c.cleaned_ranges.insert(0_o, 50_o);
    write_compaction(tp_a, std::move(c));
    expect_clean(validate({.tidp = tp_a}));
}
TEST_F(PartitionValidatorTest, CompactionRangeBelowStart) {
    auto o = create_object_id();
    add_extent(tp_a, o, 0_o, 99_o);
    write_metadata(tp_a, 10_o, 100_o, partition_state::compaction_epoch_t{1});
    compaction_state c;
    c.cleaned_ranges.insert(0_o, 50_o);
    write_compaction(tp_a, std::move(c));
    expect_anomalies(
      validate({.tidp = tp_a}), {anomaly_type::compaction_range_below_start});
}
TEST_F(PartitionValidatorTest, CompactionRangeAboveNext) {
    auto o = create_object_id();
    add_extent(tp_a, o, 0_o, 99_o);
    write_metadata(tp_a, 0_o, 100_o, partition_state::compaction_epoch_t{1});
    compaction_state c;
    c.cleaned_ranges.insert(0_o, 100_o);
    write_compaction(tp_a, std::move(c));
    expect_anomalies(
      validate({.tidp = tp_a}), {anomaly_type::compaction_range_above_next});
}
TEST_F(PartitionValidatorTest, CompactionTombstoneOutsideCleaned) {
    auto o = create_object_id();
    add_extent(tp_a, o, 0_o, 99_o);
    write_metadata(tp_a, 0_o, 100_o, partition_state::compaction_epoch_t{1});
    compaction_state c;
    c.cleaned_ranges.insert(0_o, 30_o);
    c.cleaned_ranges_with_tombstones.insert(
      compaction_state::cleaned_range_with_tombstones{
        .base_offset = 40_o,
        .last_offset = 60_o,
        .cleaned_with_tombstones_at = 1000_t});
    write_compaction(tp_a, std::move(c));
    expect_anomalies(
      validate({.tidp = tp_a}),
      {anomaly_type::compaction_tombstone_outside_cleaned});
}
TEST_F(PartitionValidatorTest, CompactionTombstoneOverlap) {
    auto o = create_object_id();
    add_extent(tp_a, o, 0_o, 99_o);
    write_metadata(tp_a, 0_o, 100_o, partition_state::compaction_epoch_t{1});
    compaction_state c;
    c.cleaned_ranges.insert(0_o, 50_o);
    c.cleaned_ranges_with_tombstones.insert(
      compaction_state::cleaned_range_with_tombstones{
        .base_offset = 0_o,
        .last_offset = 20_o,
        .cleaned_with_tombstones_at = 1000_t});
    c.cleaned_ranges_with_tombstones.insert(
      compaction_state::cleaned_range_with_tombstones{
        .base_offset = 15_o,
        .last_offset = 30_o,
        .cleaned_with_tombstones_at = 1000_t});
    write_compaction(tp_a, std::move(c));
    expect_anomalies(
      validate({.tidp = tp_a}), {anomaly_type::compaction_tombstone_overlap});
}
TEST_F(PartitionValidatorTest, CompactionStateWithoutEpoch) {
    auto o = create_object_id();
    add_extent(tp_a, o, 0_o, 99_o);
    compaction_state c;
    c.cleaned_ranges.insert(0_o, 50_o);
    write_compaction(tp_a, std::move(c));
    expect_anomalies(
      validate({.tidp = tp_a}), {anomaly_type::compaction_state_unexpected});
}
TEST_F(PartitionValidatorTest, CompactionEpochWithoutState) {
    auto o = create_object_id();
    add_extent(tp_a, o, 0_o, 99_o);
    write_metadata(tp_a, 0_o, 100_o, partition_state::compaction_epoch_t{1});
    expect_anomalies(
      validate({.tidp = tp_a}), {anomaly_type::compaction_state_unexpected});
}
TEST_F(PartitionValidatorTest, CompactionValidAfterSetStartOffset) {
    auto o = create_object_id();
    write_metadata(tp_a, 50_o, 100_o, partition_state::compaction_epoch_t{1});
    write_extent(tp_a, 0_o, 99_o, o);
    write_object(o);
    write_term(tp_a, model::term_id{1}, 0_o);
    compaction_state c;
    c.cleaned_ranges.insert(50_o, 70_o);
    write_compaction(tp_a, std::move(c));
    expect_clean(validate({.tidp = tp_a}));
}

// Simulate a prefix truncation between paginated validation calls.  Page 1
// validates [0,99] and returns resume_at_offset=0.  Before page 2,
// set_start_offset advances start to 100 and removes the [0,99] extent. The
// validator should detect that resume_at is below start_offset and fall back
// to first-page behavior.
TEST_F(PartitionValidatorTest, PrefixTruncationDuringPagination) {
    auto o1 = create_object_id();
    auto o2 = create_object_id();
    add_extent(tp_a, o1, 0_o, 99_o);
    add_extent(tp_a, o2, 100_o, 199_o);

    auto p1 = validate({.tidp = tp_a, .max_extents = 1});
    expect_clean(p1);
    ASSERT_TRUE(p1.resume_at_offset.has_value());

    // Simulate prefix truncation: advance start_offset, remove first extent.
    write_metadata(tp_a, 100_o, 200_o);
    {
        auto wb = db_->create_write_batch();
        wb.remove(extent_row_key::encode(tp_a, 0_o), next_seqno());
        db_->apply(std::move(wb)).get();
    }

    // Page 2 with stale resume_at_offset: clamped to start_offset=100,
    // re-reads [100,199] (exact match, not counted), no more extents.
    auto p2 = validate({.tidp = tp_a, .resume_at_offset = p1.resume_at_offset});
    expect_clean(p2);
    EXPECT_FALSE(p2.resume_at_offset.has_value());
}

TEST_F(PartitionValidatorTest, MidExtentStartWithPagination) {
    auto o1 = create_object_id();
    auto o2 = create_object_id();
    write_metadata(tp_a, 50_o, 200_o);

    // NOTE: first extent falls below start offset.
    write_extent(tp_a, 0_o, 99_o, o1);
    write_extent(tp_a, 100_o, 199_o, o2);
    write_object(o1);
    write_object(o2);
    write_term(tp_a, model::term_id{1}, 0_o);

    auto p1 = validate({.tidp = tp_a, .max_extents = 1});
    expect_clean(p1);
    EXPECT_EQ(p1.extents_validated, 1);
    ASSERT_TRUE(p1.resume_at_offset.has_value());

    // The returned resume should be the first extent, which is below the start
    // offset.
    EXPECT_EQ(*p1.resume_at_offset, 0_o);

    // We should be able to page through the rest of the partition without
    // issues (e.g. no looping on the same extent).
    auto p2 = validate(
      {.tidp = tp_a,
       .resume_at_offset = p1.resume_at_offset,
       .max_extents = 1});
    expect_clean(p2);
    EXPECT_TRUE(p2.resume_at_offset.has_value());
    EXPECT_EQ(*p2.resume_at_offset, 100_o);
    EXPECT_GE(p2.extents_validated, 1);

    auto p3 = validate(
      {.tidp = tp_a,
       .resume_at_offset = p2.resume_at_offset,
       .max_extents = 1});
    expect_clean(p3);
    EXPECT_FALSE(p3.resume_at_offset.has_value());
    EXPECT_GE(p3.extents_validated, 0);
}
