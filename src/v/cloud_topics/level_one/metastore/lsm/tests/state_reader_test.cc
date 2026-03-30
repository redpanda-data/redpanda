/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/lsm/keys.h"
#include "cloud_topics/level_one/metastore/lsm/state_reader.h"
#include "cloud_topics/level_one/metastore/lsm/values.h"
#include "cloud_topics/level_one/metastore/state.h"
#include "lsm/io/memory_persistence.h"
#include "lsm/lsm.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "serde/rw/rw.h"
#include "test_utils/test_macros.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace cloud_topics::l1;

class StateReaderTestFixture : public ::testing::Test {
protected:
    void SetUp() override {
        db_ = lsm::database::open(
                {.database_epoch = 0},
                lsm::io::persistence{
                  .data = lsm::io::make_memory_data_persistence(),
                  .metadata = lsm::io::make_memory_metadata_persistence(),
                })
                .get();
    }
    void TearDown() override {
        if (db_) {
            db_->close().get();
        }
    }

    model::topic_id_partition make_tidp(int partition = 0) {
        return model::topic_id_partition(
          model::topic_id(
            uuid_t::from_string("12345678-1234-5678-1234-567812345678")),
          model::partition_id(partition));
    }

    object_id make_oid() { return object_id(uuid_t::create()); }

    void write_metadata(
      const model::topic_id_partition& tidp,
      kafka::offset start_offset,
      kafka::offset next_offset) {
        auto key = metadata_row_key::encode(tidp);
        metadata_row_value val{
          .start_offset = start_offset,
          .next_offset = next_offset,
        };
        auto val_buf = serde::to_iobuf(std::move(val));

        auto wb = db_->create_write_batch();
        wb.put(key, std::move(val_buf), next_seqno());
        db_->apply(std::move(wb)).get();
    }

    void write_extent(
      const model::topic_id_partition& tidp,
      kafka::offset base_offset,
      kafka::offset last_offset,
      model::timestamp max_timestamp,
      size_t filepos,
      size_t len,
      object_id oid) {
        auto key = extent_row_key::encode(tidp, base_offset);
        extent_row_value val{
          .last_offset = last_offset,
          .max_timestamp = max_timestamp,
          .filepos = filepos,
          .len = len,
          .oid = oid,
        };
        auto val_buf = serde::to_iobuf(std::move(val));

        auto wb = db_->create_write_batch();
        wb.put(key, std::move(val_buf), next_seqno());
        db_->apply(std::move(wb)).get();
    }

    void write_term_start(
      const model::topic_id_partition& tidp,
      model::term_id term,
      kafka::offset term_start_offset) {
        auto key = term_row_key::encode(tidp, term);
        term_row_value val{
          .term_start_offset = term_start_offset,
        };
        auto val_buf = serde::to_iobuf(std::move(val));

        auto wb = db_->create_write_batch();
        wb.put(key, std::move(val_buf), next_seqno());
        db_->apply(std::move(wb)).get();
    }

    void write_compaction_state(
      const model::topic_id_partition& tidp, compaction_state state) {
        auto key = compaction_row_key::encode(tidp);
        compaction_row_value val{
          .state = std::move(state),
        };
        auto val_buf = serde::to_iobuf(std::move(val));

        auto wb = db_->create_write_batch();
        wb.put(key, std::move(val_buf), next_seqno());
        db_->apply(std::move(wb)).get();
    }

    void write_object(object_id oid, object_entry entry) {
        auto key = object_row_key::encode(oid);
        object_row_value val{
          .object = std::move(entry),
        };
        auto val_buf = serde::to_iobuf(std::move(val));

        auto wb = db_->create_write_batch();
        wb.put(key, std::move(val_buf), next_seqno());
        db_->apply(std::move(wb)).get();
    }

    state_reader make_reader() {
        auto snap = db_->create_snapshot();
        return state_reader(std::move(snap));
    }

    object_entry make_object_entry(size_t object_size = 1024) {
        return object_entry{
          .total_data_size = object_size,
          .object_size = object_size,
        };
    }

    lsm::sequence_number next_seqno() {
        auto max_applied_opt = db_->max_applied_seqno();
        if (!max_applied_opt) {
            return lsm::sequence_number(1);
        }
        return lsm::sequence_number{max_applied_opt.value()() + 1};
    }

    std::optional<lsm::database> db_;
};

TEST_F(StateReaderTestFixture, TestGetMetadata) {
    auto tidp0 = make_tidp(0);
    auto tidp1 = make_tidp(1);

    write_metadata(tidp0, kafka::offset(100), kafka::offset(200));
    write_metadata(tidp1, kafka::offset(300), kafka::offset(400));

    auto reader = make_reader();

    auto res0 = reader.get_metadata(tidp0).get();
    ASSERT_TRUE(res0.has_value());
    ASSERT_TRUE(res0.value().has_value());
    EXPECT_EQ(res0.value()->start_offset, kafka::offset(100));
    EXPECT_EQ(res0.value()->next_offset, kafka::offset(200));

    auto res1 = reader.get_metadata(tidp1).get();
    ASSERT_TRUE(res1.has_value());
    ASSERT_TRUE(res1.value().has_value());
    EXPECT_EQ(res1.value()->start_offset, kafka::offset(300));
    EXPECT_EQ(res1.value()->next_offset, kafka::offset(400));

    auto missing_tidp = make_tidp(2);
    auto missing_res = reader.get_metadata(missing_tidp).get();
    EXPECT_TRUE(missing_res.has_value());
    EXPECT_FALSE(missing_res.value().has_value());
}

TEST_F(StateReaderTestFixture, TestGetCompactionMetadata) {
    auto tidp0 = make_tidp(0);
    auto tidp1 = make_tidp(1);

    auto state = compaction_state{};
    write_compaction_state(tidp0, state);
    write_compaction_state(tidp1, state);

    auto reader = make_reader();

    auto res0 = reader.get_compaction_metadata(tidp0).get();
    ASSERT_TRUE(res0.has_value());
    ASSERT_TRUE(res0.value().has_value());

    auto res1 = reader.get_compaction_metadata(tidp1).get();
    ASSERT_TRUE(res1.has_value());
    ASSERT_TRUE(res1.value().has_value());

    auto missing_tidp = make_tidp(2);
    auto missing_res = reader.get_compaction_metadata(missing_tidp).get();
    EXPECT_TRUE(missing_res.has_value());
    EXPECT_FALSE(missing_res.value().has_value());
}

TEST_F(StateReaderTestFixture, TestGetObject) {
    auto oid0 = make_oid();
    auto oid1 = make_oid();
    write_object(oid0, make_object_entry(2048));
    write_object(oid1, make_object_entry(4096));

    auto reader = make_reader();

    auto res0 = reader.get_object(oid0).get();
    ASSERT_TRUE(res0.has_value());
    ASSERT_TRUE(res0.value().has_value());
    EXPECT_EQ(res0.value()->object_size, 2048);
    EXPECT_EQ(res0.value()->total_data_size, 2048);

    auto res1 = reader.get_object(oid1).get();
    ASSERT_TRUE(res1.has_value());
    ASSERT_TRUE(res1.value().has_value());
    EXPECT_EQ(res1.value()->object_size, 4096);
    EXPECT_EQ(res1.value()->total_data_size, 4096);

    auto missing_oid = make_oid();
    auto missing_res = reader.get_object(missing_oid).get();
    EXPECT_TRUE(missing_res.has_value());
    EXPECT_FALSE(missing_res.value().has_value());
}

TEST_F(StateReaderTestFixture, TestGetMaxTerm) {
    auto tidp0 = make_tidp(0);
    auto tidp1 = make_tidp(1);

    // Write a few terms for one partition...
    write_term_start(tidp0, model::term_id(1), kafka::offset(0));
    write_term_start(tidp0, model::term_id(3), kafka::offset(100));
    write_term_start(tidp0, model::term_id(7), kafka::offset(250));

    // ...and just one for another partition.
    write_term_start(tidp1, model::term_id(10), kafka::offset(200));

    auto reader = make_reader();
    auto res0 = reader.get_max_term(tidp0).get();
    ASSERT_TRUE(res0.has_value());
    ASSERT_TRUE(res0.value().has_value());
    EXPECT_EQ(res0.value()->term_id, model::term_id(7));
    EXPECT_EQ(res0.value()->start_offset, kafka::offset(250));

    auto res1 = reader.get_max_term(tidp1).get();
    ASSERT_TRUE(res1.has_value());
    ASSERT_TRUE(res1.value().has_value());
    EXPECT_EQ(res1.value()->term_id, model::term_id(10));
    EXPECT_EQ(res1.value()->start_offset, kafka::offset(200));

    auto missing_tidp = make_tidp(2);
    auto missing_res = reader.get_max_term(missing_tidp).get();
    EXPECT_TRUE(missing_res.has_value());
    EXPECT_FALSE(missing_res.value().has_value());
}

TEST_F(StateReaderTestFixture, TestGetExtentGe) {
    auto tidp0 = make_tidp(0);
    auto oid = make_oid();
    write_extent(
      tidp0,
      kafka::offset(100),
      kafka::offset(199),
      model::timestamp(1000),
      0,
      1024,
      oid);
    write_extent(
      tidp0,
      kafka::offset(200),
      kafka::offset(299),
      model::timestamp(2000),
      1024,
      1024,
      oid);

    auto reader = make_reader();

    // Test a few key values for the first extent.
    for (const auto o : {0, 99, 100, 199}) {
        SCOPED_TRACE(fmt::format("Querying offset {}", o));
        auto res_exact = reader.get_extent_ge(tidp0, kafka::offset(o)).get();
        ASSERT_TRUE(res_exact.has_value());
        ASSERT_TRUE(res_exact.value().has_value());
        EXPECT_EQ(res_exact.value()->base_offset, kafka::offset(100));
        EXPECT_EQ(res_exact.value()->last_offset, kafka::offset(199));
    }

    // Test a few key values for the second extent.
    for (const auto o : {200, 299}) {
        SCOPED_TRACE(fmt::format("Querying offset {}", o));
        auto res_next = reader.get_extent_ge(tidp0, kafka::offset(o)).get();
        ASSERT_TRUE(res_next.has_value());
        ASSERT_TRUE(res_next.value().has_value());
        EXPECT_EQ(res_next.value()->base_offset, kafka::offset(200));
        EXPECT_EQ(res_next.value()->last_offset, kafka::offset(299));
    }

    // Test query beyond all extents
    auto res_missing = reader.get_extent_ge(tidp0, kafka::offset(300)).get();
    ASSERT_TRUE(res_missing.has_value());
    EXPECT_FALSE(res_missing.value().has_value());
}

TEST_F(StateReaderTestFixture, TestGetExtentGeSurroundingPartitions) {
    auto tidp0 = make_tidp(0);
    auto tidp1 = make_tidp(1);
    auto tidp2 = make_tidp(2);
    auto oid = make_oid();

    // We'll query tidp1, so sandwich its extents with tidp0 and tidp2.
    write_extent(
      tidp0,
      kafka::offset(0),
      kafka::offset(199),
      model::timestamp(1000),
      0,
      1024,
      oid);
    write_extent(
      tidp1,
      kafka::offset(100),
      kafka::offset(199),
      model::timestamp(1000),
      0,
      1024,
      oid);
    write_extent(
      tidp1,
      kafka::offset(200),
      kafka::offset(299),
      model::timestamp(2000),
      1024,
      1024,
      oid);
    write_extent(
      tidp2,
      kafka::offset(0),
      kafka::offset(199),
      model::timestamp(1000),
      0,
      1024,
      oid);

    auto reader = make_reader();

    // Test a few key values for the first extent.
    for (const auto o : {0, 99, 100, 199}) {
        SCOPED_TRACE(fmt::format("Querying offset {}", o));
        auto res_exact = reader.get_extent_ge(tidp1, kafka::offset(o)).get();
        ASSERT_TRUE(res_exact.has_value());
        ASSERT_TRUE(res_exact.value().has_value());
        EXPECT_EQ(res_exact.value()->base_offset, kafka::offset(100));
        EXPECT_EQ(res_exact.value()->last_offset, kafka::offset(199));
    }

    // Test a few key values for the second extent.
    for (const auto o : {200, 299}) {
        SCOPED_TRACE(fmt::format("Querying offset {}", o));
        auto res_next = reader.get_extent_ge(tidp1, kafka::offset(o)).get();
        ASSERT_TRUE(res_next.has_value());
        ASSERT_TRUE(res_next.value().has_value());
        EXPECT_EQ(res_next.value()->base_offset, kafka::offset(200));
        EXPECT_EQ(res_next.value()->last_offset, kafka::offset(299));
    }

    // Test query beyond all extents
    auto res_missing = reader.get_extent_ge(tidp1, kafka::offset(300)).get();
    ASSERT_TRUE(res_missing.has_value());
    EXPECT_FALSE(res_missing.value().has_value());
}

namespace {

void validate_extent_range(
  state_reader& reader,
  const model::topic_id_partition& tidp,
  int64_t base,
  int64_t last,
  size_t expected_extents) {
    auto range_res
      = reader.get_extent_range(tidp, kafka::offset(base), kafka::offset(last))
          .get();
    ASSERT_TRUE(range_res.has_value());
    if (expected_extents == 0) {
        EXPECT_FALSE(range_res.value().has_value());
        return;
    }
    EXPECT_TRUE(range_res.value().has_value());
    auto rows = range_res.value().value().materialize_rows().get();
    EXPECT_EQ(rows.size(), expected_extents);
    ASSERT_FALSE(rows.empty());

    auto base_key = extent_row_key::decode(rows.front()->key);
    ASSERT_TRUE(base_key.has_value());
    EXPECT_EQ(base_key->base_offset(), base);
    EXPECT_EQ(rows.back()->val.last_offset(), last);
}

void verify_inclusive_extents(
  state_reader& reader,
  const model::topic_id_partition& tidp,
  std::optional<int64_t> min_offset,
  std::optional<int64_t> max_offset,
  std::vector<int64_t> expected_base_offsets) {
    SCOPED_TRACE(
      fmt::format(
        "min={}, max={}", min_offset.value_or(-1), max_offset.value_or(-1)));

    std::optional<kafka::offset> min_opt = min_offset
                                             ? std::make_optional(
                                                 kafka::offset(*min_offset))
                                             : std::nullopt;
    std::optional<kafka::offset> max_opt = max_offset
                                             ? std::make_optional(
                                                 kafka::offset(*max_offset))
                                             : std::nullopt;

    auto res = reader.get_inclusive_extents(tidp, min_opt, max_opt).get();
    ASSERT_TRUE(res.has_value());

    if (expected_base_offsets.empty()) {
        EXPECT_FALSE(res.value().has_value());
        return;
    }

    ASSERT_TRUE(res.value().has_value());
    auto rows = res.value()->materialize_rows().get();
    ASSERT_EQ(rows.size(), expected_base_offsets.size());
    for (size_t i = 0; i < expected_base_offsets.size(); ++i) {
        auto key = extent_row_key::decode(rows[i]->key);
        ASSERT_TRUE(key.has_value());
        EXPECT_EQ(key->base_offset, kafka::offset(expected_base_offsets[i]));
    }
}

} // namespace

TEST_F(StateReaderTestFixture, TestGetExtentRangeSurroundedPartitions) {
    auto tidp0 = make_tidp(0);
    auto tidp1 = make_tidp(1);
    auto oid = make_oid();
    write_extent(
      tidp0,
      kafka::offset(100),
      kafka::offset(199),
      model::timestamp(1000),
      0,
      1024,
      oid);
    write_extent(
      tidp0,
      kafka::offset(200),
      kafka::offset(299),
      model::timestamp(2000),
      1024,
      1024,
      oid);

    auto reader = make_reader();

    // Valid, exact ranges.
    validate_extent_range(reader, tidp0, 100, 199, 1);
    validate_extent_range(reader, tidp0, 200, 299, 1);
    validate_extent_range(reader, tidp0, 100, 299, 2);

    // Non-exact ranges.
    validate_extent_range(reader, tidp0, 99, 100, 0);
    validate_extent_range(reader, tidp0, 99, 99, 0);
    validate_extent_range(reader, tidp0, 100, 100, 0);
    validate_extent_range(reader, tidp0, 100, 200, 0);
    validate_extent_range(reader, tidp0, 199, 199, 0);
    validate_extent_range(reader, tidp0, 200, 200, 0);
    validate_extent_range(reader, tidp0, 200, 300, 0);
    validate_extent_range(reader, tidp0, 300, 300, 0);

    // Wrong partition.
    validate_extent_range(reader, tidp1, 100, 199, 0);
    validate_extent_range(reader, tidp1, 200, 299, 0);
    validate_extent_range(reader, tidp1, 100, 299, 0);
}

TEST_F(StateReaderTestFixture, TestGetExtentRange) {
    auto tidp0 = make_tidp(0);
    auto tidp1 = make_tidp(1);
    auto tidp2 = make_tidp(2);
    auto oid = make_oid();

    // We'll query tidp1, so sandwich its extents with tidp0 and tidp2.
    write_extent(
      tidp0,
      kafka::offset(0),
      kafka::offset(199),
      model::timestamp(1000),
      0,
      1024,
      oid);
    write_extent(
      tidp1,
      kafka::offset(100),
      kafka::offset(199),
      model::timestamp(1000),
      0,
      1024,
      oid);
    write_extent(
      tidp1,
      kafka::offset(200),
      kafka::offset(299),
      model::timestamp(2000),
      1024,
      1024,
      oid);
    write_extent(
      tidp2,
      kafka::offset(0),
      kafka::offset(199),
      model::timestamp(1000),
      0,
      1024,
      oid);
    auto reader = make_reader();

    // Valid, exact ranges.
    validate_extent_range(reader, tidp1, 100, 199, 1);
    validate_extent_range(reader, tidp1, 200, 299, 1);
    validate_extent_range(reader, tidp1, 100, 299, 2);

    // Non-exact ranges.
    validate_extent_range(reader, tidp1, 99, 100, 0);
    validate_extent_range(reader, tidp1, 99, 99, 0);
    validate_extent_range(reader, tidp1, 100, 100, 0);
    validate_extent_range(reader, tidp1, 100, 200, 0);
    validate_extent_range(reader, tidp1, 199, 199, 0);
    validate_extent_range(reader, tidp1, 200, 200, 0);
    validate_extent_range(reader, tidp1, 200, 300, 0);
    validate_extent_range(reader, tidp1, 300, 300, 0);
}

TEST_F(StateReaderTestFixture, TestGetExtentsForward) {
    auto tidp = make_tidp(0);
    auto oid = make_oid();

    // Create 3 extents: [100-199], [200-299], [300-399]
    write_extent(
      tidp,
      kafka::offset(100),
      kafka::offset(199),
      model::timestamp(1000),
      0,
      1024,
      oid);
    write_extent(
      tidp,
      kafka::offset(200),
      kafka::offset(299),
      model::timestamp(2000),
      1024,
      1024,
      oid);
    write_extent(
      tidp,
      kafka::offset(300),
      kafka::offset(399),
      model::timestamp(3000),
      2048,
      1024,
      oid);

    auto reader = make_reader();

    // Exact matches.
    verify_inclusive_extents(reader, tidp, 100, 399, {100, 200, 300});
    verify_inclusive_extents(reader, tidp, 200, 399, {200, 300});
    verify_inclusive_extents(reader, tidp, 100, 299, {100, 200});
    verify_inclusive_extents(reader, tidp, 100, 199, {100});
    verify_inclusive_extents(reader, tidp, 200, 299, {200});
    verify_inclusive_extents(reader, tidp, 300, 399, {300});

    // Imprecise boundaries.
    verify_inclusive_extents(reader, tidp, 150, 350, {100, 200, 300});
    verify_inclusive_extents(reader, tidp, 99, 399, {100, 200, 300});
    verify_inclusive_extents(reader, tidp, 100, 400, {100, 200, 300});
    verify_inclusive_extents(reader, tidp, 99, 400, {100, 200, 300});
    verify_inclusive_extents(reader, tidp, 0, 1000, {100, 200, 300});
    verify_inclusive_extents(reader, tidp, 99, 199, {100});
    verify_inclusive_extents(reader, tidp, 300, 400, {300});
    verify_inclusive_extents(reader, tidp, 0, 99, {});
    verify_inclusive_extents(reader, tidp, 400, 500, {});

    // Null boundaries.
    verify_inclusive_extents(reader, tidp, std::nullopt, 99, {});
    verify_inclusive_extents(reader, tidp, std::nullopt, 100, {100});
    verify_inclusive_extents(reader, tidp, std::nullopt, 199, {100});
    verify_inclusive_extents(reader, tidp, std::nullopt, 200, {100, 200});
    verify_inclusive_extents(reader, tidp, std::nullopt, 299, {100, 200});
    verify_inclusive_extents(reader, tidp, std::nullopt, 300, {100, 200, 300});
    verify_inclusive_extents(reader, tidp, std::nullopt, 399, {100, 200, 300});
    verify_inclusive_extents(reader, tidp, std::nullopt, 400, {100, 200, 300});
    verify_inclusive_extents(reader, tidp, 99, std::nullopt, {100, 200, 300});
    verify_inclusive_extents(reader, tidp, 100, std::nullopt, {100, 200, 300});
    verify_inclusive_extents(reader, tidp, 199, std::nullopt, {100, 200, 300});
    verify_inclusive_extents(reader, tidp, 200, std::nullopt, {200, 300});
    verify_inclusive_extents(reader, tidp, 299, std::nullopt, {200, 300});
    verify_inclusive_extents(reader, tidp, 300, std::nullopt, {300});
    verify_inclusive_extents(reader, tidp, 399, std::nullopt, {300});
    verify_inclusive_extents(reader, tidp, 400, std::nullopt, {});

    // Both bounds special cases
    verify_inclusive_extents(
      reader, tidp, std::nullopt, std::nullopt, {100, 200, 300});
    verify_inclusive_extents(reader, tidp, 199, 200, {100, 200});
    verify_inclusive_extents(reader, tidp, 200, 200, {200});
    verify_inclusive_extents(reader, tidp, 300, 100, {});
}

TEST_F(StateReaderTestFixture, TestGetExtentsSizeOne) {
    auto tidp = make_tidp(0);
    auto oid = make_oid();

    // Create 3 extents with size 1: [100-100], [101-101], [102-102]
    write_extent(
      tidp,
      kafka::offset(100),
      kafka::offset(100),
      model::timestamp(1000),
      0,
      1024,
      oid);
    write_extent(
      tidp,
      kafka::offset(101),
      kafka::offset(101),
      model::timestamp(2000),
      1024,
      1024,
      oid);
    write_extent(
      tidp,
      kafka::offset(102),
      kafka::offset(102),
      model::timestamp(3000),
      2048,
      1024,
      oid);

    auto reader = make_reader();

    // Exact matches.
    verify_inclusive_extents(reader, tidp, 100, 102, {100, 101, 102});
    verify_inclusive_extents(reader, tidp, 101, 102, {101, 102});
    verify_inclusive_extents(reader, tidp, 100, 101, {100, 101});
    verify_inclusive_extents(reader, tidp, 100, 100, {100});
    verify_inclusive_extents(reader, tidp, 101, 101, {101});
    verify_inclusive_extents(reader, tidp, 102, 102, {102});

    // Imprecise boundaries.
    verify_inclusive_extents(reader, tidp, 99, 102, {100, 101, 102});
    verify_inclusive_extents(reader, tidp, 100, 103, {100, 101, 102});
    verify_inclusive_extents(reader, tidp, 99, 103, {100, 101, 102});
    verify_inclusive_extents(reader, tidp, 0, 1000, {100, 101, 102});
    verify_inclusive_extents(reader, tidp, 99, 100, {100});
    verify_inclusive_extents(reader, tidp, 102, 103, {102});
    verify_inclusive_extents(reader, tidp, 99, 99, {});
    verify_inclusive_extents(reader, tidp, 103, 103, {});

    // Null boundaries.
    verify_inclusive_extents(reader, tidp, std::nullopt, 99, {});
    verify_inclusive_extents(reader, tidp, std::nullopt, 100, {100});
    verify_inclusive_extents(reader, tidp, std::nullopt, 101, {100, 101});
    verify_inclusive_extents(reader, tidp, std::nullopt, 102, {100, 101, 102});
    verify_inclusive_extents(reader, tidp, std::nullopt, 103, {100, 101, 102});
    verify_inclusive_extents(
      reader, tidp, std::nullopt, std::nullopt, {100, 101, 102});
    verify_inclusive_extents(reader, tidp, 101, std::nullopt, {101, 102});
    verify_inclusive_extents(reader, tidp, 102, std::nullopt, {102});
    verify_inclusive_extents(reader, tidp, 103, std::nullopt, {});
    verify_inclusive_extents(reader, tidp, 103, std::nullopt, {});
}

TEST_F(StateReaderTestFixture, TestGetExtentsBackward) {
    auto tidp = make_tidp(0);
    auto oid = make_oid();

    // Create 3 extents: [100-199], [200-299], [300-399]
    write_extent(
      tidp,
      kafka::offset(100),
      kafka::offset(199),
      model::timestamp(1000),
      0,
      1024,
      oid);
    write_extent(
      tidp,
      kafka::offset(200),
      kafka::offset(299),
      model::timestamp(2000),
      1024,
      1024,
      oid);
    write_extent(
      tidp,
      kafka::offset(300),
      kafka::offset(399),
      model::timestamp(3000),
      2048,
      1024,
      oid);

    auto reader = make_reader();

    // Query range [150, 350] should return all 3 extents in reverse order
    {
        auto res = reader
                     .get_inclusive_extents_backward(
                       tidp, kafka::offset(150), kafka::offset(350))
                     .get();
        ASSERT_TRUE(res.has_value());
        ASSERT_TRUE(res.value().has_value());
        auto rows = res.value()->materialize_rows().get();
        ASSERT_EQ(rows.size(), 3);
        // Backward order: highest base_offset first
        EXPECT_EQ(rows[0]->val.last_offset, kafka::offset(399));
        EXPECT_EQ(rows[1]->val.last_offset, kafka::offset(299));
        EXPECT_EQ(rows[2]->val.last_offset, kafka::offset(199));
    }

    // Query range [200, 299] should return only middle extent
    {
        auto res = reader
                     .get_inclusive_extents_backward(
                       tidp, kafka::offset(200), kafka::offset(299))
                     .get();
        ASSERT_TRUE(res.has_value());
        ASSERT_TRUE(res.value().has_value());
        auto rows = res.value()->materialize_rows().get();
        ASSERT_EQ(rows.size(), 1);
        EXPECT_EQ(rows[0]->val.last_offset, kafka::offset(299));
    }

    // Query range [0, 99] should return no extents (before all data)
    {
        auto res = reader
                     .get_inclusive_extents_backward(
                       tidp, kafka::offset(0), kafka::offset(99))
                     .get();
        ASSERT_TRUE(res.has_value());
        EXPECT_FALSE(res.value().has_value());
    }
}

namespace {

void verify_term_of(
  state_reader& reader,
  const model::topic_id_partition& tidp,
  kafka::offset o,
  std::optional<model::term_id> expected_term) {
    SCOPED_TRACE(fmt::format("tidp={}, o={}", tidp, o));
    auto res = reader.get_term_le(tidp, o).get();
    ASSERT_TRUE(res.has_value());
    if (expected_term.has_value()) {
        ASSERT_TRUE(res.value().has_value());
        EXPECT_EQ(res.value()->term_id, expected_term.value());
    } else {
        EXPECT_FALSE(res.value().has_value());
    }
}

void verify_term_end(
  state_reader& reader,
  const model::topic_id_partition& tidp,
  model::term_id t,
  std::optional<kafka::offset> expected_end) {
    SCOPED_TRACE(fmt::format("tidp={}, term={}", tidp, t));
    auto res = reader.get_term_end(tidp, t).get();
    ASSERT_TRUE(res.has_value());
    if (expected_end.has_value()) {
        ASSERT_TRUE(res.value().has_value());
        EXPECT_EQ(res.value().value(), expected_end.value());
    } else {
        EXPECT_FALSE(res.value().has_value());
    }
}

void verify_term_keys(
  state_reader& reader,
  const model::topic_id_partition& tidp,
  std::optional<kafka::offset> upper_bound,
  size_t expected_count) {
    SCOPED_TRACE(
      fmt::format(
        "tidp={}, upper_bound={}",
        tidp,
        upper_bound.has_value() ? fmt::format("{}", *upper_bound) : "nullopt"));
    auto res = reader.get_term_keys(tidp, upper_bound).get();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->size(), expected_count);
}

} // namespace

TEST_F(StateReaderTestFixture, TestGetTermLe) {
    auto tidp = make_tidp(0);
    write_term_start(tidp, model::term_id(1), kafka::offset(10));
    write_term_start(tidp, model::term_id(3), kafka::offset(100));
    write_term_start(tidp, model::term_id(7), kafka::offset(250));

    auto reader = make_reader();

    // Offsets below which we have term start return nullopt.
    verify_term_of(reader, tidp, kafka::offset(0), std::nullopt);
    verify_term_of(reader, tidp, kafka::offset(9), std::nullopt);

    // Offsets before term 3 should be in term 1.
    verify_term_of(reader, tidp, kafka::offset(10), model::term_id(1));
    verify_term_of(reader, tidp, kafka::offset(99), model::term_id(1));

    // Offsets before term 7 should be in term 3.
    verify_term_of(reader, tidp, kafka::offset(100), model::term_id(3));
    verify_term_of(reader, tidp, kafka::offset(249), model::term_id(3));

    // Offsets at or after term 7 should be in term 7.
    verify_term_of(reader, tidp, kafka::offset(250), model::term_id(7));
    verify_term_of(reader, tidp, kafka::offset(1000), model::term_id(7));

    // Missing partition should return nullopt.
    auto missing_tidp = make_tidp(1);
    verify_term_of(reader, missing_tidp, kafka::offset(100), std::nullopt);
}

TEST_F(StateReaderTestFixture, TestGetTermEnd) {
    auto tidp = make_tidp(0);

    write_term_start(tidp, model::term_id(1), kafka::offset(0));
    write_term_start(tidp, model::term_id(3), kafka::offset(100));
    write_term_start(tidp, model::term_id(7), kafka::offset(250));
    write_metadata(tidp, kafka::offset(0), kafka::offset(400));

    auto reader = make_reader();
    verify_term_end(reader, tidp, model::term_id(0), kafka::offset(0));
    verify_term_end(reader, tidp, model::term_id(1), kafka::offset(100));
    verify_term_end(reader, tidp, model::term_id(2), kafka::offset(100));
    verify_term_end(reader, tidp, model::term_id(3), kafka::offset(250));
    verify_term_end(reader, tidp, model::term_id(4), kafka::offset(250));
    verify_term_end(reader, tidp, model::term_id(5), kafka::offset(250));
    verify_term_end(reader, tidp, model::term_id(6), kafka::offset(250));
    verify_term_end(reader, tidp, model::term_id(7), kafka::offset(400));
    verify_term_end(reader, tidp, model::term_id(8), std::nullopt);

    // Missing partition should return nullopt.
    auto missing_tidp = make_tidp(1);
    verify_term_end(reader, missing_tidp, model::term_id(1), std::nullopt);
}

TEST_F(StateReaderTestFixture, TestGetTermKeys) {
    auto tidp = make_tidp(0);
    write_term_start(tidp, model::term_id(1), kafka::offset(0));
    write_term_start(tidp, model::term_id(3), kafka::offset(50));
    write_term_start(tidp, model::term_id(7), kafka::offset(100));
    auto reader = make_reader();

    // No upper bound, grab all the terms.
    verify_term_keys(reader, tidp, std::nullopt, 3);

    // With upper_bound below first term, returns empty.
    verify_term_keys(reader, tidp, kafka::offset(-1), 0);

    // With upper_bound between first and second term, returns first term only.
    verify_term_keys(reader, tidp, kafka::offset(0), 1);
    verify_term_keys(reader, tidp, kafka::offset(1), 1);
    verify_term_keys(reader, tidp, kafka::offset(49), 1);

    // With upper_bound at second term's start_offset, returns first two.
    verify_term_keys(reader, tidp, kafka::offset(50), 2);
    verify_term_keys(reader, tidp, kafka::offset(51), 2);
    verify_term_keys(reader, tidp, kafka::offset(99), 2);

    // With upper_bound at or beyond third term, returns all three.
    verify_term_keys(reader, tidp, kafka::offset(100), 3);
    verify_term_keys(reader, tidp, kafka::offset(1000), 3);

    // Missing partition returns empty.
    auto missing_tidp = make_tidp(1);
    verify_term_keys(reader, missing_tidp, kafka::offset(0), 0);
    verify_term_keys(reader, missing_tidp, kafka::offset(100), 0);
    verify_term_keys(reader, missing_tidp, std::nullopt, 0);
}

namespace {

ss::future<> hydrate_object_rows(
  ss::coroutine::experimental::generator<
    std::expected<state_reader::object_row, state_reader::error>> gen,
  chunked_vector<std::pair<object_id, object_entry>>& out) {
    while (auto row_opt = co_await gen()) {
        auto& row_res = row_opt->get();
        RPTEST_REQUIRE_CORO(row_res.has_value()) << row_res.error();
        const auto& row = row_res.value();
        auto key = object_row_key::decode(row.key);
        RPTEST_REQUIRE_CORO(key.has_value());
        out.emplace_back(key->oid, row.val.object);
    }
}

MATCHER_P2(ObjectIdAndSizeIs, expected_oid, expected_size, "") {
    return arg.first == expected_oid
           && arg.second.object_size == static_cast<size_t>(expected_size);
}

} // namespace

TEST_F(StateReaderTestFixture, TestObjectKeyRangeNoLowerBound) {
    auto oid1 = object_id(uuid_t::create());
    auto oid2 = object_id(uuid_t::create());
    auto oid3 = object_id(uuid_t::create());

    write_object(oid1, make_object_entry(1024));
    write_object(oid2, make_object_entry(2048));
    write_object(oid3, make_object_entry(4096));

    auto reader = make_reader();
    auto range_res = reader.get_object_range(std::nullopt).get();
    ASSERT_TRUE(range_res.has_value());

    chunked_vector<std::pair<object_id, object_entry>> rows;
    hydrate_object_rows(range_res->get_rows(), rows).get();
    EXPECT_THAT(
      rows,
      testing::UnorderedElementsAre(
        ObjectIdAndSizeIs(oid1, 1024),
        ObjectIdAndSizeIs(oid2, 2048),
        ObjectIdAndSizeIs(oid3, 4096)));
}

TEST_F(StateReaderTestFixture, TestObjectKeyRangeWithLowerBound) {
    auto oid1 = object_id(
      uuid_t::from_string("00000000-0000-0000-0000-000000000001"));
    auto oid2 = object_id(
      uuid_t::from_string("00000000-0000-0000-0000-000000000002"));
    auto oid3 = object_id(
      uuid_t::from_string("00000000-0000-0000-0000-000000000003"));
    auto missing_oid = object_id(
      uuid_t::from_string("00000000-0000-0000-0000-000000000004"));

    write_object(oid1, make_object_entry(1024));
    write_object(oid2, make_object_entry(2048));
    write_object(oid3, make_object_entry(4096));

    {
        auto reader = make_reader();
        auto range_res = reader.get_object_range(oid1).get();
        ASSERT_TRUE(range_res.has_value());
        chunked_vector<std::pair<object_id, object_entry>> rows;
        hydrate_object_rows(range_res->get_rows(), rows).get();
        EXPECT_THAT(
          rows,
          testing::UnorderedElementsAre(
            ObjectIdAndSizeIs(oid1, 1024),
            ObjectIdAndSizeIs(oid2, 2048),
            ObjectIdAndSizeIs(oid3, 4096)));
    }
    {
        auto reader = make_reader();
        auto range_res = reader.get_object_range(oid2).get();
        ASSERT_TRUE(range_res.has_value());
        chunked_vector<std::pair<object_id, object_entry>> rows;
        hydrate_object_rows(range_res->get_rows(), rows).get();
        EXPECT_THAT(
          rows,
          testing::ElementsAre(
            ObjectIdAndSizeIs(oid2, 2048), ObjectIdAndSizeIs(oid3, 4096)));
    }
    {
        auto reader = make_reader();
        auto range_res = reader.get_object_range(missing_oid).get();
        ASSERT_TRUE(range_res.has_value());
        chunked_vector<std::pair<object_id, object_entry>> rows;
        hydrate_object_rows(range_res->get_rows(), rows).get();
        EXPECT_THAT(rows, testing::ElementsAre());
    }
}

TEST_F(StateReaderTestFixture, GetAllTermsEmpty) {
    auto reader = make_reader();
    auto res = reader.get_all_terms(make_tidp()).get();
    ASSERT_TRUE(res.has_value());
    EXPECT_TRUE(res->empty());
}

TEST_F(StateReaderTestFixture, GetAllTermsOrdered) {
    auto tidp = make_tidp();
    write_term_start(tidp, model::term_id{1}, kafka::offset{0});
    write_term_start(tidp, model::term_id{3}, kafka::offset{100});
    write_term_start(tidp, model::term_id{5}, kafka::offset{200});

    auto reader = make_reader();
    auto res = reader.get_all_terms(tidp).get();
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res->size(), 3u);
    EXPECT_EQ((*res)[0].term_id, model::term_id{1});
    EXPECT_EQ((*res)[0].start_offset, kafka::offset{0});
    EXPECT_EQ((*res)[1].term_id, model::term_id{3});
    EXPECT_EQ((*res)[1].start_offset, kafka::offset{100});
    EXPECT_EQ((*res)[2].term_id, model::term_id{5});
    EXPECT_EQ((*res)[2].start_offset, kafka::offset{200});
}

TEST_F(StateReaderTestFixture, GetAllTermsIsolatedByPartition) {
    auto tidp0 = make_tidp(0);
    auto tidp1 = make_tidp(1);
    write_term_start(tidp0, model::term_id{1}, kafka::offset{0});
    write_term_start(tidp1, model::term_id{1}, kafka::offset{0});
    write_term_start(tidp1, model::term_id{2}, kafka::offset{50});

    auto reader = make_reader();
    auto res0 = reader.get_all_terms(tidp0).get();
    ASSERT_TRUE(res0.has_value());
    EXPECT_EQ(res0->size(), 1u);

    auto res1 = reader.get_all_terms(tidp1).get();
    ASSERT_TRUE(res1.has_value());
    EXPECT_EQ(res1->size(), 2u);
}
