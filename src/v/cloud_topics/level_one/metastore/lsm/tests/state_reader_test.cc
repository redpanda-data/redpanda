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
