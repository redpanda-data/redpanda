/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/metastore/lsm/lsm_update.h"
#include "cloud_topics/level_one/metastore/lsm/state.h"
#include "cloud_topics/level_one/metastore/lsm/write_batch_row.h"
#include "serde/async.h"

#include <gtest/gtest.h>

using namespace cloud_topics::l1;

// Verify that lsm_state and lsm_stm_snapshot use async serde writes to avoid
// reactor stalls when serializing a large volatile_buffer.
static_assert(serde::has_serde_async_write<lsm_state>);
static_assert(serde::has_serde_async_write<lsm_stm_snapshot>);

namespace {

// Helper to create a test domain UUID.
domain_uuid new_domain_uuid() { return domain_uuid(uuid_t::create()); }

// Helper to create a serialized manifest with given epoch and seqno.
lsm_state::serialized_manifest
create_test_manifest(uint64_t epoch, lsm::sequence_number seqno) {
    return lsm_state::serialized_manifest{
      .buf = iobuf::from("testbuf"),
      .last_seqno = seqno,
      .database_epoch = lsm::internal::database_epoch(epoch),
    };
}

// Helper to create test rows.
chunked_vector<write_batch_row> create_rows(size_t count) {
    chunked_vector<write_batch_row> rows;
    for (size_t i = 0; i < count; ++i) {
        rows.emplace_back(
          write_batch_row{
            .key = fmt::format("key{}", i),
            .value = iobuf::from(fmt::format("value{}", i)),
          });
    }
    return rows;
}

} // namespace

TEST(ApplyWriteBatchUpdateTest, TestApplyWriteBatchHappyPath) {
    lsm_state state;
    auto uuid = new_domain_uuid();
    state.domain_uuid = uuid;

    // Create some rows and apply them.
    auto rows = create_rows(3);
    auto update_res = apply_write_batch_update::build(
      state, uuid, std::move(rows));
    ASSERT_TRUE(update_res.has_value());

    auto& update = update_res.value();
    EXPECT_EQ(update.expected_uuid, uuid);
    EXPECT_EQ(update.rows.size(), 3);

    auto apply_res = update.apply(state, model::offset(10));
    ASSERT_TRUE(apply_res.has_value());

    // Verify rows were added to volatile buffer with correct sequence numbers.
    EXPECT_EQ(state.volatile_buffer.size(), 3);
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_EQ(state.volatile_buffer[i].seqno, lsm::sequence_number(10));
        EXPECT_EQ(state.volatile_buffer[i].row.key, fmt::format("key{}", i));
    }
}

TEST(ApplyWriteBatchUpdateTest, TestApplyWriteBatchDomainUuidMismatch) {
    lsm_state state;
    auto uuid1 = new_domain_uuid();
    auto uuid2 = new_domain_uuid();
    state.domain_uuid = uuid1;

    auto rows = create_rows(1);
    // Try to build with wrong UUID.
    auto update_res = apply_write_batch_update::build(
      state, uuid2, std::move(rows));
    EXPECT_FALSE(update_res.has_value());
    EXPECT_NE(
      update_res.error()().find("Expected domain UUID"), ss::sstring::npos);
}

TEST(ApplyWriteBatchUpdateTest, TestApplyWriteBatchEmptyRows) {
    lsm_state state;
    auto uuid = new_domain_uuid();
    state.domain_uuid = uuid;

    chunked_vector<write_batch_row> empty_rows;
    auto update_res = apply_write_batch_update::build(
      state, uuid, std::move(empty_rows));
    EXPECT_FALSE(update_res.has_value());
    EXPECT_NE(
      update_res.error()().find("Write batch is empty"), ss::sstring::npos);
}

TEST(PersistManifestUpdateTest, TestPersistManifestHappyPath) {
    lsm_state state;
    auto uuid = new_domain_uuid();
    state.domain_uuid = uuid;

    // Add some volatile rows.
    state.volatile_buffer.push_back(
      volatile_row{
        .seqno = lsm::sequence_number(5),
        .row = write_batch_row{.key = "key1", .value = iobuf::from("value1")},
      });
    state.volatile_buffer.push_back(
      volatile_row{
        .seqno = lsm::sequence_number(10),
        .row = write_batch_row{.key = "key2", .value = iobuf::from("value2")},
      });
    state.volatile_buffer.push_back(
      volatile_row{
        .seqno = lsm::sequence_number(15),
        .row = write_batch_row{.key = "key3", .value = iobuf::from("value3")},
      });

    // Persist manifest up to seqno 10.
    auto manifest = create_test_manifest(1, lsm::sequence_number(10));
    auto update_res = persist_manifest_update::build(
      state, uuid, std::move(manifest));
    ASSERT_TRUE(update_res.has_value());

    auto apply_res = update_res.value().apply(state);
    ASSERT_TRUE(apply_res.has_value());

    // Volatile buffer should be trimmed up to seqno 10.
    EXPECT_EQ(state.volatile_buffer.size(), 1);
    EXPECT_EQ(state.volatile_buffer.front().seqno, lsm::sequence_number(15));
    ASSERT_TRUE(state.persisted_manifest.has_value());
    EXPECT_EQ(state.persisted_manifest->get_last_seqno()(), 10);
}

TEST(PersistManifestUpdateTest, TestPersistManifestDomainUuidMismatch) {
    lsm_state state;
    auto uuid1 = new_domain_uuid();
    auto uuid2 = new_domain_uuid();
    state.domain_uuid = uuid1;

    auto manifest = create_test_manifest(1, lsm::sequence_number(100));
    auto update_res = persist_manifest_update::build(
      state, uuid2, std::move(manifest));
    EXPECT_FALSE(update_res.has_value());
    EXPECT_NE(
      update_res.error()().find("Expected domain UUID"), ss::sstring::npos);
}

TEST(PersistManifestUpdateTest, TestPersistManifestSeqnoNotMonotonic) {
    lsm_state state;
    auto uuid = new_domain_uuid();
    state.domain_uuid = uuid;

    // Set existing manifest with seqno 100.
    state.persisted_manifest = create_test_manifest(
      1, lsm::sequence_number(100));

    // Try to persist manifest with lower seqno.
    {
        auto manifest = create_test_manifest(1, lsm::sequence_number(99));
        auto update_res = persist_manifest_update::build(
          state, uuid, std::move(manifest));
        EXPECT_FALSE(update_res.has_value());
        EXPECT_NE(
          update_res.error()().find("below current seqno"), ss::sstring::npos);
    }
    // At the same seqno is allowed though (e.g. for compaction).
    {
        auto manifest = create_test_manifest(1, lsm::sequence_number(100));
        auto update_res = persist_manifest_update::build(
          state, uuid, std::move(manifest));
        EXPECT_TRUE(update_res.has_value());
    }
}

TEST(PersistManifestUpdateTest, TestPersistManifestEpochNotMonotonic) {
    lsm_state state;
    auto uuid = new_domain_uuid();
    state.domain_uuid = uuid;

    // Set existing manifest with a specified epoch.
    state.persisted_manifest = create_test_manifest(
      4, lsm::sequence_number(100));

    // Try to persist manifest with lower epoch.
    {
        auto manifest = create_test_manifest(3, lsm::sequence_number(100));
        auto update_res = persist_manifest_update::build(
          state, uuid, std::move(manifest));
        EXPECT_FALSE(update_res.has_value());
        EXPECT_NE(
          update_res.error()().find("below current epoch"), ss::sstring::npos);
    }
    // At the same epoch is allowed though.
    {
        auto manifest = create_test_manifest(4, lsm::sequence_number(100));
        auto update_res = persist_manifest_update::build(
          state, uuid, std::move(manifest));
        EXPECT_TRUE(update_res.has_value());
    }
}

TEST(SetDomainUuidUpdateTest, TestSetDomainUuidHappyPath) {
    lsm_state state;
    auto uuid = new_domain_uuid();
    auto update_res = set_domain_uuid_update::build(state, uuid);
    ASSERT_TRUE(update_res.has_value());

    auto apply_res = update_res.value().apply(state);
    ASSERT_TRUE(apply_res.has_value());

    EXPECT_EQ(state.domain_uuid, uuid);
}

TEST(SetDomainUuidUpdateTest, TestSetDomainUuidAlreadySet) {
    lsm_state state;
    auto uuid1 = new_domain_uuid();
    auto uuid2 = new_domain_uuid();
    state.domain_uuid = uuid1;

    auto update_res = set_domain_uuid_update::build(state, uuid2);
    EXPECT_FALSE(update_res.has_value());
    EXPECT_NE(update_res.error()().find("already set"), ss::sstring::npos);
}

TEST(ResetManifestUpdateTest, TestResetManifestHappyPath) {
    lsm_state state;
    auto old_uuid = new_domain_uuid();
    state.domain_uuid = old_uuid;

    auto new_uuid = new_domain_uuid();
    auto manifest = create_test_manifest(5, lsm::sequence_number(1000));
    auto update_res = reset_manifest_update::build(
      state, new_uuid, std::move(manifest));
    ASSERT_TRUE(update_res.has_value());

    // Apply the reset at a different term and offset.
    model::term_id log_term(3);
    model::offset log_offset(50);
    auto apply_res = update_res.value().apply(state, log_term, log_offset);
    ASSERT_TRUE(apply_res.has_value());

    EXPECT_EQ(state.domain_uuid, new_uuid);
    ASSERT_TRUE(state.persisted_manifest.has_value());
    EXPECT_EQ(state.persisted_manifest->get_last_seqno()(), 1000);
    EXPECT_EQ(state.persisted_manifest->get_database_epoch()(), 5);

    // Deltas should be computed correctly
    EXPECT_EQ(state.seqno_delta, 1000 - 50);
    EXPECT_EQ(state.db_epoch_delta, 5 - 3);

    // Add some rows.
    auto rows = create_rows(1);
    auto write_update_res = apply_write_batch_update::build(
      state, new_uuid, std::move(rows));
    ASSERT_TRUE(write_update_res.has_value());
    auto write_apply_res = write_update_res.value().apply(
      state, model::next_offset(log_offset));
    ASSERT_TRUE(write_apply_res.has_value());

    // Since we applied the write at the next offset, the applied sequence
    // number should be one above the restored.
    ASSERT_EQ(1, state.volatile_buffer.size());
    EXPECT_EQ(state.volatile_buffer[0].seqno, lsm::sequence_number(1001));
}

TEST(ResetManifestUpdateTest, TestResetManifestEmptyManifest) {
    lsm_state state;
    auto old_uuid = new_domain_uuid();
    state.domain_uuid = old_uuid;

    auto new_uuid = new_domain_uuid();
    auto update_res = reset_manifest_update::build(
      state, new_uuid, std::nullopt);
    ASSERT_TRUE(update_res.has_value());

    model::term_id log_term(2);
    model::offset log_offset(10);
    auto apply_res = update_res.value().apply(state, log_term, log_offset);
    ASSERT_TRUE(apply_res.has_value());

    // UUID should be updated
    EXPECT_EQ(state.domain_uuid, new_uuid);

    // No manifest should be set
    EXPECT_FALSE(state.persisted_manifest.has_value());

    // Deltas should be computed with seqno and epoch = 0
    EXPECT_EQ(state.seqno_delta, -10);
    EXPECT_EQ(state.db_epoch_delta, -2);

    // Add some rows.
    auto rows = create_rows(1);
    auto write_update_res = apply_write_batch_update::build(
      state, new_uuid, std::move(rows));
    ASSERT_TRUE(write_update_res.has_value());
    auto write_apply_res = write_update_res.value().apply(
      state, model::next_offset(log_offset));
    ASSERT_TRUE(write_apply_res.has_value());

    // Since we applied the write at the next offset, the applied sequence
    // number should be one above the restored.
    ASSERT_EQ(1, state.volatile_buffer.size());
    EXPECT_EQ(state.volatile_buffer[0].seqno, lsm::sequence_number(1));
}

TEST(ResetManifestUpdateTest, TestResetManifestVolatileBufferNotEmpty) {
    lsm_state state;
    auto uuid = new_domain_uuid();
    state.domain_uuid = uuid;

    // Add volatile rows
    state.volatile_buffer.push_back(
      volatile_row{
        .seqno = lsm::sequence_number(1),
        .row = write_batch_row{.key = "key1", .value = iobuf::from("value1")},
      });

    auto new_uuid = new_domain_uuid();
    auto manifest = create_test_manifest(1, lsm::sequence_number(100));
    auto update_res = reset_manifest_update::build(
      state, new_uuid, std::move(manifest));
    EXPECT_FALSE(update_res.has_value());
    EXPECT_NE(update_res.error()().find("already has"), ss::sstring::npos);
    EXPECT_NE(update_res.error()().find("rows"), ss::sstring::npos);
}

TEST(ResetManifestUpdateTest, TestResetManifestPersistedDataExists) {
    lsm_state state;
    auto uuid = new_domain_uuid();
    state.domain_uuid = uuid;

    // Set existing persisted manifest with seqno > 0
    state.persisted_manifest = create_test_manifest(
      2, lsm::sequence_number(500));

    auto new_uuid = new_domain_uuid();
    auto manifest = create_test_manifest(1, lsm::sequence_number(100));
    auto update_res = reset_manifest_update::build(
      state, new_uuid, std::move(manifest));
    EXPECT_FALSE(update_res.has_value());
    EXPECT_NE(
      update_res.error()().find("already has persisted manifest"),
      ss::sstring::npos);
}
