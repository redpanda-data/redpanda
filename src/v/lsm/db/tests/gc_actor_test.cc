// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "base/seastarx.h"
#include "lsm/core/internal/files.h"
#include "lsm/core/internal/options.h"
#include "lsm/db/gc_actor.h"
#include "lsm/db/table_cache.h"
#include "lsm/io/memory_persistence.h"
#include "lsm/sst/block_cache.h"
#include "lsm/sst/builder.h"
#include "test_utils/async.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

using lsm::internal::operator""_db_epoch;
using lsm::internal::operator""_file_id;

class GcActorTest : public testing::Test {
public:
    lsm::internal::file_handle make_sst(
      lsm::internal::file_id id,
      lsm::internal::database_epoch epoch = 0_db_epoch) {
        auto fh = lsm::internal::file_handle{.id = id, .epoch = epoch};
        auto file = _persistence->open_sequential_writer(fh).get();
        lsm::sst::builder builder(std::move(file), {});
        builder.finish().get();
        builder.close().get();
        return fh;
    }

    ss::future<std::vector<lsm::internal::file_handle>> list_files_async() {
        std::vector<lsm::internal::file_handle> result;
        auto gen = _persistence->list_files();
        while (auto fh = co_await gen()) {
            result.push_back(fh->get());
        }
        co_return result;
    }

    std::vector<lsm::internal::file_handle> list_files() {
        return list_files_async().get();
    }

    void start_gc() {
        _table_cache.emplace(
          _persistence.get(),
          10,
          ss::make_lw_shared<lsm::probe>(),
          ss::make_lw_shared<lsm::sst::block_cache>(
            1_MiB, ss::make_lw_shared<lsm::probe>()));
        _gc.emplace(_persistence.get(), _opts, &*_table_cache);
        _gc->start().get();
    }

    void TearDown() override {
        if (_gc) {
            _gc->stop().get();
        }
        if (_table_cache) {
            _table_cache->close().get();
        }
        _persistence->close().get();
    }

protected:
    std::unique_ptr<lsm::io::data_persistence> _persistence
      = lsm::io::make_memory_data_persistence();
    ss::lw_shared_ptr<lsm::internal::options> _opts
      = ss::make_lw_shared<lsm::internal::options>();
    std::optional<lsm::db::table_cache> _table_cache;
    std::optional<lsm::db::gc_actor> _gc;
};

} // namespace

TEST_F(GcActorTest, LiveFilesNotDeleted) {
    auto fh1 = make_sst(1_file_id);
    auto fh2 = make_sst(2_file_id);
    auto fh3 = make_sst(3_file_id);
    ASSERT_EQ(list_files().size(), 3);

    start_gc();

    lsm::db::gc_message msg;
    msg.safe_highest_file_id = 3_file_id;
    msg.live_files.insert(fh1);
    msg.live_files.insert(fh2);
    msg.live_files.insert(fh3);
    _gc->tell(std::move(msg));
    tests::drain_task_queue().get();

    EXPECT_EQ(list_files().size(), 3);
    EXPECT_EQ(_gc->pending_delete_count(), 0);
}

TEST_F(GcActorTest, UnusedFilesDeletedImmediatelyWithZeroDelay) {
    auto fh1 = make_sst(1_file_id);
    auto fh2 = make_sst(2_file_id);
    auto fh3 = make_sst(3_file_id);
    ASSERT_EQ(list_files().size(), 3);

    _opts->file_deletion_delay = absl::ZeroDuration();
    start_gc();

    lsm::db::gc_message msg;
    msg.safe_highest_file_id = 3_file_id;
    msg.live_files.insert(fh1);
    msg.live_files.insert(fh3);
    _gc->tell(std::move(msg));
    tests::drain_task_queue().get();

    std::ignore = fh2; // deleted
    EXPECT_THAT(list_files(), testing::UnorderedElementsAre(fh1, fh3));
    EXPECT_EQ(_gc->pending_delete_count(), 0);
}

TEST_F(GcActorTest, FutureEpochFilesNotDeleted) {
    auto fh_current = make_sst(1_file_id, 0_db_epoch);
    auto fh_future = make_sst(2_file_id, 1_db_epoch);
    ASSERT_EQ(list_files().size(), 2);

    _opts->database_epoch = 0_db_epoch;
    _opts->file_deletion_delay = absl::ZeroDuration();
    start_gc();

    lsm::db::gc_message msg;
    msg.safe_highest_file_id = 2_file_id;
    _gc->tell(std::move(msg));
    tests::drain_task_queue().get();

    std::ignore = fh_current; // deleted
    EXPECT_THAT(list_files(), testing::ElementsAre(fh_future));
    EXPECT_EQ(_gc->pending_delete_count(), 0);
}

TEST_F(GcActorTest, StopTrackingExternalDeletes) {
    auto fh_1 = make_sst(1_file_id, 0_db_epoch);
    auto fh_2 = make_sst(2_file_id, 0_db_epoch);
    auto fh_3 = make_sst(3_file_id, 0_db_epoch);
    ASSERT_EQ(list_files().size(), 3);

    _opts->database_epoch = 0_db_epoch;
    // Waiting one nanosecond basically means wait until the next iteration of
    // the gc actor to delete.
    _opts->file_deletion_delay = absl::Nanoseconds(1);
    start_gc();

    lsm::db::gc_message msg;
    msg.safe_highest_file_id = 3_file_id;
    msg.live_files.insert(fh_2);
    msg.live_files.insert(fh_3);
    _gc->tell(std::move(msg));
    tests::drain_task_queue().get();

    EXPECT_THAT(list_files(), testing::ElementsAre(fh_1, fh_2, fh_3));
    EXPECT_EQ(_gc->pending_delete_count(), 1);

    // Remove file one out of band
    _persistence->remove_file(fh_1).get();

    msg = {};
    msg.safe_highest_file_id = 3_file_id;
    msg.live_files.insert(fh_3);
    _gc->tell(std::move(msg));
    tests::drain_task_queue().get();

    EXPECT_THAT(list_files(), testing::ElementsAre(fh_2, fh_3));
    // We should only have file 2 in the pending delete list
    EXPECT_EQ(_gc->pending_delete_count(), 1);

    msg = {};
    msg.safe_highest_file_id = 3_file_id;
    msg.live_files.insert(fh_3);
    _gc->tell(std::move(msg));
    tests::drain_task_queue().get();

    EXPECT_THAT(list_files(), testing::ElementsAre(fh_3));
    // Everything should be deleted now
    EXPECT_EQ(_gc->pending_delete_count(), 0);
}
