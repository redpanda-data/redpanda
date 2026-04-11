/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/tests/s3_imposter.h"
#include "cloud_io/tests/scoped_remote.h"
#include "cloud_topics/level_one/metastore/domain_uuid.h"
#include "cloud_topics/read_replica/snapshot_manager.h"
#include "cloud_topics/read_replica/snapshot_metastore.h"
#include "cloud_topics/read_replica/tests/db_utils.h"
#include "lsm/lsm.h"
#include "test_utils/tmp_dir.h"

#include <seastar/core/coroutine.hh>

#include <gtest/gtest.h>

using namespace cloud_topics;

using namespace std::chrono_literals;

namespace {

const auto domain = l1::domain_uuid(uuid_t::create());
const auto test_tidp = model::topic_id_partition{
  model::topic_id{uuid_t::create()}, model::partition_id{0}};
using o = kafka::offset;

} // namespace
class SnapshotManagerTest
  : public ::testing::Test
  , public s3_imposter_fixture {
public:
    void SetUp() override {
        set_expectations_and_listen({});
        sr_ = cloud_io::scoped_remote::create(10, conf);
        config::shard_local_cfg()
          .cloud_storage_readreplica_manifest_sync_timeout_ms.set_value(500ms);
        staging_dir_ = std::make_unique<temporary_dir>("snapshot_manager_test");

        // Writer and reader use separate staging directories - they communicate
        // only through S3.
        writer_staging_dir_ = staging_dir_->get_path() / "writer";
        reader_staging_dir_ = staging_dir_->get_path() / "reader";
        snapshot_mgr_ = std::make_unique<read_replica::snapshot_manager>(
          reader_staging_dir_, &sr_->remote.local(), nullptr);
    }

    void TearDown() override {
        for (auto& [domain, db_ptr] : writer_dbs_) {
            db_ptr->close().get();
        }
        writer_dbs_.clear();
        if (snapshot_mgr_) {
            snapshot_mgr_->stop().get();
            snapshot_mgr_.reset();
        }
        sr_.reset();
    }

    // Helper to write data to a domain's database using L1 metastore semantics
    ss::future<lsm::sequence_number>
    write_data(l1::domain_uuid domain, kafka::offset base, kafka::offset last) {
        auto* db = co_await read_replica::test_utils::get_or_create_writer_db(
          domain,
          writer_staging_dir_,
          &sr_->remote.local(),
          bucket_name,
          writer_dbs_);
        // Convert start_idx and count to kafka offsets
        co_return co_await read_replica::test_utils::write_metastore_data(
          db, test_tidp, base, last, model::term_id{1});
    }

    ss::future<> close_writer(l1::domain_uuid domain) {
        auto it = writer_dbs_.find(domain);
        if (it != writer_dbs_.end()) {
            co_await it->second->close();
            writer_dbs_.erase(it);
        }
    }

    ss::future<std::expected<
      read_replica::snapshot_handle,
      read_replica::snapshot_provider::error>>
    get_snapshot(
      l1::domain_uuid domain,
      ss::lowres_clock::time_point min_refresh_time
      = ss::lowres_clock::time_point::min(),
      std::optional<lsm::sequence_number> min_seqno = std::nullopt,
      ss::lowres_clock::duration timeout = 2s) {
        co_return co_await snapshot_mgr_->get_snapshot(
          domain, bucket_name, min_refresh_time, min_seqno, timeout);
    }

    read_replica::snapshot_handle check_get_snapshot(
      l1::domain_uuid domain,
      ss::lowres_clock::time_point min_refresh_time
      = ss::lowres_clock::time_point::min(),
      std::optional<lsm::sequence_number> min_seqno = std::nullopt,
      ss::lowres_clock::duration timeout = 2s) {
        auto res
          = get_snapshot(domain, min_refresh_time, min_seqno, timeout).get();
        EXPECT_TRUE(res.has_value()) << fmt::format("Error: {}", res.error());
        return std::move(res.value());
    }

protected:
    std::unique_ptr<temporary_dir> staging_dir_;
    std::filesystem::path writer_staging_dir_;
    std::filesystem::path reader_staging_dir_;
    std::unique_ptr<cloud_io::scoped_remote> sr_;
    std::unique_ptr<read_replica::snapshot_manager> snapshot_mgr_;
    chunked_hash_map<l1::domain_uuid, std::unique_ptr<lsm::database>>
      writer_dbs_;
};

TEST_F(SnapshotManagerTest, WaitForRefreshSeqnoNotNewEnough) {
    auto initial_seqno = write_data(domain, o{0}, o{3}).get();

    // Get initial snapshot.
    auto s1 = check_get_snapshot(domain);
    EXPECT_EQ(s1.seqno, initial_seqno);
    auto refresh_time_1 = snapshot_mgr_->last_refresh_time(domain);
    ASSERT_TRUE(refresh_time_1.has_value());

    // Write more data to increase seqno and get another snapshot at that
    // snapshot.
    auto new_seqno = write_data(domain, o{4}, o{5}).get();
    EXPECT_GT(new_seqno, initial_seqno);
    auto s2 = check_get_snapshot(
      domain, ss::lowres_clock::time_point::min(), new_seqno);
    EXPECT_EQ(s2.seqno, new_seqno);

    // Refresh time should advance because we requested a newer seqno.
    auto refresh_time_2 = snapshot_mgr_->last_refresh_time(domain);
    ASSERT_TRUE(refresh_time_2.has_value());
    EXPECT_GT(*refresh_time_2, *refresh_time_1);
}

TEST_F(SnapshotManagerTest, WaitForRefreshTimeNotNewEnough) {
    write_data(domain, o{0}, o{3}).get();

    // Get initial snapshot.
    auto s1 = check_get_snapshot(domain);
    auto refresh_time_1 = snapshot_mgr_->last_refresh_time(domain);
    ASSERT_TRUE(refresh_time_1.has_value());

    // Write more data
    auto new_seqno = write_data(domain, o{4}, o{5}).get();
    EXPECT_GT(new_seqno, s1.seqno);

    // Request snapshot with a time that it definitely doesn't have (some time
    // in the future). This should trigger a refresh.
    auto future_time = ss::lowres_clock::now() + 200ms;
    auto s2 = check_get_snapshot(domain, future_time);
    EXPECT_EQ(s2.seqno, new_seqno);

    // Refresh time should advance because we requested a newer refresh time.
    auto refresh_time_2 = snapshot_mgr_->last_refresh_time(domain);
    ASSERT_TRUE(refresh_time_2.has_value());
    EXPECT_GT(*refresh_time_2, *refresh_time_1);
}

TEST_F(SnapshotManagerTest, WaitForRefreshTimeoutWhenSeqnoUnavailable) {
    auto initial_seqno = write_data(domain, o{0}, o{3}).get();
    check_get_snapshot(domain);
    auto refresh_time_before = snapshot_mgr_->last_refresh_time(domain);
    ASSERT_TRUE(refresh_time_before.has_value());

    // Request an impossibly high seqno. This should time out.
    auto impossible_seqno = lsm::sequence_number{initial_seqno() + 1000};
    auto res
      = get_snapshot(
          domain, ss::lowres_clock::time_point::min(), impossible_seqno, 50ms)
          .get();
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(res.error().e, read_replica::snapshot_provider::errc::io_error);

    // Even if we didn't get a new enough snapshot, we should have still
    // triggered a refresh.
    auto refresh_time_after = snapshot_mgr_->last_refresh_time(domain);
    ASSERT_TRUE(refresh_time_after.has_value());
    EXPECT_GE(*refresh_time_after, *refresh_time_before);
}

TEST_F(SnapshotManagerTest, NoWaitSnapshotAlreadyAvailable) {
    auto seqno = write_data(domain, o{0}, o{3}).get();
    auto s1 = check_get_snapshot(domain);
    EXPECT_EQ(s1.seqno, seqno);

    auto refresh_time_1 = snapshot_mgr_->last_refresh_time(domain);
    ASSERT_TRUE(refresh_time_1.has_value());

    // Get a snapshot with a refresh time we know we have already.
    auto s2 = check_get_snapshot(domain, *refresh_time_1);
    EXPECT_EQ(s1.seqno, s2.seqno);

    // Refresh time should be unchanged (no refresh occurred).
    auto refresh_time_2 = snapshot_mgr_->last_refresh_time(domain);
    EXPECT_EQ(refresh_time_1, refresh_time_2);

    // Now try with a seqno we know we already have. This should also not
    // trigger a refresh.
    auto s3 = check_get_snapshot(
      domain, ss::lowres_clock::time_point::min(), seqno);
    EXPECT_EQ(s3.seqno, seqno);
    auto refresh_time_3 = snapshot_mgr_->last_refresh_time(domain);
    EXPECT_EQ(refresh_time_1, refresh_time_3);
}

TEST_F(SnapshotManagerTest, IdleCleanup_AfterTimeout) {
    write_data(domain, o{0}, o{3}).get();

    ss::lowres_clock::time_point refresh_time_1;
    {
        auto s = check_get_snapshot(domain);
        refresh_time_1 = *snapshot_mgr_->last_refresh_time(domain);
        // Destruct the snapshot handle.
    }
    // Wait for idle timer to fire, letting the database refresher shutdown.
    ss::sleep(2s).get();

    // Access again. We should have cleaned up the database, and therefore
    // required a refresh.
    check_get_snapshot(domain);
    auto refresh_time_2 = snapshot_mgr_->last_refresh_time(domain);
    ASSERT_TRUE(refresh_time_2.has_value());
    EXPECT_GT(*refresh_time_2, refresh_time_1);
}

TEST_F(SnapshotManagerTest, ConcurrentFirstTimeCallsShareRefresh) {
    write_data(domain, o{0}, o{3}).get();

    auto future_time = ss::lowres_clock::now() + 200ms;
    auto fut1 = get_snapshot(domain, future_time);
    auto fut2 = get_snapshot(domain, future_time);
    auto fut3 = get_snapshot(domain, future_time);
    auto s1 = std::move(fut1).get();
    auto s2 = std::move(fut2).get();
    auto s3 = std::move(fut3).get();

    ASSERT_TRUE(s1.has_value());
    ASSERT_TRUE(s2.has_value());
    ASSERT_TRUE(s3.has_value());

    // All should have the same seqno.
    EXPECT_EQ(s1->seqno, s2->seqno);
    EXPECT_EQ(s2->seqno, s3->seqno);
}

TEST_F(SnapshotManagerTest, GetSnapshotAfterStopReturnsError) {
    write_data(domain, o{0}, o{3}).get();
    close_writer(domain).get();

    // Stop the snapshot manager.
    snapshot_mgr_->stop().get();
    auto cleanup = ss::defer([this] { snapshot_mgr_.reset(); });

    auto res = get_snapshot(domain).get();
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(
      res.error().e, read_replica::snapshot_provider::errc::shutting_down);
}
