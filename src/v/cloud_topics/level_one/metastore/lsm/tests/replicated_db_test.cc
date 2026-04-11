/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/remote.h"
#include "cloud_io/tests/s3_imposter.h"
#include "cloud_io/tests/scoped_remote.h"
#include "cloud_storage_clients/types.h"
#include "cloud_topics/level_one/metastore/domain_uuid.h"
#include "cloud_topics/level_one/metastore/lsm/replicated_db.h"
#include "cloud_topics/level_one/metastore/lsm/stm.h"
#include "cloud_topics/level_one/metastore/lsm/write_batch_row.h"
#include "config/node_config.h"
#include "gmock/gmock.h"
#include "lsm/io/cloud_persistence.h"
#include "lsm/io/persistence.h"
#include "model/fundamental.h"
#include "raft/tests/raft_fixture.h"
#include "random/generators.h"
#include "test_utils/async.h"
#include "test_utils/scoped_config.h"
#include "test_utils/tmp_dir.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

#include <functional>

using namespace cloud_topics::l1;
using testing::ElementsAre;

namespace {

ss::logger rdb_test_log("replicated_db_test");

MATCHER_P2(MatchesKV, key_str, val_str, "") {
    return arg.key == key_str && arg.value == iobuf::from(val_str);
}

MATCHER_P2(MatchesRow, key_str, val_str, "") {
    return arg.row.key == key_str && arg.row.value == iobuf::from(val_str);
}

ss::future<chunked_vector<write_batch_row>> get_rows(lsm::database& db) {
    chunked_vector<write_batch_row> ret;
    auto it = co_await db.create_iterator();
    co_await it.seek_to_first();
    while (it.valid()) {
        ret.emplace_back(
          write_batch_row{
            .key = ss::sstring(it.key()),
            .value = it.value(),
          });
        co_await it.next();
    }
    co_return ret;
}

struct replicated_db_node {
    replicated_db_node(
      ss::shared_ptr<stm> s,
      cloud_io::remote* remote,
      const cloud_storage_clients::bucket_name& bucket,
      const ss::sstring& staging_path)
      : stm_ptr(std::move(s))
      , remote(remote)
      , bucket(bucket)
      , staging_directory(staging_path.data()) {}

    ss::future<std::expected<replicated_database*, replicated_database::error>>
    open_db() {
        auto ret = co_await replicated_database::open(
          stm_ptr->raft()->confirmed_term(),
          stm_ptr.get(),
          staging_directory.get_path(),
          remote,
          bucket,
          as,
          ss::default_scheduling_group());
        if (!ret.has_value()) {
            co_return std::unexpected(ret.error());
        }
        auto ptr = ret.value().get();
        dbs.push_back(std::move(ret.value()));
        co_return ptr;
    }

    ss::future<> close() {
        for (auto& db : dbs) {
            auto res = co_await db->close();
            if (!res.has_value()) {
                vlog(
                  rdb_test_log.warn,
                  "Failed to close DB for node {}",
                  stm_ptr->raft()->self());
            }
        }
    }

    ss::shared_ptr<stm> stm_ptr;
    cloud_io::remote* remote;
    const cloud_storage_clients::bucket_name& bucket;
    temporary_dir staging_directory;
    ss::abort_source as;
    std::list<std::unique_ptr<replicated_database>> dbs;
};

} // namespace

class ReplicatedDatabaseTest
  : public raft::raft_fixture
  , public s3_imposter_fixture {
public:
    static constexpr auto num_nodes = 3;
    using opt_ref = std::optional<std::reference_wrapper<replicated_db_node>>;

    void SetUp() override {
        ss::smp::invoke_on_all([] {
            config::node().node_id.set_value(model::node_id{1});
        }).get();
        cfg.get("raft_heartbeat_interval_ms").set_value(50ms);
        cfg.get("raft_heartbeat_timeout_ms").set_value(500ms);

        set_expectations_and_listen({});
        sr = cloud_io::scoped_remote::create(10, conf);

        raft::raft_fixture::SetUpAsync().get();

        // Create our STMs.
        for (auto i = 0; i < num_nodes; ++i) {
            add_node(model::node_id(i), model::revision_id(0));
        }
        for (auto& [id, node] : nodes()) {
            node->initialise(all_vnodes()).get();
            auto* raft = node->raft().get();
            raft::state_machine_manager_builder builder;
            auto s = builder.create_stm<stm>(
              rdb_test_log,
              raft,
              config::mock_binding<std::chrono::seconds>(1s));

            node->start(std::move(builder)).get();

            // Create staging directory for this node.
            auto staging_path = fmt::format("replicated_db_test_{}", id());
            db_nodes.at(id()) = std::make_unique<replicated_db_node>(
              std::move(s), &sr->remote.local(), bucket_name, staging_path);
        }
        opt_ref leader;
        ASSERT_NO_FATAL_FAILURE(wait_for_leader(leader).get());
        initial_leader = &leader->get();

        auto db_res = initial_leader->open_db().get();
        ASSERT_TRUE(db_res.has_value());
        initial_db = db_res.value();
    }

    void TearDown() override {
        for (auto& node : db_nodes) {
            if (node) {
                node->close().get();
            }
        }
        raft::raft_fixture::TearDownAsync().get();
        sr.reset();
    }

    // Returns the node on the current leader.
    opt_ref leader_node() {
        auto leader_id = get_leader();
        if (!leader_id.has_value()) {
            return std::nullopt;
        }
        auto& node = *db_nodes.at(leader_id.value()());
        if (!node.stm_ptr->raft()->is_leader()) {
            return std::nullopt;
        }
        return node;
    }

    // Waits for a leader to be elected, and returns it.
    ss::future<> wait_for_leader(opt_ref& leader) {
        RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [&] {
            leader = leader_node();
            return leader.has_value();
        });
    }

    // Waits for all nodes to have applied the current committed offset.
    ss::future<> wait_for_apply() {
        model::offset committed_offset{};
        for (auto& n : nodes()) {
            committed_offset = std::max(
              committed_offset, n.second->raft()->committed_offset());
        }

        co_await parallel_for_each_node([committed_offset](auto& node) {
            return node.raft()->stm_manager()->wait(
              committed_offset, model::no_timeout);
        });
    }

    // Creates a manifest in the given epoch and domain with the given rows.
    lsm::proto::manifest create_manifest(
      uint64_t db_epoch,
      domain_uuid domain_uuid,
      chunked_vector<volatile_row> rows) {
        auto domain_prefix = cloud_storage_clients::object_key{
          domain_cloud_prefix(domain_uuid)};
        temporary_dir tmp("lsm_staging_scratch");
        auto cloud_db
          = lsm::database::open(
              {.database_epoch = db_epoch},
              lsm::io::persistence{
                .data = lsm::io::open_cloud_data_persistence(
                          tmp.get_path(),
                          &sr->remote.local(),
                          bucket_name,
                          domain_prefix,
                          ss::sstring(domain_uuid()))
                          .get(),
                .metadata = lsm::io::open_cloud_metadata_persistence(
                              &sr->remote.local(), bucket_name, domain_prefix)
                              .get()})
              .get();

        // Write some data and then flush to a new manifest.
        auto wb = cloud_db.create_write_batch();
        for (auto& r : rows) {
            wb.put(r.row.key, std::move(r.row.value), r.seqno);
        }
        cloud_db.apply(std::move(wb)).get();
        cloud_db.flush(ssx::instant::infinite_future()).get();
        cloud_db.close().get();
        auto cloud_meta_persistence = lsm::io::open_cloud_metadata_persistence(
                                        &sr->remote.local(),
                                        bucket_name,
                                        domain_prefix)
                                        .get();
        auto cloud_buf = cloud_meta_persistence
                           ->read_manifest(lsm::internal::database_epoch::max())
                           .get();
        std::optional<lsm::proto::manifest> manifest;
        if (cloud_buf) {
            manifest
              = lsm::proto::manifest::from_proto(std::move(*cloud_buf)).get();
        }
        EXPECT_TRUE(manifest.has_value());
        return std::move(*manifest);
    }

    std::array<std::unique_ptr<replicated_db_node>, num_nodes> db_nodes;
    scoped_config cfg;
    std::unique_ptr<cloud_io::scoped_remote> sr;

    // Initial leader and a database opened on that leader.
    replicated_db_node* initial_leader;
    replicated_database* initial_db;
};

// Test that if we're able to open the database, we will have already assigned
// a domain UUID and it matches on all nodes.
TEST_F(ReplicatedDatabaseTest, TestMatchingDomainUUID) {
    wait_for_apply().get();
    auto domain_uuid = initial_db->get_domain_uuid();
    for (const auto& node : db_nodes) {
        EXPECT_EQ(node->stm_ptr->state().domain_uuid, domain_uuid);
    }
}

// Basic test for writing. Once we write, we can read the rows back. Once we
// are no longer leader, we can no longer write. The new leader should be able
// to read the rows from the previous term.
TEST_F(ReplicatedDatabaseTest, TestBasicWrites) {
    // Write some initial data.
    chunked_vector<write_batch_row> rows1;
    rows1.emplace_back(
      write_batch_row{
        .key = "key1",
        .value = iobuf::from("value1"),
      });
    auto write_res = initial_db->write(std::move(rows1)).get();
    ASSERT_TRUE(write_res.has_value());
    wait_for_apply().get();
    EXPECT_THAT(
      get_rows(initial_db->db()).get(),
      ElementsAre(MatchesKV("key1", "value1")));

    // Elect a new leader.
    initial_leader->stm_ptr->raft()->step_down("test").get();
    opt_ref leader_opt;
    ASSERT_NO_FATAL_FAILURE(wait_for_leader(leader_opt).get());
    auto& new_leader = leader_opt->get();

    // Open database on the new leader.
    auto new_db_result = new_leader.open_db().get();
    ASSERT_TRUE(new_db_result.has_value());
    auto& new_db = *new_db_result.value();

    // We should see the existing rows...
    EXPECT_THAT(
      get_rows(new_db.db()).get(), ElementsAre(MatchesKV("key1", "value1")));

    // ...and be able to write more.
    chunked_vector<write_batch_row> rows2;
    rows2.emplace_back(
      write_batch_row{
        .key = "key2",
        .value = iobuf::from("value2"),
      });
    write_res = new_db.write(std::move(rows2)).get();
    ASSERT_TRUE(write_res.has_value());
    EXPECT_THAT(
      get_rows(new_db.db()).get(),
      ElementsAre(MatchesKV("key1", "value1"), MatchesKV("key2", "value2")));

    // Validate that all nodes see the same state.
    wait_for_apply().get();
    for (const auto& node : db_nodes) {
        EXPECT_THAT(
          node->stm_ptr->state().volatile_buffer,
          ElementsAre(
            MatchesRow("key1", "value1"), MatchesRow("key2", "value2")));
    }
}

// Basic test for flushing. Once we flush, we can still read the rows back, but
// shouldn't see these as volatile rows in the STM. This should be true for
// subsequent leaders.
TEST_F(ReplicatedDatabaseTest, TestBasicFlush) {
    // Write some initial data.
    chunked_vector<write_batch_row> rows1;
    rows1.emplace_back(
      write_batch_row{
        .key = "key1",
        .value = iobuf::from("value1"),
      });
    auto write_res = initial_db->write(std::move(rows1)).get();
    ASSERT_TRUE(write_res.has_value());
    wait_for_apply().get();
    EXPECT_THAT(
      get_rows(initial_db->db()).get(),
      ElementsAre(MatchesKV("key1", "value1")));

    // Flush and validate that we have nothing in the resulting buffer.
    auto flush_res = initial_db->flush().get();
    ASSERT_TRUE(flush_res.has_value());

    // The rows should be visible through the database, and shouldn't exist in
    // the volatile buffer since they've been flushed.
    EXPECT_THAT(
      get_rows(initial_db->db()).get(),
      ElementsAre(MatchesKV("key1", "value1")));
    wait_for_apply().get();
    for (const auto& node : db_nodes) {
        EXPECT_THAT(node->stm_ptr->state().volatile_buffer, ElementsAre());
    }

    // Write some more.
    chunked_vector<write_batch_row> rows2;
    rows2.emplace_back(
      write_batch_row{
        .key = "key2",
        .value = iobuf::from("value2"),
      });
    write_res = initial_db->write(std::move(rows2)).get();
    ASSERT_TRUE(write_res.has_value());
    EXPECT_THAT(
      get_rows(initial_db->db()).get(),
      ElementsAre(MatchesKV("key1", "value1"), MatchesKV("key2", "value2")));

    // The volatile buffer only includes the new rows on all replicas.
    wait_for_apply().get();
    for (const auto& node : db_nodes) {
        EXPECT_THAT(
          node->stm_ptr->state().volatile_buffer,
          ElementsAre(MatchesRow("key2", "value2")));
    }

    // Opening the database on a new leader should yield all the rows.
    initial_leader->stm_ptr->raft()->step_down("test").get();
    opt_ref leader_opt;
    ASSERT_NO_FATAL_FAILURE(wait_for_leader(leader_opt).get());
    auto& new_leader = leader_opt->get();
    auto db_res = new_leader.open_db().get();
    ASSERT_TRUE(db_res.has_value());
    auto& new_db = *db_res.value();
    EXPECT_THAT(
      get_rows(new_db.db()).get(),
      ElementsAre(MatchesKV("key1", "value1"), MatchesKV("key2", "value2")));
}

// A flush with nothing in the db is a no-op.
TEST_F(ReplicatedDatabaseTest, TestEmptyFlush) {
    auto flush_res = initial_db->flush().get();
    EXPECT_TRUE(flush_res.has_value());
    EXPECT_FALSE(
      initial_leader->stm_ptr->state().persisted_manifest.has_value());
}

// Test that we can reset the manifest from an input manifest.
TEST_F(ReplicatedDatabaseTest, TestReset) {
    // Get the initial domain UUID
    auto initial_uuid = initial_db->get_domain_uuid();
    ASSERT_FALSE(initial_uuid().is_nil());

    // Create a manifest under a new domain UUID.
    auto new_uuid = domain_uuid(uuid_t::create());
    ASSERT_NE(initial_uuid, new_uuid);

    const auto new_seqno = lsm::sequence_number{12345};
    chunked_vector<volatile_row> initial_rows;
    initial_rows.emplace_back(
      volatile_row{
        .seqno = new_seqno,
        .row
        = write_batch_row{.key = "key_before_reset", .value = iobuf::from("value_before_reset"),},
      });
    auto manifest = create_manifest(6789, new_uuid, std::move(initial_rows));

    // Reset the STM from the manifest.
    auto reset_res = initial_db->reset(new_uuid, std::move(manifest)).get();
    ASSERT_TRUE(reset_res.has_value());

    // Verify the domain UUID was updated and that the data is what we expect.
    ASSERT_EQ(initial_db->get_domain_uuid(), new_uuid);
    ASSERT_EQ(initial_leader->stm_ptr->state().domain_uuid, new_uuid);
    ASSERT_TRUE(initial_db->needs_reopen());

    auto db_res = initial_leader->open_db().get();
    ASSERT_TRUE(db_res.has_value());
    auto& reopened_db = *db_res.value();
    EXPECT_EQ(new_seqno, reopened_db.db().max_persisted_seqno());
    EXPECT_EQ(new_seqno, reopened_db.db().max_applied_seqno());

    EXPECT_THAT(
      get_rows(reopened_db.db()).get(),
      ElementsAre(MatchesKV("key_before_reset", "value_before_reset")));

    ASSERT_GT(initial_leader->stm_ptr->state().seqno_delta, 0);
    ASSERT_EQ(0, initial_leader->stm_ptr->state().volatile_buffer.size());

    // Write more with the reopened database and validate again.
    chunked_vector<write_batch_row> rows;
    rows.emplace_back(
      write_batch_row{
        .key = "key_after_reset",
        .value = iobuf::from("value_after_reset"),
      });
    auto write_result = reopened_db.write(std::move(rows)).get();
    ASSERT_TRUE(write_result.has_value());
    ASSERT_EQ(1, initial_leader->stm_ptr->state().volatile_buffer.size());
    EXPECT_THAT(
      get_rows(reopened_db.db()).get(),
      ElementsAre(
        MatchesKV("key_after_reset", "value_after_reset"),
        MatchesKV("key_before_reset", "value_before_reset")));
}

TEST_F(ReplicatedDatabaseTest, TestFlushFailure) {
    // Write some rows.
    chunked_vector<write_batch_row> rows;
    rows.emplace_back(
      write_batch_row{
        .key = "key1",
        .value = iobuf::from("value1"),
      });
    auto write_res = initial_db->write(std::move(rows)).get();
    ASSERT_TRUE(write_res.has_value());
    wait_for_apply().get();
    EXPECT_THAT(
      get_rows(initial_db->db()).get(),
      ElementsAre(MatchesKV("key1", "value1")));

    // Step down as leader of the given term.
    initial_leader->stm_ptr->raft()->step_down("test").get();
    opt_ref leader_opt;
    ASSERT_NO_FATAL_FAILURE(wait_for_leader(leader_opt).get());

    // Still try to flush with the same initial database. This should fail
    // gracefully (no hangs, crashes, etc).
    auto flush_res = initial_db->flush(1s).get();
    ASSERT_FALSE(flush_res.has_value());
    EXPECT_EQ(flush_res.error().e, replicated_database::errc::io_error);
}

TEST_F(ReplicatedDatabaseTest, TestResetWithEmptyManifest) {
    auto initial_uuid = initial_db->get_domain_uuid();
    ASSERT_FALSE(initial_uuid().is_nil());

    auto new_uuid = domain_uuid(uuid_t::create());
    ASSERT_NE(initial_uuid, new_uuid);

    // Reset from an empty manifest.
    auto reset_result = initial_db->reset(new_uuid, std::nullopt).get();
    ASSERT_TRUE(reset_result.has_value());

    // The domain UUID should still be updated.
    ASSERT_EQ(initial_db->get_domain_uuid(), new_uuid);
    ASSERT_EQ(initial_leader->stm_ptr->state().domain_uuid, new_uuid);
    ASSERT_TRUE(initial_db->needs_reopen());

    // Verify database is still functional - write and read data
    chunked_vector<write_batch_row> rows;
    rows.emplace_back(
      write_batch_row{
        .key = "key_after_reset",
        .value = iobuf::from("value_after_reset"),
      });

    // Writes should fail, and we should need to reopen the database.
    auto write_res = initial_db->write(std::move(rows)).get();
    ASSERT_FALSE(write_res.has_value());
    ASSERT_EQ(0, initial_leader->stm_ptr->state().volatile_buffer.size());
    ASSERT_TRUE(initial_db->needs_reopen());

    // Try again with the reopened database.
    chunked_vector<write_batch_row> new_rows;
    new_rows.emplace_back(
      write_batch_row{
        .key = "key_after_reset",
        .value = iobuf::from("value_after_reset"),
      });
    auto db_res = initial_leader->open_db().get();
    ASSERT_TRUE(db_res.has_value());
    auto& reopened_db = *db_res.value();
    write_res = reopened_db.write(std::move(new_rows)).get();
    ASSERT_TRUE(write_res.has_value());
    ASSERT_EQ(1, initial_leader->stm_ptr->state().volatile_buffer.size());
    EXPECT_THAT(
      get_rows(reopened_db.db()).get(),
      ElementsAre(MatchesKV("key_after_reset", "value_after_reset")));
}

TEST_F(ReplicatedDatabaseTest, TestResetFailsNonEmpty) {
    // Write a row.
    chunked_vector<write_batch_row> rows;
    rows.emplace_back(
      write_batch_row{
        .key = "existing_key",
        .value = iobuf::from("existing_value"),
      });
    auto write_res = initial_db->write(std::move(rows)).get();
    ASSERT_TRUE(write_res.has_value());
    wait_for_apply().get();

    // Create a manifest.
    auto new_uuid = domain_uuid(uuid_t::create());
    ASSERT_NE(initial_db->get_domain_uuid(), new_uuid);
    chunked_vector<volatile_row> manifest_rows;
    manifest_rows.emplace_back(
      volatile_row{
        .seqno = lsm::sequence_number{100},
        .row
        = write_batch_row{.key = "reset_key", .value = iobuf::from("reset_value"),},
      });
    auto manifest = create_manifest(999, new_uuid, std::move(manifest_rows));

    // Attempt to reset with the manifest. This should fail since the STM is
    // non-empty.
    auto reset_res = initial_db->reset(new_uuid, std::move(manifest)).get();
    ASSERT_FALSE(reset_res.has_value());

    // Now flush.
    auto flush_res = initial_db->flush().get();
    ASSERT_TRUE(flush_res.has_value());
    wait_for_apply().get();

    // Try reseting again. This should still fail.
    chunked_vector<volatile_row> new_rows;
    new_rows.emplace_back(
      volatile_row{
        .seqno = lsm::sequence_number{100},
        .row
        = write_batch_row{.key = "reset_key", .value = iobuf::from("reset_value"),},
      });
    manifest = create_manifest(999, new_uuid, std::move(new_rows));
    reset_res = initial_db->reset(new_uuid, std::move(manifest)).get();
    ASSERT_FALSE(reset_res.has_value());

    // Do a sanity check that the STM still has our expected row.
    EXPECT_THAT(
      get_rows(initial_db->db()).get(),
      ElementsAre(MatchesKV("existing_key", "existing_value")));
}

TEST_F(ReplicatedDatabaseTest, TestConcurrentWrites) {
    // Kick off writes in parallel.
    constexpr int num_concurrent_writes = 5;
    std::vector<ss::future<std::expected<void, replicated_database::error>>>
      write_futs;
    write_futs.reserve(num_concurrent_writes);
    for (int i = 0; i < num_concurrent_writes; ++i) {
        chunked_vector<write_batch_row> rows;
        rows.emplace_back(
          write_batch_row{
            .key = fmt::format("key{}", i),
            .value = iobuf::from(fmt::format("value{}", i)),
          });
        write_futs.push_back(initial_db->write(std::move(rows)));
    }
    auto results
      = ss::when_all_succeed(write_futs.begin(), write_futs.end()).get();

    // Verify all writes succeeded.
    for (const auto& result : results) {
        ASSERT_TRUE(result.has_value());
    }
    EXPECT_THAT(
      get_rows(initial_db->db()).get(),
      ElementsAre(
        MatchesKV("key0", "value0"),
        MatchesKV("key1", "value1"),
        MatchesKV("key2", "value2"),
        MatchesKV("key3", "value3"),
        MatchesKV("key4", "value4")));
}

// Regression test that tombstones in the volatile buffer are properly replayed
// as deletions when opening the database on a new leader.
TEST_F(ReplicatedDatabaseTest, TestReplayTombstones) {
    chunked_vector<write_batch_row> rows1;
    rows1.emplace_back(
      write_batch_row{
        .key = "key1",
        .value = iobuf::from("value1"),
      });
    auto write_res = initial_db->write(std::move(rows1)).get();
    ASSERT_TRUE(write_res.has_value());

    // Write a tombstone.
    chunked_vector<write_batch_row> tombstone_rows;
    tombstone_rows.emplace_back(
      write_batch_row{
        .key = "key1",
        .value = iobuf{},
      });
    write_res = initial_db->write(std::move(tombstone_rows)).get();
    ASSERT_TRUE(write_res.has_value());
    EXPECT_THAT(get_rows(initial_db->db()).get(), ElementsAre());

    // Transfer leadership. The new leader will replay the tombstone from the
    // volatile buffer.
    initial_leader->stm_ptr->raft()->step_down("test").get();
    opt_ref leader_opt;
    ASSERT_NO_FATAL_FAILURE(wait_for_leader(leader_opt).get());
    auto& new_leader = leader_opt->get();

    auto new_db_result = new_leader.open_db().get();
    ASSERT_TRUE(new_db_result.has_value());
    auto& new_db = *new_db_result.value();

    // The key should still be deleted after replay.
    EXPECT_THAT(get_rows(new_db.db()).get(), ElementsAre());
}

// Test that concurrent flush and write operations execute correctly.
TEST_F(ReplicatedDatabaseTest, TestConcurrentFlushAndWrite) {
    // Start a fiber that is repeatedly calling flush until we stop it.
    ss::abort_source flush_as;
    auto flush_fut = ss::do_until(
      [&flush_as] { return flush_as.abort_requested(); },
      [this] {
          return initial_db->flush().then([](auto) { return ss::sleep(1ms); });
      });

    // Call write a few times.
    for (int i = 0; i < 10; ++i) {
        chunked_vector<write_batch_row> rows;
        rows.emplace_back(
          write_batch_row{
            .key = fmt::format("key{}", i),
            .value = iobuf::from(fmt::format("value{}", i)),
          });
        auto write_res = initial_db->write(std::move(rows)).get();
        ASSERT_TRUE(write_res.has_value());
    }

    // Stop the flush fiber.
    flush_as.request_abort();
    flush_fut.get();

    // Validate the rows match what we expect.
    EXPECT_THAT(
      get_rows(initial_db->db()).get(),
      ElementsAre(
        MatchesKV("key0", "value0"),
        MatchesKV("key1", "value1"),
        MatchesKV("key2", "value2"),
        MatchesKV("key3", "value3"),
        MatchesKV("key4", "value4"),
        MatchesKV("key5", "value5"),
        MatchesKV("key6", "value6"),
        MatchesKV("key7", "value7"),
        MatchesKV("key8", "value8"),
        MatchesKV("key9", "value9")));
}
