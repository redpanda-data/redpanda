/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_io/remote.h"
#include "cloud_io/tests/scoped_remote.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "datalake/coordinator/iceberg_file_committer.h"
#include "datalake/coordinator/tests/state_test_utils.h"
#include "datalake/table_definition.h"
#include "iceberg/filesystem_catalog.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_io.h"
#include "iceberg/metadata_query.h"
#include "iceberg/transaction.h"
#include "iceberg/values_bytes.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

using namespace datalake::coordinator;

namespace {
const model::topic topic{"test-topic"};
using pairs_t = std::vector<std::pair<int64_t, int64_t>>;
void add_partition_state(
  std::vector<pairs_t> offset_bounds_by_pid,
  topic_state& state,
  model::offset added_at,
  bool with_files) {
    for (size_t i = 0; i < offset_bounds_by_pid.size(); i++) {
        auto pid = static_cast<model::partition_id>(i);
        partition_state p_state;
        for (auto& f :
             make_pending_files(offset_bounds_by_pid[i], with_files)) {
            p_state.pending_entries.emplace_back(pending_entry{
              .data = std::move(f), .added_pending_at = added_at});
        }
        state.pid_to_pending_files[pid] = std::move(p_state);
    }
}
topic_state make_topic_state(
  std::vector<pairs_t> offset_bounds_by_pid,
  model::offset added_at = model::offset{1000},
  bool with_files = false) {
    topic_state state;
    add_partition_state(
      std::move(offset_bounds_by_pid), state, added_at, with_files);
    return state;
}

// Simulates a coordinator repeatedly committing a fixed sequence of data
// files.
ss::future<>
file_committer_loop(file_committer& committer, size_t num_chunks, bool& done) {
    for (size_t i = 0; i < num_chunks; ++i) {
        while (!done) {
            topics_state state;
            state.topic_to_state[topic] = make_topic_state(
              {{{i * 100, i * 100 + 99}}},
              model::offset{static_cast<int64_t>(i)},
              true);
            auto res
              = committer.commit_topic_files_to_catalog(topic, state).get();
            if (res.has_value()) {
                break;
            }
            // Keep retrying until there is no error. This may be the case if
            // we actually committed files, or if we deduplicated some files.
        }
    }
    done = true;
    co_return;
}
} // namespace

class FileCommitterTest
  : public s3_imposter_fixture
  , public ::testing::Test {
public:
    static constexpr std::string_view base_location{"test"};
    FileCommitterTest()
      : sr(cloud_io::scoped_remote::create(10, conf))
      , catalog(remote(), bucket_name, ss::sstring(base_location))
      , schema_mgr(catalog)
      , manifest_io(remote(), bucket_name)
      , committer(catalog, manifest_io) {
        set_expectations_and_listen({});
    }
    cloud_io::remote& remote() { return sr->remote.local(); }

    void create_table() {
        auto res = schema_mgr
                     .ensure_table_schema(
                       topic, datalake::schemaless_struct_type())
                     .get();
        ASSERT_FALSE(res.has_error());
    }

    std::unique_ptr<cloud_io::scoped_remote> sr;
    iceberg::filesystem_catalog catalog;
    datalake::catalog_schema_manager schema_mgr;
    iceberg::manifest_io manifest_io;
    iceberg_file_committer committer;
};

TEST_F(FileCommitterTest, TestCommit) {
    create_table();
    topics_state state;
    state.topic_to_state[topic] = make_topic_state({
      {{0, 99}, {100, 199}},
      {{0, 99}, {100, 199}, {200, 299}},
      {},
      {{100, 199}, {200, 299}},
    });
    auto res = committer.commit_topic_files_to_catalog(topic, state).get();
    ASSERT_FALSE(res.has_error());
    auto updates = std::move(res.value());
    ASSERT_EQ(updates.size(), 3);
    ASSERT_EQ(
      updates[0].tp, model::topic_partition(topic, model::partition_id{0}));
    ASSERT_EQ(updates[0].new_committed(), 199);
    ASSERT_EQ(
      updates[1].tp, model::topic_partition(topic, model::partition_id{1}));
    ASSERT_EQ(updates[1].new_committed(), 299);
    ASSERT_EQ(
      updates[2].tp, model::topic_partition(topic, model::partition_id{3}));
    ASSERT_EQ(updates[2].new_committed(), 299);
}

TEST_F(FileCommitterTest, TestMissingTable) {
    auto load_res = catalog
                      .load_table(
                        iceberg::table_identifier{{"redpanda"}, "test-topic"})
                      .get();
    ASSERT_TRUE(load_res.has_error());
    ASSERT_EQ(load_res.error(), iceberg::catalog::errc::not_found);

    // Add topic to the starting state with nothing in it. This should no-op.
    topics_state state;
    state.topic_to_state[topic] = make_topic_state({});

    // If there are no files to commit, this should be a no-op even if the table
    // is not there yet.
    auto res = committer.commit_topic_files_to_catalog(topic, state).get();
    ASSERT_FALSE(res.has_error());
    ASSERT_EQ(res.value().size(), 0);

    create_table();

    res = committer.commit_topic_files_to_catalog(topic, state).get();
    ASSERT_FALSE(res.has_error());
    ASSERT_TRUE(res.value().empty());
    load_res = catalog
                 .load_table(
                   iceberg::table_identifier{{"redpanda"}, "test-topic"})
                 .get();
    // The table should be created.
    ASSERT_FALSE(load_res.has_error());
    ASSERT_FALSE(load_res.value().snapshots.has_value());

    // Now try again with some data.
    state.topic_to_state[topic] = make_topic_state({{{0, 100}}});
    res = committer.commit_topic_files_to_catalog(topic, state).get();
    ASSERT_FALSE(res.has_error());
    ASSERT_EQ(1, res.value().size());

    load_res = catalog
                 .load_table(
                   iceberg::table_identifier{{"redpanda"}, "test-topic"})
                 .get();
    ASSERT_FALSE(load_res.has_error());

    // Simple check for the schema.
    const auto& table = load_res.value();
    ASSERT_EQ(1, table.schemas.size());
    ASSERT_EQ(1, table.schemas[0].schema_struct.fields.size());
    ASSERT_EQ(1, table.partition_specs.size());
    ASSERT_EQ(1, table.partition_specs[0].fields.size());
}

TEST_F(FileCommitterTest, TestMissingTopic) {
    topics_state state;
    auto res = committer.commit_topic_files_to_catalog(topic, state).get();
    ASSERT_FALSE(res.has_error());
    ASSERT_TRUE(res.value().empty());

    // If our state didn't have the topic, we won't bother creating a table.
    auto load_res = catalog
                      .load_table(
                        iceberg::table_identifier{{"redpanda"}, "test-topic"})
                      .get();
    ASSERT_TRUE(load_res.has_error());
    ASSERT_EQ(load_res.error(), iceberg::catalog::errc::not_found);
}

TEST_F(FileCommitterTest, TestFilesGetPartitionKey) {
    create_table();

    using namespace iceberg;
    // Constructs topic state with offset ranges added to partition 0.
    model::offset added_at_counter{1000};
    auto make_single_partition_state = [&](pairs_t offsets, int hour) {
        topics_state state;
        auto t_state = make_topic_state(
          {
            std::move(offsets),
          },
          added_at_counter++);
        for (auto& e : t_state.pid_to_pending_files[model::partition_id{0}]
                         .pending_entries) {
            e.data.files.emplace_back(datalake::coordinator::data_file{
              .row_count = 100,
              .file_size_bytes = 1024,
              .hour = hour,
            });
        }
        state.topic_to_state[topic] = std::move(t_state);
        return state;
    };
    auto state = make_single_partition_state({{0, 99}, {100, 199}}, 10000);
    auto res = committer.commit_topic_files_to_catalog(topic, state).get();
    ASSERT_FALSE(res.has_error());

    // Collects the manifest files whose lower bounds are in the given range
    // (inclusive).
    auto match_hour = [&](
                        int min_hour,
                        int max_hour,
                        chunked_vector<manifest_file>& ret) {
        auto load_res = catalog
                          .load_table(iceberg::table_identifier{
                            {"redpanda"}, "test-topic"})
                          .get();
        ASSERT_FALSE(load_res.has_error());
        auto lb_matcher = [min_hour, max_hour](const manifest_file& file) {
            auto val = std::get<int_value>(
                         std::get<primitive_value>(value_from_bytes(
                           int_type{}, file.partitions[0].lower_bound.value())))
                         .val;
            return val >= min_hour && val <= max_hour;
        };
        metadata_query_executor executor(manifest_io, load_res.value());
        metadata_query<result_type::manifest_file> q_m{
          .manifest_file_matcher = lb_matcher};
        auto query_res = executor.execute_query(q_m).get();
        ASSERT_FALSE(query_res.has_error());
        ret = std::move(query_res.value());
    };
    chunked_vector<manifest_file> mfiles;
    ASSERT_NO_FATAL_FAILURE(match_hour(10000, 10000, mfiles));

    // When we committed the file, it should have created a new manifest.
    ASSERT_EQ(1, mfiles.size());
    ASSERT_EQ(2, mfiles[0].added_files_count);
    ASSERT_EQ(200, mfiles[0].added_rows_count);

    state = make_single_partition_state({{200, 299}}, 10001);
    res = committer.commit_topic_files_to_catalog(topic, state).get();
    ASSERT_FALSE(res.has_error());

    // Committing new hour's state shouldn't affect the results of the first
    // query since it was added to an new hour.
    ASSERT_NO_FATAL_FAILURE(match_hour(10000, 10000, mfiles));
    ASSERT_EQ(1, mfiles.size());
    ASSERT_EQ(2, mfiles[0].added_files_count);
    ASSERT_EQ(200, mfiles[0].added_rows_count);

    // Match the new hour.
    ASSERT_NO_FATAL_FAILURE(match_hour(10001, 10001, mfiles));
    ASSERT_EQ(1, mfiles.size());
    ASSERT_EQ(1, mfiles[0].added_files_count);
    ASSERT_EQ(100, mfiles[0].added_rows_count);

    // Now check that we can filter to find both manifest files.
    ASSERT_NO_FATAL_FAILURE(match_hour(10000, 10001, mfiles));
    ASSERT_EQ(2, mfiles.size());
    ASSERT_EQ(2, mfiles[0].added_files_count);
    ASSERT_EQ(200, mfiles[0].added_rows_count);
    ASSERT_EQ(1, mfiles[1].added_files_count);
    ASSERT_EQ(100, mfiles[1].added_rows_count);
}

// Test that deduplication happens when all of the pending files are already
// committed to Iceberg.
TEST_F(FileCommitterTest, TestDeduplicateAllFiles) {
    create_table();

    topics_state state;
    state.topic_to_state[topic] = make_topic_state(
      {
        {{0, 99}, {100, 199}},
      },
      model::offset{1000},
      true);

    auto res = committer.commit_topic_files_to_catalog(topic, state).get();
    ASSERT_FALSE(res.has_error());
    auto load_res = catalog
                      .load_table(
                        iceberg::table_identifier{{"redpanda"}, "test-topic"})
                      .get();
    ASSERT_FALSE(load_res.has_error());
    ASSERT_TRUE(load_res.value().snapshots.has_value());
    ASSERT_EQ(1, load_res.value().snapshots.value().size());

    auto updates = std::move(res.value());
    ASSERT_EQ(updates.size(), 1);
    ASSERT_EQ(
      updates[0].tp, model::topic_partition(topic, model::partition_id{0}));
    ASSERT_EQ(updates[0].new_committed(), 199);

    // Attempt to commit the same metadata we just committed.
    res = committer.commit_topic_files_to_catalog(topic, state).get();
    ASSERT_FALSE(res.has_error());

    // There should be no update to Iceberg.
    load_res = catalog
                 .load_table(
                   iceberg::table_identifier{{"redpanda"}, "test-topic"})
                 .get();
    ASSERT_FALSE(load_res.has_error());
    ASSERT_TRUE(load_res.value().snapshots.has_value());
    ASSERT_EQ(1, load_res.value().snapshots.value().size());

    // This should result in a metadata update to be replicated, as presumably
    // the earlier one was not successfully replicated (e.g. because of a
    // leadership change).
    updates = std::move(res.value());
    ASSERT_EQ(updates.size(), 1);
    ASSERT_EQ(
      updates[0].tp, model::topic_partition(topic, model::partition_id{0}));
    ASSERT_EQ(updates[0].new_committed(), 199);
}

// Test that deduplication happens when some of the pending files are already
// committed to Iceberg.
TEST_F(FileCommitterTest, TestDeduplicateSomeFiles) {
    create_table();

    topics_state state;
    state.topic_to_state[topic] = make_topic_state(
      {{{0, 99}, {100, 199}}}, model::offset{1000}, true);

    auto res = committer.commit_topic_files_to_catalog(topic, state).get();
    ASSERT_FALSE(res.has_error());
    auto load_res = catalog
                      .load_table(
                        iceberg::table_identifier{{"redpanda"}, "test-topic"})
                      .get();
    ASSERT_FALSE(load_res.has_error());
    ASSERT_TRUE(load_res.value().snapshots.has_value());
    ASSERT_EQ(1, load_res.value().snapshots.value().size());

    auto updates = std::move(res.value());
    ASSERT_EQ(updates.size(), 1);
    ASSERT_EQ(
      updates[0].tp, model::topic_partition(topic, model::partition_id{0}));
    ASSERT_EQ(updates[0].new_committed(), 199);

    // Add additional files, so that there are files that have been committed,
    // _and_ files that have not yet been committed.
    add_partition_state(
      {{{200, 299}}}, state.topic_to_state[topic], model::offset{1001}, true);
    res = committer.commit_topic_files_to_catalog(topic, state).get();
    ASSERT_FALSE(res.has_error());

    // There should be an update to Iceberg, since there were new files
    // committed.
    load_res = catalog
                 .load_table(
                   iceberg::table_identifier{{"redpanda"}, "test-topic"})
                 .get();
    ASSERT_FALSE(load_res.has_error());
    ASSERT_TRUE(load_res.value().snapshots.has_value());
    ASSERT_EQ(2, load_res.value().snapshots.value().size());

    // This should result in a metadata update to be replicated, as there are
    // new files committed.
    updates = std::move(res.value());
    ASSERT_EQ(updates.size(), 1);
    ASSERT_EQ(
      updates[0].tp, model::topic_partition(topic, model::partition_id{0}));
    ASSERT_EQ(updates[0].new_committed(), 299);
}

// Test that deduplication happens when the Iceberg commit metadata is not in
// the latest snapshot.
TEST_F(FileCommitterTest, TestDeduplicateFromAncestor) {
    create_table();

    topics_state state;
    state.topic_to_state[topic] = make_topic_state(
      {
        {{0, 99}, {100, 199}},
      },
      model::offset{1000},
      true);

    auto res = committer.commit_topic_files_to_catalog(topic, state).get();
    ASSERT_FALSE(res.has_error());
    auto updates = std::move(res.value());
    ASSERT_EQ(updates.size(), 1);
    ASSERT_EQ(
      updates[0].tp, model::topic_partition(topic, model::partition_id{0}));
    ASSERT_EQ(updates[0].new_committed(), 199);

    const auto table_id = iceberg::table_identifier{{"redpanda"}, "test-topic"};
    auto load_res = catalog.load_table(table_id).get();
    ASSERT_FALSE(load_res.has_error());
    ASSERT_TRUE(load_res.value().snapshots.has_value());
    ASSERT_EQ(1, load_res.value().snapshots.value().size());
    ASSERT_TRUE(
      load_res.value().snapshots.value().back().summary.other.contains(
        "redpanda.commit-metadata"));

    // Add a new snapshot to the table by appending some data. Explicitly
    // _don't_ add the commit metadata property.
    iceberg::transaction tx(std::move(load_res.value()));
    chunked_vector<iceberg::data_file> new_files;
    iceberg::partition_key pk;
    pk.val = std::make_unique<iceberg::struct_value>();
    pk.val->fields.emplace_back(iceberg::int_value{0});
    new_files.emplace_back(iceberg::data_file{
      .file_path = iceberg::uri("foobar"),
      .partition = std::move(pk),
      .file_size_bytes = 1024,
    });
    auto append_res = tx.merge_append(manifest_io, std::move(new_files)).get();
    ASSERT_FALSE(append_res.has_error());
    EXPECT_FALSE(catalog.commit_txn(table_id, std::move(tx)).get().has_error());
    load_res = catalog.load_table(table_id).get();
    ASSERT_FALSE(load_res.has_error());
    ASSERT_TRUE(load_res.value().snapshots.has_value());
    ASSERT_EQ(2, load_res.value().snapshots.value().size());
    ASSERT_FALSE(
      load_res.value().snapshots.value().back().summary.other.contains(
        "redpanda.commit-metadata"));

    // When we commit, we should get back an update to mark the files
    // committed, but with no corresponding change to Iceberg.
    res = committer.commit_topic_files_to_catalog(topic, state).get();
    ASSERT_FALSE(res.has_error());

    updates = std::move(res.value());
    ASSERT_EQ(updates.size(), 1);
    ASSERT_EQ(
      updates[0].tp, model::topic_partition(topic, model::partition_id{0}));
    ASSERT_EQ(updates[0].new_committed(), 199);

    load_res = catalog.load_table(table_id).get();
    ASSERT_FALSE(load_res.has_error());
    ASSERT_TRUE(load_res.value().snapshots.has_value());
    ASSERT_EQ(2, load_res.value().snapshots.value().size());
}

TEST_F(FileCommitterTest, TestDeduplicateConcurrently) {
    create_table();

    std::vector<ss::future<>> committers;
    constexpr auto num_committers = 10;
    constexpr auto num_chunks = 50;
    committers.reserve(num_committers);
    bool done = false;
    for (int i = 0; i < num_committers; ++i) {
        committers.emplace_back(
          file_committer_loop(committer, num_chunks, done));
    }
    auto stop = ss::defer([&] {
        done = true;
        for (auto& f : committers) {
            f.get();
        }
    });
    RPTEST_REQUIRE_EVENTUALLY(30s, [&done] { return done; });
    for (auto& f : committers) {
        EXPECT_NO_FATAL_FAILURE(f.get());
    }
    stop.cancel();

    const auto table_id = iceberg::table_identifier{{"redpanda"}, "test-topic"};
    auto load_res = catalog.load_table(table_id).get();
    ASSERT_FALSE(load_res.has_error());
    const auto& table = load_res.value();
    ASSERT_TRUE(table.snapshots.has_value());

    // Check that each snapshot does not contain duplicates.
    size_t max_num_files = 0;
    for (const auto& snap : *table.snapshots) {
        const auto& mlist_uri = snap.manifest_list_path;
        auto mlist_res = manifest_io.download_manifest_list(mlist_uri).get();
        ASSERT_TRUE(mlist_res.has_value());
        const auto& mlist = mlist_res.value();

        chunked_vector<ss::sstring> uris;
        chunked_hash_set<ss::sstring> uris_deduped;
        auto pk_type = iceberg::partition_key_type::create(
          datalake::hour_partition_spec(), datalake::default_schema());

        // Collect all the data files for this snapshot.
        for (const auto& m : mlist.files) {
            auto m_res
              = manifest_io.download_manifest(m.manifest_path, pk_type).get();
            ASSERT_TRUE(m_res.has_value());
            for (const auto& e : m_res.value().entries) {
                uris.emplace_back(e.data_file.file_path());
                uris_deduped.emplace(e.data_file.file_path());
            }
        }
        // Ensure no duplicates.
        ASSERT_EQ(uris.size(), uris_deduped.size());
        max_num_files = std::max(uris.size(), max_num_files);
    }
    // The total number of data files should match the number of chunks.
    ASSERT_EQ(max_num_files, num_chunks);
}
