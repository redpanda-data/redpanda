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
#include "iceberg/filesystem_catalog.h"
#include "iceberg/manifest_io.h"
#include "iceberg/metadata_query.h"
#include "iceberg/values_bytes.h"

#include <gtest/gtest.h>

using namespace datalake::coordinator;

namespace {
const model::topic topic{"test-topic"};
using pairs_t = std::vector<std::pair<int64_t, int64_t>>;
topic_state make_topic_state(std::vector<pairs_t> offset_bounds_by_pid) {
    topic_state state;
    for (size_t i = 0; i < offset_bounds_by_pid.size(); i++) {
        auto pid = static_cast<model::partition_id>(i);
        partition_state p_state;
        for (auto& f : make_pending_files(offset_bounds_by_pid[i])) {
            p_state.pending_entries.emplace_back(std::move(f));
        }
        state.pid_to_pending_files[pid] = std::move(p_state);
    }
    return state;
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
      , manifest_io(remote(), bucket_name)
      , committer(catalog, manifest_io) {
        set_expectations_and_listen({});
    }
    cloud_io::remote& remote() { return sr->remote.local(); }

    std::unique_ptr<cloud_io::scoped_remote> sr;
    iceberg::filesystem_catalog catalog;
    iceberg::manifest_io manifest_io;
    iceberg_file_committer committer;
};

TEST_F(FileCommitterTest, TestCommit) {
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

TEST_F(FileCommitterTest, TestLoadOrCreateTable) {
    auto load_res = catalog
                      .load_table(
                        iceberg::table_identifier{{"redpanda"}, "test-topic"})
                      .get();
    ASSERT_TRUE(load_res.has_error());
    ASSERT_EQ(load_res.error(), iceberg::catalog::errc::not_found);

    // Add topic to the starting state with nothing in it. This should no-op.
    topics_state state;
    state.topic_to_state[topic] = make_topic_state({});

    auto res = committer.commit_topic_files_to_catalog(topic, state).get();
    ASSERT_FALSE(res.has_error());
    ASSERT_TRUE(res.value().empty());
    load_res = catalog
                 .load_table(
                   iceberg::table_identifier{{"redpanda"}, "test-topic"})
                 .get();

    // The table shouldn't have been created because of the no-op.
    ASSERT_TRUE(load_res.has_error());
    ASSERT_EQ(load_res.error(), iceberg::catalog::errc::not_found);

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
    ASSERT_EQ(4, table.schemas[0].schema_struct.fields.size());
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
    using namespace iceberg;
    // Constructs topic state with offset ranges added to partition 0.
    auto make_single_partition_state = [&](pairs_t offsets, int hour) {
        topics_state state;
        auto t_state = make_topic_state({
          std::move(offsets),
        });
        for (auto& e : t_state.pid_to_pending_files[model::partition_id{0}]
                         .pending_entries) {
            e.files.emplace_back(datalake::coordinator::data_file{
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
