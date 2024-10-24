// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_io/remote.h"
#include "cloud_io/tests/scoped_remote.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "iceberg/metadata_query.h"
#include "iceberg/table_metadata.h"
#include "iceberg/tests/test_schemas.h"
#include "iceberg/transaction.h"
#include "iceberg/values_bytes.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace iceberg;

class MetadataQueryTest
  : public s3_imposter_fixture
  , public ::testing::Test {
public:
    MetadataQueryTest()
      : sr(cloud_io::scoped_remote::create(10, conf))
      , io(remote(), bucket_name) {
        set_expectations_and_listen({});
    }
    cloud_io::remote& remote() { return sr->remote.local(); }
    // Create a simple table with no snapshots.
    table_metadata create_table() {
        auto s = schema{
          .schema_struct = std::get<struct_type>(test_nested_schema_type()),
          .schema_id = schema::id_t{0},
          .identifier_field_ids = {},
        };
        chunked_vector<schema> schemas;
        schemas.emplace_back(s.copy());
        chunked_vector<partition_spec> pspecs;
        pspecs.emplace_back(partition_spec{
          .spec_id = partition_spec::id_t{0},
          .fields = {
            partition_field{
              .source_id = nested_field::id_t{2},
              .field_id = partition_field::id_t{1000},
              .name = "bar",
              .transform = identity_transform{},
            },
          },
        });
        return table_metadata{
          .format_version = format_version::v2,
          .table_uuid = uuid_t::create(),
          .location = "foo/bar",
          .last_sequence_number = sequence_number{0},
          .last_updated_ms = model::timestamp::now(),
          .last_column_id = s.highest_field_id().value(),
          .schemas = std::move(schemas),
          .current_schema_id = schema::id_t{0},
          .partition_specs = std::move(pspecs),
          .default_spec_id = partition_spec::id_t{0},
          .last_partition_id = partition_field::id_t{-1},
        };
    }

    partition_key make_int_pk(int32_t v) {
        auto pk_struct = std::make_unique<struct_value>();
        pk_struct->fields.emplace_back(int_value{v});
        return {std::move(pk_struct)};
    }

    chunked_vector<data_file> create_data_files(
      const ss::sstring& path_base,
      size_t num_files,
      size_t record_count,
      int32_t pk_value = 42) {
        chunked_vector<data_file> ret;
        ret.reserve(num_files);
        const auto records_per_file = record_count / num_files;
        const auto leftover_records = record_count % num_files;
        for (size_t i = 0; i < num_files; i++) {
            const auto path = fmt::format("{}-{}", path_base, i);
            ret.emplace_back(data_file{
              .content_type = data_file_content_type::data,
              .file_path = path,
              .partition = partition_key{make_int_pk(pk_value)},
              .record_count = records_per_file,
              .file_size_bytes = 1_KiB,
            });
        }
        ret[0].record_count += leftover_records;
        return ret;
    }
    /**
     * Generates table with 5 snapshots.
     */
    ss::future<transaction> generate_metadata() {
        transaction tx(io, create_table());
        (void)co_await tx.merge_append(create_data_files("test_1", 1, 10, 1));
        (void)co_await tx.merge_append(create_data_files("test_2", 2, 20, 2));
        (void)co_await tx.merge_append(create_data_files("test_3", 3, 30, 3));
        (void)co_await tx.merge_append(create_data_files("test_4", 2, 40, 4));
        (void)co_await tx.merge_append(create_data_files("test_5", 1, 50, 5));
        co_return tx;
    }

    ss::future<chunked_vector<iceberg::manifest_file>>
    collect_all_manifest_files(const table_metadata& table) {
        chunked_hash_set<ss::sstring> paths;
        chunked_vector<iceberg::manifest_file> files;

        for (auto& s : *table.snapshots) {
            auto m_list = co_await io.download_manifest_list_uri(
              s.manifest_list_path);
            for (auto& f : m_list.assume_value().files) {
                if (!paths.contains(f.manifest_path)) {
                    paths.emplace(f.manifest_path);
                    files.push_back(std::move(f));
                }
            }
        }
        co_return files;
    }

    partition_key_type make_partition_key_type(const table_metadata& table) {
        return partition_key_type::create(
          table.partition_specs[0], table.schemas[0]);
    }

    ss::future<chunked_vector<iceberg::manifest>>
    collect_all_manifests(const table_metadata& table) {
        chunked_vector<iceberg::manifest> manifests;
        auto files = co_await collect_all_manifest_files(table);
        for (auto& f : files) {
            auto m = co_await io.download_manifest_uri(
              f.manifest_path, make_partition_key_type(table));
            manifests.push_back(std::move(m.assume_value()));
        }
        co_return manifests;
    }

    template<result_type ResultT>
    auto execute_query(
      const table_metadata& table,
      std::optional<matcher<snapshot>> snapshot_matcher = std::nullopt,
      std::optional<matcher<manifest_file>> manifest_file_matcher
      = std::nullopt,
      std::optional<matcher<manifest>> manifest_matcher = std::nullopt) {
        metadata_query_executor executor(io, table);
        metadata_query<ResultT> q{
          .snapshot_matcher = std::move(snapshot_matcher),
          .manifest_file_matcher = std::move(manifest_file_matcher),
          .manifest_matcher = std::move(manifest_matcher)};
        auto r = executor.execute_query(q).get();

        return std::move(r.assume_value());
    }

    auto execute_for_all_results(
      const table_metadata& table,
      const std::optional<std::function<bool(const snapshot&)>>&
        snapshot_matcher
      = std::nullopt,
      const std::optional<std::function<bool(const manifest_file&)>>&
        manifest_file_matcher
      = std::nullopt,
      const std::optional<std::function<bool(const manifest&)>>&
        manifest_matcher
      = std::nullopt) {
        metadata_query_executor executor(io, table);
        metadata_query<result_type::snapshot> q_s{
          .snapshot_matcher = snapshot_matcher,
          .manifest_file_matcher = manifest_file_matcher,
          .manifest_matcher = manifest_matcher};
        metadata_query<result_type::manifest_file> q_mf{
          .snapshot_matcher = snapshot_matcher,
          .manifest_file_matcher = manifest_file_matcher,
          .manifest_matcher = manifest_matcher};
        metadata_query<result_type::manifest> q_m{
          .snapshot_matcher = snapshot_matcher,
          .manifest_file_matcher = manifest_file_matcher,
          .manifest_matcher = manifest_matcher};

        auto r_s = executor.execute_query(q_s).get();
        auto r_mf = executor.execute_query(q_mf).get();
        auto r_m = executor.execute_query(q_m).get();

        return std::make_tuple(
          std::move(r_s.assume_value()),
          std::move(r_mf.assume_value()),
          std::move(r_m.assume_value()));
    }

    std::unique_ptr<cloud_io::scoped_remote> sr;
    manifest_io io;
};

using namespace testing;
TEST_F(MetadataQueryTest, TestEmptyQueryMatchesEverything) {
    auto tx = generate_metadata().get();
    auto [snapshots, files, manifests] = execute_for_all_results(tx.table());
    ASSERT_EQ(snapshots.size(), 5);
    // all snapshots should be present in the result
    ASSERT_EQ(snapshots, tx.table().snapshots);

    // all manifest_files should be present in result
    ASSERT_EQ(files, collect_all_manifest_files(tx.table()).get());

    // all manifests should be present in result
    auto all = collect_all_manifests(tx.table()).get();
    ASSERT_EQ(manifests, collect_all_manifests(tx.table()).get());
}

bool is_even(int64_t v) { return v % 2 == 0; }

MATCHER_P(EqRef, expected_value, "") { return arg.value == expected_value; }
TEST_F(MetadataQueryTest, TestQuerySnapshots) {
    auto tx = generate_metadata().get();
    // snapshot query, only those snapshots which has even sequence number,
    // those will be snapshot 2 and 4
    auto s_matcher = [](const snapshot& s) {
        return is_even(s.sequence_number());
    };

    auto [snapshots, files, manifests] = execute_for_all_results(
      tx.table(), s_matcher);
    ASSERT_EQ(snapshots.size(), 2);
    EXPECT_THAT(
      snapshots, Each(Field(&snapshot::sequence_number, Truly(is_even))));

    auto all_manifest_files = collect_all_manifest_files(tx.table()).get();

    /**
     * The ordering here is counter intuitive.
     * Table metadata contains 5 snapshots [s1,s2,s3,s4,s5] each snapshot
     * contains metadata files
     *
     * s1 - [f0]
     * s2 - [f1,f0]
     * s3 - [f2,f1,f0]
     * s4 - [f3,f2,f1,f0]
     * s5 - [f4,f3,f2,f1,f0]
     *
     * When test util function collects all files/manifests it traverses the
     * snapshot list and deduplicate the files. The result of this operation is:
     * [f0,f1,f2,f3,f4]
     *
     * When query is executed with is even segment sequence it matches s2 and s4
     * from s2 it takes
     *
     * [f1, f0] and then from s4 [f3,f2]
     *
     * So the end result is [f1,f0,f3,f2].
     */
    ASSERT_EQ(files.size(), 4);
    ASSERT_EQ(files[0], all_manifest_files[1]);
    ASSERT_EQ(files[1], all_manifest_files[0]);
    ASSERT_EQ(files[2], all_manifest_files[3]);
    ASSERT_EQ(files[3], all_manifest_files[2]);

    // the same ordering rule applies to the manifests
    auto all_manifests = collect_all_manifests(tx.table()).get();
    ASSERT_EQ(manifests.size(), 4);
    ASSERT_EQ(manifests[0], all_manifests[1]);
    ASSERT_EQ(manifests[1], all_manifests[0]);
    ASSERT_EQ(manifests[2], all_manifests[3]);
    ASSERT_EQ(manifests[3], all_manifests[2]);
}

TEST_F(MetadataQueryTest, TestQueryManifestFile) {
    auto tx = generate_metadata().get();
    // manifest file query, only those manifest files which partition key
    // lower bound is greater or equal than 3
    auto matcher = [](const manifest_file& file) {
        auto value = value_from_bytes(
          int_type{}, file.partitions[0].lower_bound.value());
        return std::get<int_value>(std::get<primitive_value>(value)).val >= 3;
    };

    auto [snapshots, files, manifests] = execute_for_all_results(
      tx.table(), std::nullopt, matcher);

    // it should only contain snapshots 3,4,5
    ASSERT_EQ(snapshots.size(), 3);
    EXPECT_THAT(
      snapshots,
      Each(Field(&snapshot::sequence_number, Ge(sequence_number(3)))));

    auto all_manifest_files = collect_all_manifest_files(tx.table()).get();

    ASSERT_EQ(files.size(), 3);
    ASSERT_EQ(files[0], all_manifest_files[2]);
    ASSERT_EQ(files[1], all_manifest_files[3]);
    ASSERT_EQ(files[2], all_manifest_files[4]);

    auto all_manifests = collect_all_manifests(tx.table()).get();
    ASSERT_EQ(manifests.size(), 3);
    ASSERT_EQ(manifests[0], all_manifests[2]);
    ASSERT_EQ(manifests[1], all_manifests[3]);
    ASSERT_EQ(manifests[2], all_manifests[4]);
}

TEST_F(MetadataQueryTest, TestQueryManifests) {
    auto tx = generate_metadata().get();

    auto matcher = [](const manifest& m) { return m.entries.size() == 3; };

    auto [snapshots, files, manifests] = execute_for_all_results(
      tx.table(), std::nullopt, std::nullopt, matcher);
    // it should only contain 3 snapshots as they contain manifest we are
    // looking for
    ASSERT_EQ(snapshots.size(), 3);
    ASSERT_EQ(snapshots[0], (*tx.table().snapshots)[2]);
    ASSERT_EQ(snapshots[1], (*tx.table().snapshots)[3]);
    ASSERT_EQ(snapshots[2], (*tx.table().snapshots)[4]);

    auto all_manifest_files = collect_all_manifest_files(tx.table()).get();

    // only one manifest has 3 entries, we deduplicate the result so here
    // there should be only one element
    ASSERT_EQ(files.size(), 1);
    ASSERT_EQ(files[0], all_manifest_files[2]);

    auto all_manifests = collect_all_manifests(tx.table()).get();
    ASSERT_EQ(manifests.size(), 1);
    ASSERT_EQ(manifests[0], all_manifests[2]);
}

template<typename TupleT, std::size_t... Is>
AssertionResult do_check_query_result_is_empty(
  const metadata_query_executor& executor,
  const TupleT& tp,
  std::index_sequence<Is...>) {
    auto query = [&executor](const auto& query) {
        auto r = executor.execute_query(query).get();

        return r.has_value() && r.value().empty();
    };
    bool success = (query(std::get<Is>(tp)) && ...);
    if (!success) {
        return AssertionFailure() << "Queries should return empty results";
    }
    return AssertionSuccess();
}

template<typename TupleT, std::size_t tuple_size = std::tuple_size_v<TupleT>>
AssertionResult check_query_result_is_empty(
  const metadata_query_executor& executor, const TupleT& tp) {
    return do_check_query_result_is_empty(
      executor, tp, std::make_index_sequence<std::tuple_size_v<TupleT>>{});
}

template<typename EntityT>
bool match_none(const EntityT&) {
    return false;
}

TEST_F(MetadataQueryTest, TestQueryMatchingNone) {
    auto tx = generate_metadata().get();

    auto queries = std::make_tuple(
      metadata_query<result_type::snapshot>{
        .snapshot_matcher = match_none<snapshot>},
      metadata_query<result_type::snapshot>{
        .manifest_file_matcher = match_none<manifest_file>},
      metadata_query<result_type::snapshot>{
        .manifest_matcher = match_none<manifest>},
      metadata_query<result_type::manifest_file>{
        .snapshot_matcher = match_none<snapshot>},
      metadata_query<result_type::manifest_file>{
        .manifest_file_matcher = match_none<manifest_file>},
      metadata_query<result_type::manifest_file>{
        .manifest_matcher = match_none<manifest>},
      metadata_query<result_type::manifest>{
        .snapshot_matcher = match_none<snapshot>},
      metadata_query<result_type::manifest>{
        .manifest_file_matcher = match_none<manifest_file>},
      metadata_query<result_type::manifest>{
        .manifest_matcher = match_none<manifest>});

    metadata_query_executor executor(io, tx.table());
    EXPECT_TRUE(check_query_result_is_empty(executor, queries));
}

TEST_F(MetadataQueryTest, TestCombinedQuery) {
    auto tx = generate_metadata().get();
    // match snapshot 5
    auto s_matcher = [](const snapshot& s) {
        return s.sequence_number == sequence_number(5);
    };
    auto mf_matcher = [](const manifest_file& mf) {
        return mf.added_rows_count == 30;
    };
    auto [e_s, e_mf, e_m] = execute_for_all_results(
      tx.table(), s_matcher, match_none<manifest_file>);
    ASSERT_TRUE(e_s.empty());
    ASSERT_TRUE(e_mf.empty());
    ASSERT_TRUE(e_m.empty());

    auto [snapshots, files, manifests] = execute_for_all_results(
      tx.table(), s_matcher, mf_matcher);
    ASSERT_EQ(snapshots.size(), 1);
    ASSERT_EQ(snapshots[0], tx.table().snapshots->at(4));
    ASSERT_EQ(files.size(), 1);
    auto all_manifest_files = collect_all_manifest_files(tx.table()).get();
    ASSERT_EQ(files[0], all_manifest_files[2]);

    ASSERT_EQ(manifests.size(), 1);
    auto all_manifest = collect_all_manifests(tx.table()).get();
    ASSERT_EQ(manifests[0], all_manifest[2]);
}
