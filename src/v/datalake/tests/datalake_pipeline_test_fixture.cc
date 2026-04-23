/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/tests/datalake_pipeline_test_fixture.h"

#include "cloud_io/provider.h"
#include "config/property.h"
#include "datalake/local_parquet_file_writer.h"
#include "datalake/location.h"
#include "datalake/table_definition.h"
#include "datalake/tests/e2e_test_utils.h"
#include "datalake/tests/iceberg_table_reader.h"
#include "iceberg/table_identifier.h"

namespace datalake::tests {

namespace {
using namespace std::chrono_literals;
const model::cluster_uuid cluster_uuid{uuid_t::create()};

storage::api
dummy_storage(ss::sharded<features::feature_table>& feature_table) {
    return storage::api{
      []() {
          return storage::kvstore_config(
            1_MiB,
            config::mock_binding(10ms),
            "dummy.dir",
            storage::make_sanitized_file_config());
      },
      []() { return storage::log_config("dummy.dir", 1_GiB); },
      feature_table};
}
} // namespace

datalake_pipeline_test_fixture::datalake_pipeline_test_fixture()
  : storage_(dummy_storage(feature_table_))
  , schema_mgr_(
      catalog,
      [this] {
          feature_table_.start().get();
          feature_table_
            .invoke_on_all(
              [](features::feature_table& f) { f.testing_activate_all(); })
            .get();
          return &feature_table_.local();
      }())
  , manifest_io_(scoped_remote->remote.local(), bucket_name)
  , committer_(storage_, catalog, manifest_io_, config::mock_binding(false)) {
    storage_.start().get();
    storage_.set_cluster_uuid(cluster_uuid);
}

void datalake_pipeline_test_fixture::TearDown() {
    storage_.stop().get();
    feature_table_.stop().get();
}

record_multiplexer datalake_pipeline_test_fixture::make_multiplexer(
  const model::ntp& ntp, model::revision_id rev, model::iceberg_mode mode) {
    type_resolver* resolver = &bin_resolver_;
    record_translator* translator = &kv_translator_;

    using v = model::iceberg_mode::variant;
    switch (mode.kind()) {
    case v::key_value:
        translator = &kv_translator_;
        table_creator_ = std::make_unique<direct_table_creator>(
          bin_resolver_, schema_mgr_);
        break;
    case v::cdc_key_value:
        translator = &cdc_kv_translator_;
        table_creator_ = std::make_unique<direct_table_creator>(
          bin_resolver_, schema_mgr_, identity_key_partition_spec());
        break;
    default:
        throw std::runtime_error(
          fmt::format("Unsupported iceberg mode for pipeline test: {}", mode));
    }
    probe_ = std::make_unique<translation_probe>(ntp);

    auto tmp_dir = std::filesystem::temp_directory_path() / "datalake-e2e-test";
    std::filesystem::create_directories(tmp_dir);

    auto ostream_factory = ss::make_shared<serde_parquet_writer_factory>();
    auto factory = std::make_unique<local_parquet_file_writer_factory>(
      local_path(tmp_dir), "test", ostream_factory, mem_tracker_);

    return record_multiplexer(
      ntp,
      rev,
      std::move(factory),
      schema_mgr_,
      *resolver,
      *translator,
      *table_creator_,
      model::iceberg_invalid_record_action::dlq_table,
      location_provider(
        cloud_io::s3_compat_provider{"s3"},
        cloud_storage_clients::bucket_name{bucket_name}),
      *probe_,
      &feature_table_.local());
}

ss::future<chunked_vector<serde::parquet::group_value>>
datalake_pipeline_test_fixture::commit_and_read(
  const model::topic& topic,
  record_multiplexer::write_result result,
  record_multiplexer::finished_files files) {
    auto commit_res = co_await commit_finished_files(topic, result, files);
    if (commit_res.has_error()) {
        throw std::runtime_error("Commit failed");
    }
    co_return co_await read_committed_table(topic);
}

ss::future<checked<
  chunked_vector<coordinator::mark_files_committed_update>,
  coordinator::file_committer::errc>>
datalake_pipeline_test_fixture::commit_finished_files(
  const model::topic& topic,
  const record_multiplexer::write_result& result,
  const record_multiplexer::finished_files& files) {
    auto tor = co_await upload_and_convert(manifest_io_, result, files);

    coordinator::topics_state state;
    coordinator::topic_state t_state;
    t_state.revision = model::revision_id{1};
    coordinator::partition_state p_state;
    p_state.pending_entries.push_back(
      coordinator::pending_entry{
        .data = std::move(tor),
        .added_pending_at = model::offset{0},
      });
    t_state.pid_to_pending_files.emplace(
      model::partition_id{0}, std::move(p_state));
    state.topic_to_state.emplace(topic, std::move(t_state));

    co_return co_await committer_.commit_topic_files_to_catalog(topic, state);
}

ss::future<chunked_vector<serde::parquet::group_value>>
datalake_pipeline_test_fixture::read_committed_table(
  const model::topic& topic) {
    auto table_id = iceberg::table_identifier{
      .ns = {"redpanda"}, .table = topic};
    auto load_res = co_await catalog.load_table(table_id);
    if (load_res.has_error()) {
        throw std::runtime_error("Failed to load table");
    }
    co_return co_await read_iceberg_table(manifest_io_, load_res.value());
}

} // namespace datalake::tests
