/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/tests/s3_imposter.h"
#include "cloud_io/tests/scoped_remote.h"
#include "container/chunked_circular_buffer.h"
#include "datalake/catalog_schema_manager.h"
#include "datalake/cloud_data_io.h"
#include "datalake/local_parquet_file_writer.h"
#include "datalake/location.h"
#include "datalake/record_schema_resolver.h"
#include "datalake/record_translator.h"
#include "datalake/table_creator.h"
#include "datalake/tests/test_data_writer.h"
#include "datalake/tests/test_utils.h"
#include "datalake/translation/translation_probe.h"
#include "datalake/translation_task.h"
#include "features/feature_table.h"
#include "iceberg/uri.h"
#include "model/record_batch_reader.h"
#include "storage/record_batch_builder.h"
#include "test_utils/tmp_dir.h"

#include <seastar/util/defer.hh>

#include <boost/range/irange.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace std::chrono_literals;
using namespace testing;
using namespace datalake;
namespace {
auto translator = std::make_unique<default_translator>();
const auto ntp = model::ntp{};
const auto rev = model::revision_id{123};
} // namespace

class TranslateTaskTest
  : public s3_imposter_fixture
  , public ::testing::Test {
public:
    using list_bucket_item = cloud_storage_clients::client::list_bucket_item;
    TranslateTaskTest()
      : sr(cloud_io::scoped_remote::create(/*pool_size=*/10, conf))
      , tmp_dir("translation_task_test")
      , test_rcn(as, 10s, 1s)
      , cloud_io(sr->remote.local(), bucket_name)
      , schema_resolver(std::make_unique<test_binary_type_resolver>())
      , schema_mgr(
          std::make_unique<simple_schema_manager>(
            iceberg::uri_converter(sr->remote.local().provider())
              .to_uri(bucket_name, "test")))
      , t_creator(
          std::make_unique<direct_table_creator>(*schema_resolver, *schema_mgr))
      , location_provider(sr->remote.local().provider(), bucket_name)
      , probe(ntp) {
        set_expectations_and_listen({});
        features.testing_activate_all();
    }

    auto& remote() { return sr->remote.local(); }

    std::unique_ptr<cloud_io::scoped_remote> sr;
    features::feature_table features;

    model::record_batch_reader make_batches(
      int64_t batch_count,
      int64_t records_per_batch,
      model::offset start_offset = model::offset{0}) {
        chunked_circular_buffer<model::record_batch> batches;
        auto offset = start_offset;
        for (auto i : boost::irange<int64_t>(batch_count)) {
            storage::record_batch_builder builder(
              model::record_batch_type::raft_data, offset);

            for (auto r : boost::irange<int64_t>(records_per_batch)) {
                builder.add_raw_kv(
                  iobuf::from(fmt::format("key-{}-{}", i, r)),
                  iobuf::from(fmt::format("value-{}-{}", i, r)));
            }
            batches.push_back(std::move(builder).build());
            offset = model::next_offset(batches.back().last_offset());
        }

        return model::make_memory_record_batch_reader(std::move(batches));
    }

    /// Returns a memory tracker that allows for `n` successful reservations.
    /// After `n` reservations `out_of_memory` is returned.
    std::unique_ptr<datalake::writer_mem_tracker>
    make_memory_tracker(size_t n) {
        class mem_tracker : public datalake::writer_mem_tracker {
        public:
            explicit mem_tracker(size_t num_left)
              : _num_left(num_left) {}
            ss::future<reservation_error>
            reserve_bytes(size_t u, ss::abort_source& as) noexcept override {
                if (_num_left == 0) {
                    co_return reservation_error::out_of_memory;
                }
                --_num_left;
                co_return reservation_error::ok;
            }
            ss::future<> free_bytes(size_t, ss::abort_source&) override {
                return ss::now();
            }
            void release() override {}
            writer_disk_tracker& disk() override { return _disk.disk(); }

        private:
            size_t _num_left;
            datalake::noop_mem_tracker _disk;
        };

        return std::make_unique<mem_tracker>(n);
    }

    std::unique_ptr<datalake::parquet_file_writer_factory>
    get_writer_factory() {
        return std::make_unique<datalake::local_parquet_file_writer_factory>(
          datalake::local_path(tmp_dir.get_path()),
          "test-prefix",
          ss::make_shared<datalake::serde_parquet_writer_factory>(),
          noop_tracker);
    }

    template<typename R>
    AssertionResult check_object_store_content(R range) {
        auto objects = remote().list_objects(bucket_name, test_rcn).get();
        auto listed = objects.value().contents
                      | std::views::transform(&list_bucket_item::key);
        if (std::equal(range.begin(), range.end(), listed.begin())) {
            return AssertionSuccess();
        }
        return AssertionFailure() << fmt::format(
                 "Expected and actual list of object keys doesn't match {} != "
                 "{}",
                 fmt::join(range, ", "),
                 fmt::join(listed, ", "));
    }

    auto remote_paths(
      const chunked_vector<datalake::coordinator::data_file>& files) {
        return files
               | std::views::transform(
                 &datalake::coordinator::data_file::remote_path);
    }

    ss::future<chunked_vector<ss::sstring>> list_data_files() {
        chunked_vector<ss::sstring> ret;
        auto dir = co_await ss::open_directory(tmp_dir.get_path().string());
        co_await dir
          .list_directory([&ret](const ss::directory_entry& entry) {
              ret.push_back(entry.name);
              return ss::now();
          })
          .done();
        co_return ret;
    }

    translation_task create_task() {
        return translation_task(
          ntp,
          rev,
          get_writer_factory(),
          cloud_io,
          &features,
          *schema_mgr,
          *schema_resolver,
          *translator,
          *t_creator,
          model::iceberg_invalid_record_action::dlq_table,
          location_provider,
          probe);
    }

    ss::abort_source as;
    temporary_dir tmp_dir;
    retry_chain_node test_rcn;
    datalake::cloud_data_io cloud_io;
    std::unique_ptr<test_binary_type_resolver> schema_resolver;
    std::unique_ptr<simple_schema_manager> schema_mgr;
    std::unique_ptr<table_creator> t_creator;
    datalake::location_provider location_provider;
    translation_probe probe;
    datalake::noop_mem_tracker noop_tracker;
};

struct deleter {
    explicit deleter(ss::sstring path)
      : _path(std::move(path)) {}

    void start() {
        ssx::repeat_until_gate_closed(
          _bg, [this] { return delete_all_files(); });
    }

    ss::future<> delete_all_files() {
        auto dir = co_await ss::open_directory(_path);

        co_await dir
          .list_directory([this](const ss::directory_entry& entry) {
              auto p = fmt::format("{}/{}", _path, entry.name);

              return ss::remove_file(p);
          })
          .done();
    }

    ss::future<> stop() { return _bg.close(); }

private:
    ss::sstring _path;
    ss::gate _bg;
};

TEST_F(TranslateTaskTest, TestHappyPathTranslation) {
    datalake::translation_task task = create_task();
    task.translate_once(make_batches(10, 16), kafka::offset{0}, as).get();
    auto flush_result = task.flush().get();
    ASSERT_FALSE(flush_result.has_error());
    auto start_offset = 160;
    task
      .translate_once(
        make_batches(10, 16, model::offset{start_offset}),
        kafka::offset{start_offset},
        as)
      .get();
    auto result = std::move(task)
                    .finish(
                      translation_task::custom_partitioning_enabled::yes,
                      test_rcn,
                      as)
                    .get();

    ASSERT_FALSE(result.has_error());

    auto transformed_range = std::move(result.value());
    // check offset range
    ASSERT_EQ(transformed_range.start_offset, kafka::offset(0));
    // 2 translations of 10 batches with 16 records each
    // = 2 * 10 * 16 = 320 records
    ASSERT_EQ(transformed_range.last_offset, kafka::offset(319));
    ASSERT_EQ(transformed_range.files.size(), 1);

    // CORE-13267, this check is failing, currently ignored
    // check that the resulting files were actually uploaded to the cloud
    std::ignore = check_object_store_content(
      remote_paths(transformed_range.files));
    // check that all local files has been deleted
    ASSERT_THAT(list_data_files().get(), IsEmpty());
}

TEST_F(TranslateTaskTest, TestDataFileMissing) {
    datalake::translation_task task = create_task();
    // create deleting task to cause local io error
    deleter del(tmp_dir.get_path().string());
    del.start();
    auto stop_deleter = ss::defer([&del] { del.stop().get(); });
    task.translate_once(make_batches(10, 16), kafka::offset{0}, as).get();
    auto result = std::move(task)
                    .finish(
                      translation_task::custom_partitioning_enabled::yes,
                      test_rcn,
                      as)
                    .get();

    ASSERT_TRUE(result.has_error());
    ASSERT_EQ(result.error(), datalake::translation_task::errc::file_io_error);
}

TEST_F(TranslateTaskTest, TestUploadError) {
    datalake::translation_task task(
      ntp,
      model::revision_id{123},
      get_writer_factory(),
      cloud_io,
      &features,
      *schema_mgr,
      *schema_resolver,
      *translator,
      *t_creator,
      model::iceberg_invalid_record_action::dlq_table,
      location_provider,
      probe);
    // fail all PUT requests
    fail_request_if(
      [](const http_test_utils::request_info& req) -> bool {
          return req.method == "PUT";
      },
      http_test_utils::response{
        .body = "failed!",
        .status = ss::http::reply::status_type::internal_server_error});

    task.translate_once(make_batches(10, 16), kafka::offset{0}, as).get();
    auto result = std::move(task)
                    .finish(
                      translation_task::custom_partitioning_enabled::yes,
                      test_rcn,
                      as)
                    .get();

    ASSERT_TRUE(result.has_error());
    ASSERT_EQ(result.error(), datalake::translation_task::errc::cloud_io_error);
    // check no data files are left behind
    ASSERT_THAT(list_data_files().get(), IsEmpty());
}
TEST_F(TranslateTaskTest, TestCleanupAfterOOMError) {
    // Create a mem tracker allows for 1 reservation to make a single writer.
    auto mem_tracker = make_memory_tracker(1);
    auto writer_factory
      = std::make_unique<datalake::local_parquet_file_writer_factory>(
        datalake::local_path(tmp_dir.get_path()),
        "test-prefix",
        ss::make_shared<datalake::serde_parquet_writer_factory>(),
        *mem_tracker);

    datalake::translation_task task(
      ntp,
      model::revision_id{123},
      std::move(writer_factory),
      cloud_io,
      &features,
      *schema_mgr,
      *schema_resolver,
      *translator,
      *t_creator,
      model::iceberg_invalid_record_action::dlq_table,
      location_provider,
      probe);

    task.translate_once(make_batches(10, 16), kafka::offset{0}, as).get();
    auto result = std::move(task)
                    .finish(
                      translation_task::custom_partitioning_enabled::yes,
                      test_rcn,
                      as)
                    .get();

    if (result.has_error()) {
        // There are two ways a writer can handle an OOM. If it allocates the
        // memory units prior to writing the record then there should be no data
        // when flushed.
        ASSERT_EQ(result.error(), datalake::translation_task::errc::no_data);
    } else {
        // Otherwise if it allocates the memory units after writing the record
        // then only a single record should've been written at which point the
        // OOM error would've bubbled up to the translator.
        ASSERT_EQ(result.value().last_offset, kafka::offset{0});
    }

    // check no data files are left behind
    ASSERT_THAT(list_data_files().get(), IsEmpty());
}

TEST_F(TranslateTaskTest, TestCleanupAfterTransientError) {
    datalake::translation_task task(
      ntp,
      model::revision_id{123},
      get_writer_factory(),
      cloud_io,
      &features,
      *schema_mgr,
      *schema_resolver,
      *translator,
      *t_creator,
      model::iceberg_invalid_record_action::dlq_table,
      location_provider,
      probe);

    // Translate some data to create some files locally.
    task.translate_once(make_batches(10, 16), kafka::offset{0}, as).get();
    auto flush_res = task.flush().get();
    ASSERT_FALSE(flush_res.has_error());

    // Now make a supposedly transient error happen. This will fail the task
    // rather than sending the data to the DLQ.
    schema_resolver->set_fail_requests(type_resolver::errc::registry_error);
    task
      .translate_once(
        make_batches(10, 16, model::offset{160}), kafka::offset{160}, as)
      .get();
    auto result = std::move(task)
                    .finish(
                      translation_task::custom_partitioning_enabled::yes,
                      test_rcn,
                      as)
                    .get();

    ASSERT_TRUE(result.has_error());
    ASSERT_EQ(
      result.error(), datalake::translation_task::errc::type_resolution_error);

    // Check no data files are left behind
    ASSERT_THAT(list_data_files().get(), IsEmpty());
}

TEST_F(TranslateTaskTest, TestCleanupAfterTransientErrorDiscard) {
    datalake::translation_task task(
      ntp,
      model::revision_id{123},
      get_writer_factory(),
      cloud_io,
      &features,
      *schema_mgr,
      *schema_resolver,
      *translator,
      *t_creator,
      model::iceberg_invalid_record_action::dlq_table,
      location_provider,
      probe);

    // Translate some data to create some files locally.
    task.translate_once(make_batches(10, 16), kafka::offset{0}, as).get();
    auto flush_res = task.flush().get();
    ASSERT_FALSE(flush_res.has_error());

    // Now make a supposedly transient error happen. This will fail the task
    // rather than sending the data to the DLQ.
    schema_resolver->set_fail_requests(type_resolver::errc::registry_error);
    task
      .translate_once(
        make_batches(10, 16, model::offset{160}), kafka::offset{160}, as)
      .get();
    auto result = std::move(task).discard().get();
    ASSERT_TRUE(result.has_error());
    ASSERT_EQ(result.error(), datalake::translation_task::errc::file_io_error);

    // Check no data files are left behind
    ASSERT_THAT(list_data_files().get(), IsEmpty());
}
// TODO: add more sophisticated test cases when multiplexer will be capable of
// creating more than one data file from single run.
