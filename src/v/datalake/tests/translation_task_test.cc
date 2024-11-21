/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/tests/scoped_remote.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "datalake/batching_parquet_writer.h"
#include "datalake/catalog_schema_manager.h"
#include "datalake/cloud_data_io.h"
#include "datalake/local_parquet_file_writer.h"
#include "datalake/record_schema_resolver.h"
#include "datalake/record_translator.h"
#include "datalake/table_creator.h"
#include "datalake/translation_task.h"
#include "model/record_batch_reader.h"
#include "storage/record_batch_builder.h"
#include "test_utils/tmp_dir.h"

#include <seastar/util/defer.hh>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace std::chrono_literals;
using namespace testing;
using namespace datalake;
namespace {
auto schema_mgr = std::make_unique<simple_schema_manager>();
auto schema_resolver = std::make_unique<binary_type_resolver>();
auto translator = std::make_unique<default_translator>();
auto t_creator = std::make_unique<direct_table_creator>(
  *schema_resolver, *schema_mgr);
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
      , cloud_io(sr->remote.local(), bucket_name) {
        set_expectations_and_listen({});
    }

    auto& remote() { return sr->remote.local(); }

    std::unique_ptr<cloud_io::scoped_remote> sr;

    model::record_batch_reader make_batches(
      int64_t batch_count,
      int64_t records_per_batch,
      model::offset start_offset = model::offset{0}) {
        ss::circular_buffer<model::record_batch> batches;
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

    std::unique_ptr<datalake::parquet_file_writer_factory> get_writer_factory(
      size_t row_threshold = 200, size_t bytes_threshold = 4096) {
        return std::make_unique<datalake::local_parquet_file_writer_factory>(
          datalake::local_path(tmp_dir.get_path()),
          "test-prefix",
          ss::make_shared<datalake::batching_parquet_writer_factory>(
            row_threshold, bytes_threshold));
    }

    lazy_abort_source& never_abort() {
        static thread_local lazy_abort_source noop = {
          []() { return std::nullopt; }};
        return noop;
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

    ss::abort_source as;
    temporary_dir tmp_dir;
    retry_chain_node test_rcn;
    datalake::cloud_data_io cloud_io;
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
    datalake::translation_task task(
      cloud_io, *schema_mgr, *schema_resolver, *translator, *t_creator);

    auto result = task
                    .translate(
                      ntp,
                      rev,
                      get_writer_factory(),
                      make_batches(10, 16),
                      datalake::remote_path("test/location/1"),
                      test_rcn,
                      never_abort())
                    .get();

    ASSERT_FALSE(result.has_error());

    auto transformed_range = std::move(result.value());
    // check offset range
    ASSERT_EQ(transformed_range.start_offset, kafka::offset(0));
    ASSERT_EQ(transformed_range.last_offset, kafka::offset(159));
    ASSERT_EQ(transformed_range.files.size(), 1);

    // check that the resulting files were actually uploaded to the cloud
    check_object_store_content(remote_paths(transformed_range.files));
    // check that all local files has been deleted
    ASSERT_THAT(list_data_files().get(), IsEmpty());
}

TEST_F(TranslateTaskTest, TestDataFileMissing) {
    datalake::translation_task task(
      cloud_io, *schema_mgr, *schema_resolver, *translator, *t_creator);
    // create deleting task to cause local io error
    deleter del(tmp_dir.get_path().string());
    del.start();
    auto stop_deleter = ss::defer([&del] { del.stop().get(); });
    auto result = task
                    .translate(
                      ntp,
                      rev,
                      get_writer_factory(),
                      make_batches(10, 16),
                      datalake::remote_path("test/location/1"),
                      test_rcn,
                      never_abort())
                    .get();

    ASSERT_TRUE(result.has_error());
    ASSERT_EQ(result.error(), datalake::translation_task::errc::file_io_error);
}

TEST_F(TranslateTaskTest, TestUploadError) {
    datalake::translation_task task(
      cloud_io, *schema_mgr, *schema_resolver, *translator, *t_creator);
    // fail all PUT requests
    fail_request_if(
      [](const http_test_utils::request_info& req) -> bool {
          return req.method == "PUT";
      },
      http_test_utils::response{
        .body = "failed!",
        .status = ss::http::reply::status_type::internal_server_error});

    auto result = task
                    .translate(
                      ntp,
                      model::revision_id{123},
                      get_writer_factory(),
                      make_batches(10, 16),
                      datalake::remote_path("test/location/1"),
                      test_rcn,
                      never_abort())
                    .get();

    ASSERT_TRUE(result.has_error());
    ASSERT_EQ(result.error(), datalake::translation_task::errc::cloud_io_error);
    // check no data files are left behind
    ASSERT_THAT(list_data_files().get(), IsEmpty());
}
// TODO: add more sophisticated test cases when multiplexer will be capable of
// creating more than one data file from single run.
