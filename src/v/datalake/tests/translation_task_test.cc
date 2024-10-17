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
#include "datalake/cloud_data_io.h"
#include "datalake/translation_task.h"
#include "model/record_batch_reader.h"
#include "storage/record_batch_builder.h"
#include "test_utils/tmp_dir.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace std::chrono_literals;
using namespace testing;
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

    std::unique_ptr<datalake::data_writer_factory> get_writer_factory(
      size_t row_threshold = 200, size_t bytes_threshold = 4096) {
        return std::make_unique<datalake::batching_parquet_writer_factory>(
          datalake::local_path(tmp_dir.get_path()),
          "test-prefix",
          row_threshold,
          bytes_threshold);
    }

    lazy_abort_source never_abort() {
        return {[]() { return std::nullopt; }};
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

    ss::abort_source as;
    temporary_dir tmp_dir;
    retry_chain_node test_rcn;
    datalake::cloud_data_io cloud_io;
};

TEST_F(TranslateTaskTest, TestHappyPathTranslation) {
    datalake::translation_task task(cloud_io);

    auto result = task
                    .translate(
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
}
