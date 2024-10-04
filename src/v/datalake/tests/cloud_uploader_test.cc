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
#include "data_file.h"
#include "datalake/cloud_uploader.h"
#include "datalake/data_writer_interface.h"
#include "model/fundamental.h"
#include "test_utils/tmp_dir.h"
#include "utils/lazy_abort_source.h"
#include "utils/retry_chain_node.h"

#include <gtest/gtest.h>

class DatalakeCloudUploaderTest
  : public s3_imposter_fixture
  , public ::testing::Test {
public:
    DatalakeCloudUploaderTest()
      : sr(cloud_io::scoped_remote::create(/*pool_size=*/10, conf)) {
        set_expectations_and_listen({});
    }

    auto& remote() { return sr->remote.local(); }

    std::unique_ptr<cloud_io::scoped_remote> sr;
};

TEST_F(DatalakeCloudUploaderTest, UploadsData) {
    using namespace std::chrono_literals;

    // Write out a test file
    temporary_dir tmp_dir("datalake_cloud_uploader_test");
    std::filesystem::path file_path = tmp_dir.get_path() / "test_file";
    std::string contents = "Hello world!";
    std::ofstream ofile(file_path);
    ofile << contents;
    ofile.close();

    // Upload it
    datalake::cloud_uploader uploader(
      remote(),
      cloud_storage_clients::bucket_name{"test-bucket"},
      "remote_test/remote_directory");
    ss::abort_source never_abort;
    retry_chain_node retry(never_abort, 10s, 1s);
    lazy_abort_source always_continue{[]() { return std::nullopt; }};

    datalake::coordinator::data_file local_file{
      .remote_path = file_path.c_str(),
      .row_count = 1,
      .file_size_bytes = contents.size(),
    };

    auto cloud_result
      = uploader
          .upload_data_file(
            local_file, "remote_test_file", retry, always_continue)
          .get0();

    EXPECT_TRUE(cloud_result.has_value());
    EXPECT_EQ(
      cloud_result.value().remote_path,
      "s3://test-bucket/remote_test/remote_directory/remote_test_file");
    EXPECT_EQ(cloud_result.value().file_size_bytes, local_file.file_size_bytes);
    EXPECT_EQ(cloud_result.value().row_count, local_file.row_count);
}

TEST_F(DatalakeCloudUploaderTest, HandlesMissingFiles) {
    using namespace std::chrono_literals;

    // Don't write out a test file
    temporary_dir tmp_dir("datalake_cloud_uploader_test");
    std::filesystem::path file_path = tmp_dir.get_path() / "test_file";

    // Upload it
    datalake::cloud_uploader uploader(
      remote(),
      cloud_storage_clients::bucket_name{"test-bucket"},

      "test/directory");
    ss::abort_source never_abort;
    retry_chain_node retry(never_abort, 10s, 1s);
    lazy_abort_source always_continue{[]() { return std::nullopt; }};

    datalake::coordinator::data_file local_file{
      .remote_path = file_path.c_str(),
      .row_count = 1,
      .file_size_bytes = 100,
    };

    auto cloud_result
      = uploader
          .upload_data_file(
            local_file, "remote_test_file", retry, always_continue)
          .get0();

    EXPECT_TRUE(cloud_result.has_error());
}
