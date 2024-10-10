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

datalake::local_data_file make_test_data_file(
  std::filesystem::path directory,
  std::string contents,
  std::string file_name) {
    std::ofstream ofile(directory / file_name);
    ofile << contents;
    ofile.close();

    return datalake::local_data_file{
      .base_path = directory.c_str(),
      .file_path = file_name,
      .row_count = 1,
      .file_size_bytes = contents.size(),
    };
}

// Make a local_data_file_result pointing to a non-existent file.
datalake::local_data_file make_bad_file(
  std::filesystem::path directory,
  std::string contents,
  std::string file_name) {
    return datalake::local_data_file{
      .base_path = directory.c_str(),
      .file_path = file_name,
      .row_count = 1,
      .file_size_bytes = contents.size(),
    };
}

TEST_F(DatalakeCloudUploaderTest, UploadsData) {
    using namespace std::chrono_literals;

    // Write out a test file
    temporary_dir tmp_dir("datalake_cloud_uploader_test");
    auto local_file = make_test_data_file(
      tmp_dir.get_path(), "Hello World", "test_data_file");

    // Upload it
    datalake::cloud_uploader uploader(
      remote(),
      cloud_storage_clients::bucket_name{"test-bucket"},
      "remote_test/remote_directory");
    ss::abort_source never_abort;
    retry_chain_node retry(never_abort, 10s, 1s);
    lazy_abort_source always_continue{[]() { return std::nullopt; }};

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
    auto local_file = make_bad_file(
      tmp_dir.get_path(), "this will not be written", "test_file");

    datalake::cloud_uploader uploader(
      remote(),
      cloud_storage_clients::bucket_name{"test-bucket"},
      "remote_test/remote_directory");
    ss::abort_source never_abort;
    retry_chain_node retry(never_abort, 10s, 1s);
    lazy_abort_source always_continue{[]() { return std::nullopt; }};

    auto cloud_result
      = uploader
          .upload_data_file(
            local_file, "remote_test_file", retry, always_continue)
          .get0();

    EXPECT_TRUE(cloud_result.has_error());
}

TEST_F(DatalakeCloudUploaderTest, UploadsMultiple) {
    using namespace std::chrono_literals;

    // Write out a test file
    temporary_dir tmp_dir("datalake_cloud_uploader_test");
    chunked_vector<datalake::local_data_file> local_files;
    int local_file_count = 10;

    std::string contents = "";
    for (int i = 0; i < local_file_count; i++) {
        auto local_file = make_test_data_file(
          tmp_dir.get_path(), contents, fmt::format("data-{}", i));
        local_files.push_back(local_file);
        contents += "a";
    }
    // Upload it
    datalake::cloud_uploader uploader(
      remote(),
      cloud_storage_clients::bucket_name{"test-bucket"},
      "remote_test/remote_directory");
    ss::abort_source never_abort;
    retry_chain_node retry(never_abort, 10s, 1s);
    lazy_abort_source always_continue{[]() { return std::nullopt; }};

    auto remote_files_result
      = uploader.upload_multiple(std::move(local_files), retry, always_continue)
          .get0();

    ASSERT_TRUE(remote_files_result.has_value());
    const auto& remote_files = remote_files_result.value();

    EXPECT_EQ(remote_files.size(), local_file_count);
    for (int i = 0; i < remote_files.size(); i++) {
        const auto& cloud_result = remote_files[i];
        std::filesystem::path expected_path
          = std::filesystem::path("s3://test-bucket/") / "remote_test"
            / "remote_directory" / fmt::format("data-{}", i);
        EXPECT_EQ(cloud_result.remote_path, expected_path.string());
        // We constructed the files so file i will have size i
        EXPECT_EQ(cloud_result.file_size_bytes, i);
        EXPECT_EQ(cloud_result.row_count, 1);
    }
}

TEST_F(DatalakeCloudUploaderTest, CleansUpOnFailure) {
    using namespace std::chrono_literals;

    // Write out a test file
    temporary_dir tmp_dir("datalake_cloud_uploader_test");
    chunked_vector<datalake::local_data_file> local_files;
    int local_file_count = 10;

    std::string contents = "";
    for (int i = 0; i < local_file_count; i++) {
        if (i == 5) {
            auto local_file = make_bad_file(
              tmp_dir.get_path(), contents, fmt::format("data-{}", i));
            local_files.push_back(local_file);
        } else {
            auto local_file = make_test_data_file(
              tmp_dir.get_path(), contents, fmt::format("data-{}", i));
            local_files.push_back(local_file);
        }
        contents += "a";
    }
    // Upload it
    datalake::cloud_uploader uploader(
      remote(),
      cloud_storage_clients::bucket_name{"test-bucket"},
      "remote_test/remote_directory");
    ss::abort_source never_abort;
    retry_chain_node retry(never_abort, 10s, 1s);
    lazy_abort_source always_continue{[]() { return std::nullopt; }};

    auto remote_files_result
      = uploader.upload_multiple(std::move(local_files), retry, always_continue)
          .get0();

    EXPECT_FALSE(remote_files_result.has_value());
}
