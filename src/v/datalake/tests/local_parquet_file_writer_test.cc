/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/local_parquet_file_writer.h"
#include "datalake/tests/test_data.h"
#include "iceberg/tests/value_generator.h"
#include "test_utils/test.h"
#include "test_utils/tmp_dir.h"

#include <seastar/core/seastar.hh>

#include <gtest/gtest.h>

#include <filesystem>

namespace {
struct test_writer : datalake::parquet_ostream {
    test_writer(
      size_t error_after_rows, bool error_on_finish, ss::output_stream<char> os)
      : error_after_rows_(error_after_rows)
      , error_on_finish_(error_on_finish)
      , os_(std::move(os)) {}
    ss::future<datalake::writer_error>
    add_data_struct(iceberg::struct_value, size_t) final {
        if (rows_ >= error_after_rows_) {
            co_return datalake::writer_error::file_io_error;
        }
        rows_++;

        co_return datalake::writer_error::ok;
    };

    ss::future<datalake::writer_error> finish() final {
        if (error_on_finish_) {
            co_return datalake::writer_error::file_io_error;
        }
        co_await os_.close();
        co_return datalake::writer_error::ok;
    }

    size_t error_after_rows_;
    bool error_on_finish_;
    size_t rows_{0};
    ss::output_stream<char> os_;
};

struct test_writer_factory : datalake::parquet_ostream_factory {
    explicit test_writer_factory(
      size_t error_after_rows = std::numeric_limits<size_t>::max(),
      bool error_on_finish = false)
      : error_after_rows_(error_after_rows)
      , error_on_finish_(error_on_finish) {}

    ss::future<std::unique_ptr<datalake::parquet_ostream>> create_writer(
      const iceberg::struct_type&, ss::output_stream<char> os) final {
        co_return std::make_unique<test_writer>(
          error_after_rows_, error_on_finish_, std::move(os));
    };

    size_t error_after_rows_;
    bool error_on_finish_;
};

} // namespace

struct LocalFileWriterTest : public testing::Test {
    // Sets up the test fixture.
    void SetUp() final {}

    // Tears down the test fixture.
    void TearDown() final {}

    temporary_dir tmp_dir = temporary_dir("batching_parquet_writer");
    std::filesystem::path file_path = "test_file.parquet";
    std::filesystem::path full_path = tmp_dir.get_path() / file_path;
};

TEST_F(LocalFileWriterTest, TestHappyPath) {
    datalake::local_parquet_file_writer file_writer(
      datalake::local_path(full_path), ss::make_shared<test_writer_factory>());

    auto schema = test_schema(iceberg::field_required::no);
    file_writer.initialize(schema).get();

    size_t rows = 1000;
    for (size_t i = 0; i < rows; i++) {
        auto data = iceberg::tests::make_struct_value(
          iceberg::tests::value_spec{},
          test_schema(iceberg::field_required::no));

        auto res = file_writer.add_data_struct(std::move(data), 1000).get();
        ASSERT_EQ(datalake::writer_error::ok, res);
    }

    auto result = file_writer.finish().get();

    ASSERT_TRUE(result.has_value());

    EXPECT_EQ(result.value().path, full_path);
    EXPECT_EQ(result.value().row_count, rows);
    auto true_file_size = std::filesystem::file_size(full_path);
    EXPECT_EQ(result.value().size_bytes, true_file_size);
}

TEST_F(LocalFileWriterTest, TestErrorOnWrite) {
    datalake::local_parquet_file_writer file_writer(
      datalake::local_path(full_path),
      ss::make_shared<test_writer_factory>(100));
    auto schema = test_schema(iceberg::field_required::no);
    file_writer.initialize(schema).get();

    size_t rows = 1000;
    for (size_t i = 0; i < rows; i++) {
        auto data = iceberg::tests::make_struct_value(
          iceberg::tests::value_spec{},
          test_schema(iceberg::field_required::no));

        file_writer.add_data_struct(std::move(data), 1000).get();
    }

    auto result = file_writer.finish().get();

    ASSERT_TRUE(result.has_error());

    // intermediate file shuld be removed
    ASSERT_FALSE(std::filesystem::exists(full_path));
}

TEST_F(LocalFileWriterTest, TestErrorOnFinish) {
    datalake::local_parquet_file_writer file_writer(
      datalake::local_path(full_path),
      ss::make_shared<test_writer_factory>(5000, true));
    auto schema = test_schema(iceberg::field_required::no);
    file_writer.initialize(schema).get();

    size_t rows = 1000;
    for (size_t i = 0; i < rows; i++) {
        auto data = iceberg::tests::make_struct_value(
          iceberg::tests::value_spec{},
          test_schema(iceberg::field_required::no));

        file_writer.add_data_struct(std::move(data), 1000).get();
    }

    auto result = file_writer.finish().get();

    ASSERT_TRUE(result.has_error());

    // intermediate file should be removed
    ASSERT_FALSE(std::filesystem::exists(full_path));
}
