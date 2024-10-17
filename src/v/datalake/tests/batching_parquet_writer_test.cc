/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/batching_parquet_writer.h"
#include "datalake/tests/test_data.h"
#include "iceberg/tests/value_generator.h"
#include "iceberg/values.h"
#include "test_utils/tmp_dir.h"

#include <seastar/core/seastar.hh>

#include <arrow/io/file.h>
#include <arrow/table.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>

#include <filesystem>

namespace datalake {

TEST(BatchingParquetWriterTest, WritesParquetFiles) {
    temporary_dir tmp_dir("batching_parquet_writer");
    std::filesystem::path file_path = "test_file.parquet";
    std::filesystem::path full_path = tmp_dir.get_path() / file_path;
    int num_rows = 1000;

    datalake::batching_parquet_writer writer(
      test_schema(iceberg::field_required::no),
      500,
      1000000,
      local_path(full_path));

    writer.initialize().get();

    for (int i = 0; i < num_rows; i++) {
        auto data = iceberg::tests::make_struct_value(
          iceberg::tests::value_spec{
            .forced_fixed_val = iobuf::from("Hello world")},
          test_schema(iceberg::field_required::no));
        writer.add_data_struct(std::move(data), 1000).get();
    }

    auto result = writer.finish().get0();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value().path, full_path);
    EXPECT_EQ(result.value().row_count, num_rows);
    auto true_file_size = std::filesystem::file_size(full_path);
    EXPECT_EQ(result.value().size_bytes, true_file_size);

    // Read the file and check the contents
    auto reader = arrow::io::ReadableFile::Open(full_path).ValueUnsafe();

    // Open Parquet file reader
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    ASSERT_TRUE(parquet::arrow::OpenFile(
                  reader, arrow::default_memory_pool(), &arrow_reader)
                  .ok());

    // Read entire file as a single Arrow table
    std::shared_ptr<arrow::Table> table;
    auto r = arrow_reader->ReadTable(&table);
    ASSERT_TRUE(r.ok());

    EXPECT_EQ(table->num_rows(), num_rows);
    EXPECT_EQ(table->num_columns(), 17);
}

TEST(BatchingParquetWriterTest, DeletesFileOnAbort) {
    temporary_dir tmp_dir("batching_parquet_writer");
    std::filesystem::path file_path = "test_file.parquet";
    int num_rows = 1000;

    datalake::batching_parquet_writer writer(
      test_schema(iceberg::field_required::no),
      500,
      1000000,
      local_path(tmp_dir.get_path() / file_path));

    writer.initialize().get();

    for (int i = 0; i < num_rows; i++) {
        auto data = iceberg::tests::make_struct_value(
          iceberg::tests::value_spec{
            .forced_fixed_val = iobuf::from("Hello world")},
          test_schema(iceberg::field_required::no));
        writer.add_data_struct(std::move(data), 1000).get0();
    }
    writer.abort().get();
    auto exists = ss::file_exists(file_path.c_str()).get();
    EXPECT_FALSE(exists);
}

} // namespace datalake
