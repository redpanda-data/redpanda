/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/base_types.h"
#include "datalake/batching_parquet_writer.h"
#include "datalake/record_multiplexer.h"
#include "datalake/tests/test_data_writer.h"
#include "model/fundamental.h"
#include "model/tests/random_batch.h"
#include "test_utils/tmp_dir.h"

#include <arrow/io/file.h>
#include <arrow/table.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>

#include <filesystem>

TEST(DatalakeMultiplexerTest, TestMultiplexer) {
    int record_count = 10;
    int batch_count = 10;
    int start_offset = 1005;
    auto writer_factory = std::make_unique<datalake::test_data_writer_factory>(
      false);
    datalake::record_multiplexer multiplexer(std::move(writer_factory));

    model::test::record_batch_spec batch_spec;
    batch_spec.records = record_count;
    batch_spec.count = batch_count;
    batch_spec.offset = model::offset{start_offset};
    ss::circular_buffer<model::record_batch> batches
      = model::test::make_random_batches(batch_spec).get();

    auto reader = model::make_generating_record_batch_reader(
      [batches = std::move(batches)]() mutable {
          return ss::make_ready_future<model::record_batch_reader::data_t>(
            std::move(batches));
      });

    auto result
      = reader.consume(std::move(multiplexer), model::no_timeout).get();

    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value().data_files.size(), 1);
    EXPECT_EQ(
      result.value().data_files[0].row_count, record_count * batch_count);
    EXPECT_EQ(result.value().start_offset(), start_offset);
    // Subtract one since offsets end at 0, and this is an inclusive range.
    EXPECT_EQ(
      result.value().last_offset(),
      start_offset + record_count * batch_count - 1);
}
TEST(DatalakeMultiplexerTest, TestMultiplexerWriteError) {
    int record_count = 10;
    int batch_count = 10;
    auto writer_factory = std::make_unique<datalake::test_data_writer_factory>(
      true);
    datalake::record_multiplexer multiplexer(std::move(writer_factory));

    model::test::record_batch_spec batch_spec;
    batch_spec.records = record_count;
    batch_spec.count = batch_count;
    ss::circular_buffer<model::record_batch> batches
      = model::test::make_random_batches(batch_spec).get0();

    auto reader = model::make_generating_record_batch_reader(
      [batches = std::move(batches)]() mutable {
          return ss::make_ready_future<model::record_batch_reader::data_t>(
            std::move(batches));
      });
    auto res = reader.consume(std::move(multiplexer), model::no_timeout).get0();
    ASSERT_TRUE(res.has_error());
    EXPECT_EQ(
      res.error(), datalake::data_writer_error::parquet_conversion_error);
}

TEST(DatalakeMultiplexerTest, WritesDataFiles) {
    // Almost an integration test:
    // Stitch together as many parts of the data path as is reasonable in a
    // single test and make sure we can go from Kafka log to Parquet files on
    // disk.
    temporary_dir tmp_dir("datalake_multiplexer_test");

    int record_count = 50;
    int batch_count = 20;
    int start_offset = 1005;

    auto writer_factory
      = std::make_unique<datalake::batching_parquet_writer_factory>(
        datalake::local_path(tmp_dir.get_path()), "data", 100, 10000);
    datalake::record_multiplexer multiplexer(std::move(writer_factory));

    model::test::record_batch_spec batch_spec;
    batch_spec.records = record_count;
    batch_spec.count = batch_count;
    batch_spec.offset = model::offset{start_offset};
    ss::circular_buffer<model::record_batch> batches
      = model::test::make_random_batches(batch_spec).get0();

    auto reader = model::make_generating_record_batch_reader(
      [batches = std::move(batches)]() mutable {
          return ss::make_ready_future<model::record_batch_reader::data_t>(
            std::move(batches));
      });

    auto result
      = reader.consume(std::move(multiplexer), model::no_timeout).get0();

    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value().data_files.size(), 1);
    EXPECT_EQ(
      result.value().data_files[0].row_count, record_count * batch_count);
    EXPECT_EQ(result.value().start_offset(), start_offset);
    // Subtract one since offsets end at 0, and this is an inclusive range.
    EXPECT_EQ(
      result.value().last_offset(),
      start_offset + record_count * batch_count - 1);

    // Open the resulting file and check that it has data in it with the
    // appropriate counts.
    int file_count = 0;
    for (const auto& entry :
         std::filesystem::directory_iterator(tmp_dir.get_path())) {
        file_count++;
        auto arrow_file_reader
          = arrow::io::ReadableFile::Open(entry.path()).ValueUnsafe();

        // Open Parquet file reader
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        ASSERT_TRUE(
          parquet::arrow::OpenFile(
            arrow_file_reader, arrow::default_memory_pool(), &arrow_reader)
            .ok());

        // Read entire file as a single Arrow table
        std::shared_ptr<arrow::Table> table;
        auto r = arrow_reader->ReadTable(&table);
        ASSERT_TRUE(r.ok());

        EXPECT_EQ(table->num_rows(), record_count * batch_count);
        // Expect 4 columns for schemaless: offset, timestamp, key, value
        EXPECT_EQ(table->num_columns(), 4);
    }
    // Expect this test to create exactly 1 file
    EXPECT_EQ(file_count, 1);
}
