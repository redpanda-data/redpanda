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
#include "datalake/catalog_schema_manager.h"
#include "datalake/local_parquet_file_writer.h"
#include "datalake/record_multiplexer.h"
#include "datalake/record_schema_resolver.h"
#include "datalake/record_translator.h"
#include "datalake/serde_parquet_writer.h"
#include "datalake/table_creator.h"
#include "datalake/tests/catalog_and_registry_fixture.h"
#include "datalake/tests/record_generator.h"
#include "datalake/tests/test_data_writer.h"
#include "iceberg/filesystem_catalog.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "storage/record_batch_builder.h"
#include "test_utils/tmp_dir.h"

#include <gtest/gtest.h>

#include <filesystem>

using namespace datalake;
namespace {
simple_schema_manager simple_schema_mgr;
binary_type_resolver bin_resolver;
direct_table_creator t_creator{bin_resolver, simple_schema_mgr};
const model::ntp
  ntp(model::ns{"rp"}, model::topic{"t"}, model::partition_id{0});
const model::revision_id rev{123};
default_translator translator;
} // namespace

TEST(DatalakeMultiplexerTest, TestMultiplexer) {
    int record_count = 10;
    int batch_count = 10;
    int start_offset = 1005;
    auto writer_factory = std::make_unique<datalake::test_data_writer_factory>(
      false);
    datalake::record_multiplexer multiplexer(
      ntp,
      rev,
      std::move(writer_factory),
      simple_schema_mgr,
      bin_resolver,
      translator,
      t_creator);

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
    datalake::record_multiplexer multiplexer(
      ntp,
      rev,
      std::move(writer_factory),
      simple_schema_mgr,
      bin_resolver,
      translator,
      t_creator);

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
    EXPECT_EQ(res.error(), datalake::writer_error::parquet_conversion_error);
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

    auto writer_factory = std::make_unique<local_parquet_file_writer_factory>(
      datalake::local_path(tmp_dir.get_path()),
      "data",
      ss::make_shared<datalake::serde_parquet_writer_factory>());

    datalake::record_multiplexer multiplexer(
      ntp,
      rev,
      std::move(writer_factory),
      simple_schema_mgr,
      bin_resolver,
      translator,
      t_creator);

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
}

namespace {
constexpr std::string_view avro_schema = R"({
    "type": "record",
    "name": "RootRecord",
    "fields": [
        { "name": "mylong", "doc": "mylong field doc.", "type": "long" },
        {
            "name": "nestedrecord",
            "type": {
                "type": "record",
                "name": "Nested",
                "fields": [
                    { "name": "inval1", "type": "double" },
                    { "name": "inval2", "type": "string" },
                    { "name": "inval3", "type": "int" }
                ]
            }
        },
        { "name": "myarray", "type": { "type": "array", "items": "double" } },
        { "name": "mybool", "type": "boolean" },
        { "name": "myfixed", "type": { "type": "fixed", "size": 16, "name": "md5" } },
        { "name": "anotherint", "type": "int" },
        { "name": "bytes", "type": "bytes" }
    ]
})";
} // namespace

class RecordMultiplexerParquetTest
  : public tests::catalog_and_registry_fixture
  , public ::testing::Test {
public:
    RecordMultiplexerParquetTest()
      : schema_mgr(catalog)
      , type_resolver(registry)
      , t_creator(type_resolver, schema_mgr) {}
    catalog_schema_manager schema_mgr;
    record_schema_resolver type_resolver;
    direct_table_creator t_creator;
};

TEST_F(RecordMultiplexerParquetTest, TestSimple) {
    tests::record_generator gen(&registry);
    auto reg_res = gen.register_avro_schema("schema", avro_schema).get();
    EXPECT_FALSE(reg_res.has_error()) << reg_res.error();
    ss::circular_buffer<model::record_batch> batches;
    model::offset o{0};
    const auto start_offset = o;
    const size_t num_hrs = 3;
    const size_t batches_per_hr = 4;
    const size_t records_per_batch = 4;
    auto start_ts = model::timestamp::now();
    constexpr auto ms_per_hr = 1000 * 3600;
    for (size_t h = 0; h < num_hrs; ++h) {
        // Split batches across the hours.
        auto h_ts = model::timestamp{
          start_ts.value() + ms_per_hr * static_cast<long>(h)};
        for (size_t b = 0; b < batches_per_hr; ++b) {
            storage::record_batch_builder batch_builder(
              model::record_batch_type::raft_data, model::offset{o});
            batch_builder.set_timestamp(h_ts);

            // Add some records per batch.
            for (size_t r = 0; r < records_per_batch; ++r) {
                auto add_res = gen
                                 .add_random_avro_record(
                                   batch_builder, "schema", std::nullopt)
                                 .get();
                ASSERT_FALSE(add_res.has_error());
                ++o;
            }
            batches.emplace_back(std::move(batch_builder).build());
        }
    }
    auto reader = model::make_memory_record_batch_reader(std::move(batches));

    temporary_dir tmp_dir("datalake_multiplexer_test");

    auto writer_factory = std::make_unique<local_parquet_file_writer_factory>(
      datalake::local_path(tmp_dir.get_path()),
      "data",
      ss::make_shared<datalake::serde_parquet_writer_factory>());
    record_multiplexer mux(
      ntp,
      rev,
      std::move(writer_factory),
      schema_mgr,
      type_resolver,
      translator,
      t_creator);
    auto res = reader.consume(std::move(mux), model::no_timeout).get();
    ASSERT_FALSE(res.has_error()) << res.error();
    EXPECT_EQ(res.value().start_offset(), start_offset());

    const auto num_records = num_hrs * batches_per_hr * records_per_batch;
    EXPECT_EQ(res.value().last_offset(), start_offset() + num_records - 1);
}
