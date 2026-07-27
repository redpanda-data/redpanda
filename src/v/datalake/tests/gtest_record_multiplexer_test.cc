/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_io/provider.h"
#include "container/chunked_circular_buffer.h"
#include "datalake/base_types.h"
#include "datalake/catalog_schema_manager.h"
#include "datalake/local_parquet_file_writer.h"
#include "datalake/record_multiplexer.h"
#include "datalake/record_schema_resolver.h"
#include "datalake/record_translator.h"
#include "datalake/serde_parquet_writer.h"
#include "datalake/table_id_provider.h"
#include "datalake/tests/catalog_and_registry_fixture.h"
#include "datalake/tests/record_generator.h"
#include "datalake/tests/test_data_writer.h"
#include "datalake/tests/test_utils.h"
#include "datalake/translation/translation_probe.h"
#include "features/feature_table.h"
#include "iceberg/datatypes.h"
#include "iceberg/schema.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "storage/record_batch_builder.h"
#include "test_utils/tmp_dir.h"

#include <gtest/gtest.h>

using namespace datalake;
namespace {
simple_schema_manager simple_schema_mgr(iceberg::uri("s3://bucket/test"));
binary_type_resolver bin_resolver;
direct_table_creator t_creator{simple_schema_mgr};
const model::ntp
  ntp(model::ns{"rp"}, model::topic{"t"}, model::partition_id{0});
const model::revision_id rev{123};
record_translator translator;
ss::abort_source as;
// Record timestamps are pinned to the middle of an hour: the default table
// partition spec is hour(redpanda.timestamp), so wall-clock timestamps
// produce an extra partition (and data file) whenever a test's records
// straddle the top of an hour.
constexpr model::timestamp mid_hour_timestamp{1000002600000};
} // namespace

TEST(DatalakeMultiplexerTest, TestMultiplexer) {
    int record_count = 10;
    int batch_count = 10;
    int start_offset = 1005;
    auto writer_factory = std::make_unique<datalake::test_data_writer_factory>(
      false);
    translation_probe probe(ntp);
    features::feature_table features;
    features.testing_activate_all();
    datalake::record_multiplexer multiplexer(
      ntp,
      rev,
      std::move(writer_factory),
      simple_schema_mgr,
      bin_resolver,
      bin_resolver,
      translator,
      t_creator,
      model::iceberg_invalid_record_action::dlq_table,
      iceberg::field_name_comparison::verbatim,
      location_provider(
        cloud_io::s3_compat_provider{"s3"},
        cloud_storage_clients::bucket_name{"bucket"}),
      probe,
      &features);

    model::test::record_batch_spec batch_spec;
    batch_spec.records = record_count;
    batch_spec.count = batch_count;
    batch_spec.offset = model::offset{start_offset};
    batch_spec.timestamp = mid_hour_timestamp;
    chunked_circular_buffer<model::record_batch> batches
      = model::test::make_random_batches(batch_spec).get();

    uint64_t total_bytes = 0;
    for (const auto& batch : batches) {
        total_bytes += batch.size_bytes();
    }

    auto reader = model::make_generating_record_batch_reader(
      [batches = std::move(batches)]() mutable {
          return ss::make_ready_future<model::record_batch_reader::data_t>(
            std::move(batches));
      });

    multiplexer
      .multiplex(
        std::move(reader), kafka::offset{start_offset}, model::no_timeout, as)
      .get();
    record_multiplexer::finished_files files;
    auto result = std::move(multiplexer).finish(files).get();

    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(files.data_files.size(), 1);
    EXPECT_EQ(
      files.data_files[0].local_file.row_count, record_count * batch_count);
    EXPECT_EQ(result.value().start_offset(), start_offset);
    // Subtract one since offsets end at 0, and this is an inclusive range.
    EXPECT_EQ(
      result.value().last_offset(),
      start_offset + record_count * batch_count - 1);
    EXPECT_EQ(result.value().kafka_bytes_processed, total_bytes);
}

TEST(DatalakeMultiplexerTest, TestMultiplexerWriteError) {
    int record_count = 10;
    int batch_count = 10;
    auto writer_factory = std::make_unique<datalake::test_data_writer_factory>(
      true);
    translation_probe probe(ntp);
    features::feature_table features;
    features.testing_activate_all();
    datalake::record_multiplexer multiplexer(
      ntp,
      rev,
      std::move(writer_factory),
      simple_schema_mgr,
      bin_resolver,
      bin_resolver,
      translator,
      t_creator,
      model::iceberg_invalid_record_action::dlq_table,
      iceberg::field_name_comparison::verbatim,
      location_provider(
        cloud_io::s3_compat_provider{"s3"},
        cloud_storage_clients::bucket_name{"bucket"}),
      probe,
      &features);

    model::test::record_batch_spec batch_spec;
    batch_spec.records = record_count;
    batch_spec.count = batch_count;
    chunked_circular_buffer<model::record_batch> batches
      = model::test::make_random_batches(batch_spec).get();

    auto reader = model::make_generating_record_batch_reader(
      [batches = std::move(batches)]() mutable {
          return ss::make_ready_future<model::record_batch_reader::data_t>(
            std::move(batches));
      });
    multiplexer
      .multiplex(std::move(reader), kafka::offset{0}, model::no_timeout, as)
      .get();
    record_multiplexer::finished_files files;
    auto res = std::move(multiplexer).finish(files).get();
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
    noop_mem_tracker tracker;
    auto writer_factory = std::make_unique<local_parquet_file_writer_factory>(
      datalake::local_path(tmp_dir.get_path()),
      "data",
      ss::make_shared<datalake::serde_parquet_writer_factory>(),
      tracker);

    translation_probe probe(ntp);
    features::feature_table features;
    features.testing_activate_all();
    datalake::record_multiplexer multiplexer(
      ntp,
      rev,
      std::move(writer_factory),
      simple_schema_mgr,
      bin_resolver,
      bin_resolver,
      translator,
      t_creator,
      model::iceberg_invalid_record_action::dlq_table,
      iceberg::field_name_comparison::verbatim,
      location_provider(
        cloud_io::s3_compat_provider{"s3"},
        cloud_storage_clients::bucket_name{"bucket"}),
      probe,
      &features);

    model::test::record_batch_spec batch_spec;
    batch_spec.records = record_count;
    batch_spec.count = batch_count;
    batch_spec.offset = model::offset{start_offset};
    batch_spec.timestamp = mid_hour_timestamp;
    chunked_circular_buffer<model::record_batch> batches
      = model::test::make_random_batches(batch_spec).get();

    auto reader = model::make_generating_record_batch_reader(
      [batches = std::move(batches)]() mutable {
          return ss::make_ready_future<model::record_batch_reader::data_t>(
            std::move(batches));
      });

    multiplexer
      .multiplex(
        std::move(reader), kafka::offset{start_offset}, model::no_timeout, as)
      .get();
    record_multiplexer::finished_files files;
    auto result = std::move(multiplexer).finish(files).get();

    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(files.data_files.size(), 1);
    EXPECT_EQ(
      files.data_files[0].local_file.row_count, record_count * batch_count);
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
      : schema_mgr(catalog, &features)
      , type_resolver(registry)
      , t_creator(schema_mgr) {
        features.testing_activate_all();
    }

    features::feature_table features;
    catalog_schema_manager schema_mgr;
    record_schema_resolver type_resolver;
    binary_type_resolver bin_key_resolver;
    // Configured to match record_schema_resolver (schema_id_prefix val mode).
    record_translator schema_translator{
      {}, {model::iceberg_mode::schema_mode::schema_id_prefix}, {}};
    direct_table_creator t_creator;
};

TEST_F(RecordMultiplexerParquetTest, TestSimple) {
    tests::record_generator gen(&registry);
    auto reg_res = gen.register_avro_schema("schema", avro_schema).get();
    EXPECT_FALSE(reg_res.has_error()) << reg_res.error();
    chunked_circular_buffer<model::record_batch> batches;
    model::offset o{0};
    const auto start_offset = o;
    const size_t num_hrs = 3;
    const size_t batches_per_hr = 4;
    const size_t records_per_batch = 4;
    auto start_ts = model::timestamp::now();
    constexpr auto ms_per_hr = 1000 * 3600;
    uint64_t total_bytes = 0;
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
            auto batch = std::move(batch_builder).build();
            total_bytes += batch.size_bytes();
            batches.emplace_back(std::move(batch));
        }
    }
    auto reader = model::make_memory_record_batch_reader(std::move(batches));

    temporary_dir tmp_dir("datalake_multiplexer_test");
    noop_mem_tracker tracker;
    auto writer_factory = std::make_unique<local_parquet_file_writer_factory>(
      datalake::local_path(tmp_dir.get_path()),
      "data",
      ss::make_shared<datalake::serde_parquet_writer_factory>(),
      tracker);
    translation_probe probe(ntp);
    record_multiplexer mux(
      ntp,
      rev,
      std::move(writer_factory),
      schema_mgr,
      type_resolver,
      bin_key_resolver,
      schema_translator,
      t_creator,
      model::iceberg_invalid_record_action::dlq_table,
      iceberg::field_name_comparison::verbatim,
      location_provider(scoped_remote->remote.local().provider(), bucket_name),
      probe,
      &features);
    mux
      .multiplex(
        std::move(reader), kafka::offset{start_offset}, model::no_timeout, as)
      .get();
    record_multiplexer::finished_files files;
    auto res = std::move(mux).finish(files).get();
    ASSERT_FALSE(res.has_error()) << res.error();
    EXPECT_EQ(res.value().start_offset(), start_offset());

    const auto num_records = num_hrs * batches_per_hr * records_per_batch;
    EXPECT_EQ(res.value().last_offset(), start_offset() + num_records - 1);
    EXPECT_EQ(res.value().kafka_bytes_processed, total_bytes);
}

namespace {
constexpr std::string_view key_avro_schema = R"({
    "type": "record",
    "name": "KeyRecord",
    "fields": [
        { "name": "id", "type": "long" }
    ]
})";
} // namespace

// Verify that key:mode=schema_id_prefix decodes keys via the schema registry
// and promotes the decoded schema type into the "redpanda.key" iceberg field
// (replacing the default binary type).
TEST_F(RecordMultiplexerParquetTest, TestKeySchemaMode) {
    tests::record_generator gen(&registry);
    auto key_reg
      = gen.register_avro_schema("key_schema", key_avro_schema).get();
    ASSERT_FALSE(key_reg.has_error()) << key_reg.error();
    auto val_reg = gen.register_avro_schema("val_schema", avro_schema).get();
    ASSERT_FALSE(val_reg.has_error()) << val_reg.error();

    // Build records with Avro-encoded keys and values.
    const int num_records = 5;
    chunked_circular_buffer<model::record_batch> batches;
    model::offset o{0};
    for (int i = 0; i < num_records; ++i) {
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset{o});
        builder.set_timestamp(mid_hour_timestamp);
        auto key_buf = gen.encode_avro_buf("key_schema").get();
        ASSERT_FALSE(key_buf.has_error()) << key_buf.error();
        auto add_res = gen
                         .add_random_avro_record(
                           builder, "val_schema", std::move(key_buf.value()))
                         .get();
        ASSERT_FALSE(add_res.has_error());
        ++o;
        batches.emplace_back(std::move(builder).build());
    }

    auto reader = model::make_memory_record_batch_reader(std::move(batches));

    temporary_dir tmp_dir("datalake_key_schema_test");
    noop_mem_tracker tracker;
    auto writer_factory = std::make_unique<local_parquet_file_writer_factory>(
      datalake::local_path(tmp_dir.get_path()),
      "data",
      ss::make_shared<datalake::serde_parquet_writer_factory>(),
      tracker);
    translation_probe probe(ntp);

    // Translator with key and value both in schema_id_prefix mode.
    using sm = model::iceberg_mode::schema_mode;
    record_translator key_schema_translator{
      {sm::schema_id_prefix}, {sm::schema_id_prefix}, {}};

    record_multiplexer mux(
      ntp,
      rev,
      std::move(writer_factory),
      schema_mgr,
      type_resolver, // val resolver: record_schema_resolver
      type_resolver, // key resolver: same, both use schema_id prefix
      key_schema_translator,
      t_creator,
      model::iceberg_invalid_record_action::dlq_table,
      iceberg::field_name_comparison::verbatim,
      location_provider(scoped_remote->remote.local().provider(), bucket_name),
      probe,
      &features);

    mux.multiplex(std::move(reader), kafka::offset{0}, model::no_timeout, as)
      .get();
    record_multiplexer::finished_files files;
    auto res = std::move(mux).finish(files).get();
    ASSERT_FALSE(res.has_error()) << res.error();
    ASSERT_EQ(files.data_files.size(), 1);
    EXPECT_EQ(files.data_files[0].local_file.row_count, num_records);

    // The "redpanda.key" field in the iceberg schema should be a struct_type
    // decoded from the Avro key schema, not the default binary_type.
    auto load_res
      = catalog.load_table(datalake::table_id_provider::table_id(ntp.tp.topic))
          .get();
    ASSERT_FALSE(load_res.has_error()) << load_res.error();
    auto& table = load_res.value();
    auto sit = std::ranges::find(
      table.schemas, table.current_schema_id, &iceberg::schema::schema_id);
    ASSERT_FALSE(sit == table.schemas.end());
    auto* key_field = sit->schema_struct.find_field_by_name(
      {ss::sstring("redpanda"), ss::sstring("key")});
    ASSERT_NE(key_field, nullptr);
    EXPECT_TRUE(std::holds_alternative<iceberg::struct_type>(key_field->type))
      << "expected redpanda.key to be struct_type (decoded Avro key schema), "
         "not binary";
}
