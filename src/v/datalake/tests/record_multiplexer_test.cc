/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "container/chunked_circular_buffer.h"
#include "datalake/catalog_schema_manager.h"
#include "datalake/record_multiplexer.h"
#include "datalake/record_schema_resolver.h"
#include "datalake/record_translator.h"
#include "datalake/table_definition.h"
#include "datalake/table_id_provider.h"
#include "datalake/tests/catalog_and_registry_fixture.h"
#include "datalake/tests/record_generator.h"
#include "datalake/tests/test_data_writer.h"
#include "datalake/tests/test_utils.h"
#include "datalake/translation/translation_probe.h"
#include "features/feature_table.h"
#include "gmock/gmock.h"
#include "iceberg/filesystem_catalog.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "storage/record_batch_builder.h"

#include <avro/Compiler.hh>
#include <gtest/gtest.h>

using namespace datalake;

namespace {
structured_data_translator translator;
const model::ntp
  ntp(model::ns{"rp"}, model::topic{"t"}, model::partition_id{0});
const model::revision_id topic_rev{123};
// v1: struct field with one field.
constexpr std::string_view avro_schema_v1_str = R"({
    "type": "record",
    "name": "RootRecord",
    "fields": [
      { "name": "mynum", "doc": "mynum field doc.", "type": "int" },
      { "name": "mylong", "doc": "mylong field doc.", "type": "long" }
    ]
})";
// v2: v1 schema + a field promotion + several others in the struct.
constexpr std::string_view avro_schema_v2_str = R"({
    "type": "record",
    "name": "RootRecord",
    "fields": [
        { "name": "mynum", "doc": "mynum field doc.", "type": "long" },
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
constexpr std::string_view avro_schema_w_redpanda_str = R"({
    "type": "record",
    "name": "RootRecord",
    "fields": [
        { "name": "mylong", "doc": "mylong field doc.", "type": "long" },
        {
            "name": "redpanda",
            "type": {
                "type": "record",
                "name": "Nested",
                "fields": [
                    { "name": "foo", "type": "double" }
                ]
            }
        }
    ]
})";
} // namespace

struct records_param {
    size_t records_per_batch;
    size_t batches_per_hr;
    size_t hrs;

    size_t num_records() const { return records_per_hr() * hrs; }
    size_t records_per_hr() const { return records_per_batch * batches_per_hr; }

    friend std::ostream& operator<<(std::ostream&, const records_param&);
};

std::ostream& operator<<(std::ostream& os, const records_param& rp) {
    fmt::print(
      os,
      "{{records_per_batch: {}, batches_per_hr: {}, hrs: {}}}",
      rp.records_per_batch,
      rp.batches_per_hr,
      rp.hrs);
    return os;
}

class RecordMultiplexerTestBase
  : public datalake::tests::catalog_and_registry_fixture {
public:
    RecordMultiplexerTestBase()
      : schema_mgr(catalog, &features)
      , type_resolver(registry)
      , t_creator(type_resolver, schema_mgr) {
        features.testing_activate_all();
    }

    record_multiplexer make_mux() {
        return record_multiplexer(
          ntp,
          topic_rev,
          std::make_unique<test_data_writer_factory>(false),
          schema_mgr,
          type_resolver,
          translator,
          t_creator,
          model::iceberg_invalid_record_action::dlq_table,
          location_provider(
            scoped_remote->remote.local().provider(), bucket_name),
          *get_or_create_probe(ntp),
          &features);
    }

    // Runs the multiplexer on records generated with cb() based on the test
    // parameters.
    std::optional<record_multiplexer::write_result> mux(
      const records_param& param,
      model::offset o,
      const std::function<void(storage::record_batch_builder&)>& cb,
      record_multiplexer::finished_files& files,
      bool expect_error = false) {
        auto start_offset = o;
        chunked_circular_buffer<model::record_batch> batches;
        const auto start_ts = model::timestamp::now();
        constexpr auto ms_per_hr = 1000 * 3600;
        for (size_t h = 0; h < param.hrs; ++h) {
            // Split batches across the hours.
            auto h_ts = model::timestamp{
              start_ts.value() + ms_per_hr * static_cast<long>(h)};
            for (size_t b = 0; b < param.batches_per_hr; ++b) {
                storage::record_batch_builder batch_builder(
                  model::record_batch_type::raft_data, model::offset{o});
                batch_builder.set_timestamp(h_ts);

                // Add some records per batch.
                for (size_t r = 0; r < param.records_per_batch; ++r) {
                    cb(batch_builder);
                    ++o;
                }
                batches.emplace_back(std::move(batch_builder).build());
            }
        }
        record_multiplexer mux = make_mux();
        // randomly split batches into multiple readers
        size_t total_batches = batches.size();
        size_t total_split_batches = 0;
        while (!batches.empty()) {
            auto subset_size = random_generators::get_int<size_t>(
              1, batches.size());
            chunked_circular_buffer<model::record_batch> subset;
            while (subset.size() != subset_size) {
                subset.push_back(batches.front().share());
                batches.pop_front();
            }
            total_split_batches += subset.size();
            auto reader = model::make_memory_record_batch_reader(
              std::move(subset));
            mux
              .multiplex(
                std::move(reader),
                kafka::offset{start_offset},
                model::no_timeout,
                as)
              .get();
            mux.flush_writers().get();
        }
        EXPECT_EQ(total_batches, total_split_batches);
        auto res = std::move(mux).finish(files).get();
        if (expect_error) {
            EXPECT_TRUE(res.has_error());
        } else {
            EXPECT_FALSE(res.has_error()) << res.error();
        }
        if (res.has_error()) {
            return std::nullopt;
        }
        EXPECT_EQ(res.value().start_offset(), start_offset());
        EXPECT_EQ(
          res.value().last_offset(), start_offset() + param.num_records() - 1);
        return std::move(res.value());
    }

    // Returns the current schema.
    std::optional<iceberg::schema> get_current_schema() {
        auto load_res
          = catalog.load_table(iceberg::table_identifier{{"redpanda"}, "t"})
              .get();
        EXPECT_FALSE(load_res.has_error()) << load_res.error();
        if (load_res.has_error()) {
            return std::nullopt;
        }
        auto& table = load_res.value();
        auto it = std::ranges::find(
          table.schemas, table.current_schema_id, &iceberg::schema::schema_id);
        EXPECT_FALSE(it == table.schemas.end());
        if (it == table.schemas.end()) {
            return std::nullopt;
        }
        return std::move(*it);
    }

    void assert_dlq_table(const model::topic& t, bool exists) {
        auto dlq_table_id = datalake::table_id_provider::dlq_table_id(t);
        auto load_res = catalog.load_table(dlq_table_id).get();
        if (exists) {
            EXPECT_FALSE(load_res.has_error())
              << "Expected DLQ table to exit but got an error: "
              << load_res.error();
        } else {
            EXPECT_TRUE(load_res.has_error());
        }
    }

    ss::lw_shared_ptr<translation_probe>
    get_or_create_probe(const model::ntp& ntp) {
        auto [it, inserted] = probes.try_emplace(ntp, nullptr);
        if (inserted) {
            it->second = ss::make_lw_shared<translation_probe>(ntp);
        }
        return it->second;
    }

    features::feature_table features;
    catalog_schema_manager schema_mgr;
    record_schema_resolver type_resolver;
    direct_table_creator t_creator;
    std::map<model::ntp, ss::lw_shared_ptr<translation_probe>> probes;
    ss::abort_source as;

    static constexpr records_param default_param = {
      .records_per_batch = 1,
      .batches_per_hr = 1,
      .hrs = 1,
    };
};

class RecordMultiplexerParamTest
  : public RecordMultiplexerTestBase
  , public ::testing::TestWithParam<records_param> {
public:
    std::optional<record_multiplexer::write_result> mux(
      model::offset o,
      const std::function<void(storage::record_batch_builder&)>& cb,
      record_multiplexer::finished_files& files,
      bool expect_error = false) {
        return RecordMultiplexerTestBase::mux(
          GetParam(), o, cb, files, expect_error);
    }
};

TEST_P(RecordMultiplexerParamTest, TestNoSchema) {
    auto start_offset = model::offset{0};
    record_multiplexer::finished_files files;
    auto res = mux(
      start_offset,
      [](storage::record_batch_builder& b) {
          b.add_raw_kv(std::nullopt, iobuf::from("foobar"));
      },
      files);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(files.data_files.size(), 0);

    assert_dlq_table(ntp.tp.topic, true);
    EXPECT_EQ(files.dlq_files.size(), GetParam().hrs);
}

TEST_P(RecordMultiplexerParamTest, TestSimpleAvroRecords) {
    tests::record_generator gen(&registry);
    auto reg_res
      = gen.register_avro_schema("avro_v1", avro_schema_v1_str).get();
    EXPECT_FALSE(reg_res.has_error()) << reg_res.error();

    // Add Avro records.
    auto start_offset = model::offset{0};
    record_multiplexer::finished_files files;
    auto res = mux(
      start_offset,
      [&gen](storage::record_batch_builder& b) {
          auto res
            = gen.add_random_avro_record(b, "avro_v1", std::nullopt).get();
          ASSERT_FALSE(res.has_error());
      },
      files);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(files.data_files.size(), GetParam().hrs);

    std::unordered_set<int> hrs;
    for (auto& f : files.data_files) {
        hrs.emplace(get_hour(f.partition_key));
        EXPECT_EQ(f.local_file.row_count, GetParam().records_per_hr());
    }
    EXPECT_EQ(hrs.size(), GetParam().hrs);

    // 4 default columns + RootRecord + mylong
    auto schema = get_current_schema();
    EXPECT_EQ(schema->highest_field_id(), 12);

    // No DLQ table when all records are valid.
    assert_dlq_table(ntp.tp.topic, false);
}

TEST_P(RecordMultiplexerParamTest, TestAvroRecordsMultipleSchemas) {
    tests::record_generator gen(&registry);
    auto reg_res
      = gen.register_avro_schema("avro_v1", avro_schema_v1_str).get();
    EXPECT_FALSE(reg_res.has_error()) << reg_res.error();
    reg_res = gen.register_avro_schema("avro_v2", avro_schema_v2_str).get();
    EXPECT_FALSE(reg_res.has_error()) << reg_res.error();

    auto start_offset = model::offset{0};
    int i = 0;
    record_multiplexer::finished_files files;
    auto res = mux(
      start_offset,
      [&gen, &i](storage::record_batch_builder& b) {
          auto res = gen
                       .add_random_avro_record(
                         b, (++i % 2) ? "avro_v1" : "avro_v2", std::nullopt)
                       .get();
          ASSERT_FALSE(res.has_error());
      },
      files);
    ASSERT_TRUE(res.has_value());

    // There should be twice as many files as normal, since we have twice the
    // schemas.
    EXPECT_EQ(files.data_files.size(), 2 * GetParam().hrs);

    std::unordered_set<int> hrs;
    for (auto& f : files.data_files) {
        hrs.emplace(get_hour(f.partition_key));
        // Each file should have half the records as normal, since we have
        // twice the files.
        EXPECT_EQ(f.local_file.row_count, GetParam().records_per_hr() / 2);
    }
    EXPECT_EQ(hrs.size(), GetParam().hrs);
    auto schema = get_current_schema();
    EXPECT_EQ(schema->highest_field_id(), 22);
}

INSTANTIATE_TEST_SUITE_P(
  RecordsArgs,
  RecordMultiplexerParamTest,
  ::testing::Values(
    records_param{
      .records_per_batch = 10,
      .batches_per_hr = 2,
      .hrs = 1,
    },
    records_param{
      .records_per_batch = 4,
      .batches_per_hr = 4,
      .hrs = 4,
    }),
  [](const auto& info) {
      const auto& p = info.param;
      return fmt::format(
        "rpb{}_bph{}_h{}", p.records_per_batch, p.batches_per_hr, p.hrs);
  });

class RecordMultiplexerTest
  : public RecordMultiplexerTestBase
  , public ::testing::Test {};

TEST_F(RecordMultiplexerTest, TestAvroRecordsWithRedpandaField) {
    tests::record_generator gen(&registry);
    auto reg_res
      = gen.register_avro_schema("avro_rp", avro_schema_w_redpanda_str).get();
    EXPECT_FALSE(reg_res.has_error()) << reg_res.error();

    // Add Avro records.
    auto start_offset = model::offset{0};
    record_multiplexer::finished_files files;
    auto res = mux(
      default_param,
      start_offset,
      [&gen](storage::record_batch_builder& b) {
          auto res
            = gen.add_random_avro_record(b, "avro_rp", std::nullopt).get();
          ASSERT_FALSE(res.has_error());
      },
      files);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(files.data_files.size(), default_param.hrs);

    std::unordered_set<int> hrs;
    for (auto& f : files.data_files) {
        hrs.emplace(get_hour(f.partition_key));
        EXPECT_EQ(f.local_file.row_count, default_param.records_per_hr());
    }
    EXPECT_EQ(hrs.size(), default_param.hrs);

    // 1 nested redpanda column + 4 default columns + mylong + 1 user redpanda
    // column + 1 nested
    auto schema = get_current_schema();
    EXPECT_EQ(schema->highest_field_id(), 13);

    // The redpanda system fields should include the 'data' column.
    const auto& rp_struct = std::get<iceberg::struct_type>(
      schema->schema_struct.fields[0]->type);
    EXPECT_EQ(7, rp_struct.fields.size());
    EXPECT_EQ("data", rp_struct.fields.back()->name);
}

TEST_F(RecordMultiplexerTest, TestMissingSchema) {
    auto start_offset = model::offset{0};
    record_multiplexer::finished_files files;
    auto res = mux(
      default_param,
      start_offset,
      [](storage::record_batch_builder& b) {
          iobuf buf;
          // Append data with a magic 0 byte that doesn't actually correspond to
          // anything.
          buf.append("\0\0\0\0\0\0\0", 7);
          b.add_raw_kv(std::nullopt, std::move(buf));
      },
      files);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(files.data_files.size(), 0);

    assert_dlq_table(ntp.tp.topic, true);
    EXPECT_EQ(files.dlq_files.size(), default_param.hrs);

    EXPECT_EQ(
      get_or_create_probe(ntp)->counter_ref(
        translation_probe::invalid_record_cause::
          failed_kafka_schema_resolution),
      default_param.num_records());
}

TEST_F(RecordMultiplexerTest, TestBadData) {
    tests::record_generator gen(&registry);
    auto reg_res
      = gen.register_avro_schema("avro_v1", avro_schema_v1_str).get();
    EXPECT_FALSE(reg_res.has_error()) << reg_res.error();
    auto ctx_schema_id = reg_res.value();

    auto start_offset = model::offset{0};
    record_multiplexer::finished_files files;
    auto res = mux(
      default_param,
      start_offset,
      [ctx_schema_id](storage::record_batch_builder& b) {
          iobuf buf;
          // Append data with a magic bytes that corresponds to the actual
          // schema.
          buf.append("\0", 1);
          int32_t encoded_id = ss::cpu_to_be(ctx_schema_id.id());
          buf.append((const uint8_t*)(&encoded_id), 4);
          buf.append("\1\1\1", 3);
          b.add_raw_kv(std::nullopt, std::move(buf));
      },
      files);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(files.data_files.size(), 0);

    assert_dlq_table(ntp.tp.topic, true);
    EXPECT_EQ(files.dlq_files.size(), default_param.hrs);

    EXPECT_EQ(
      get_or_create_probe(ntp)->counter_ref(
        translation_probe::invalid_record_cause::failed_data_translation),
      default_param.num_records());
}

TEST_F(RecordMultiplexerTest, TestBadSchemaChange) {
    constexpr std::string_view avro_incompat_schema_str = R"({
        "type": "record",
        "name": "RootRecord",
        "fields": [
            { "name": "mylong", "doc": "bad type promotion.", "type": "string" }
        ]
    })";
    tests::record_generator gen(&registry);
    auto reg_res
      = gen.register_avro_schema("avro_v1", avro_schema_v1_str).get();
    EXPECT_FALSE(reg_res.has_error()) << reg_res.error();
    reg_res
      = gen.register_avro_schema("incompat", avro_incompat_schema_str).get();
    EXPECT_FALSE(reg_res.has_error()) << reg_res.error();

    // Write with a valid schema.
    auto start_offset = model::offset{0};
    record_multiplexer::finished_files files;
    auto res = mux(
      default_param,
      start_offset++,
      [&gen](storage::record_batch_builder& b) {
          auto res
            = gen.add_random_avro_record(b, "avro_v1", std::nullopt).get();
          ASSERT_FALSE(res.has_error());
      },
      files);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(files.data_files.size(), default_param.hrs);

    // This should have registered the valid schema.
    auto schema = get_current_schema();
    EXPECT_EQ(schema->highest_field_id(), 12);

    files.data_files.clear();
    files.dlq_files.clear();
    // Now try writing with an incompatible schema.
    res = mux(
      default_param,
      start_offset,
      [&gen](storage::record_batch_builder& b) {
          auto res
            = gen.add_random_avro_record(b, "incompat", std::nullopt).get();
          ASSERT_FALSE(res.has_error());
      },
      files);

    // No new files should have been written to the main table.
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(files.data_files.size(), 0);

    // The DLQ table should have the invalid records.
    assert_dlq_table(ntp.tp.topic, true);
    EXPECT_EQ(files.dlq_files.size(), default_param.hrs);

    // The schema for the main table should not have changed.
    schema = get_current_schema();
    EXPECT_EQ(schema->highest_field_id(), 12);

    // Metrics updated.
    EXPECT_EQ(
      get_or_create_probe(ntp)->counter_ref(
        translation_probe::invalid_record_cause::
          failed_iceberg_schema_resolution),
      default_param.num_records());
}

TEST_F(RecordMultiplexerTest, TestMultiplexingFromMiddleOfBatch) {
    // Ensures that we can multiplex from the middle of a batch and respects
    // input start offset
    auto mux = make_mux();
    auto batches = model::test::make_random_batches(
                     {
                       .offset = model::offset{0},
                       .count = 1,
                       .records = 100,
                     })
                     .get();
    auto last_offset = batches.back().last_offset();
    auto start_offset = model::offset(
      random_generators::get_int<model::offset::type>(0, last_offset));
    auto reader = model::make_memory_record_batch_reader(std::move(batches));
    mux
      .multiplex(
        std::move(reader), kafka::offset{start_offset}, model::no_timeout, as)
      .get();
    record_multiplexer::finished_files files;
    auto result = std::move(mux).finish(files).get();
    EXPECT_FALSE(result.has_error()) << result.error();
    EXPECT_EQ(result.value().start_offset(), start_offset);
    EXPECT_EQ(result.value().last_offset(), last_offset);
}

TEST_F(RecordMultiplexerTest, TestRecordTimestamp) {
    // Make sure we respect client vs broker timestamps.
    binary_type_resolver kv_resolver; // This test doesn't need schemas
    direct_table_creator table_creator(kv_resolver, schema_mgr);
    key_value_translator kv_translator;
    auto mux = record_multiplexer(
      ntp,
      topic_rev,
      std::make_unique<test_data_writer_factory>(false),
      schema_mgr,
      kv_resolver,
      kv_translator,
      table_creator,
      model::iceberg_invalid_record_action::dlq_table,
      location_provider(scoped_remote->remote.local().provider(), bucket_name),
      *get_or_create_probe(ntp),
      &features);
    auto batches = model::test::make_random_batches(
                     {
                       .offset = model::offset{0},
                       .count = 2,
                       .records = 100,
                       // Use a timestamp from 1 year ago
                       .timestamp = model::timestamp{1726262280000},
                     })
                     .get();
    // Simulate the second batch is using the broker append time, which is a
    // current timestamp
    batches.back().set_max_timestamp(
      model::timestamp_type::append_time, model::timestamp{1757884680000});
    auto start_offset = batches.front().base_offset();
    auto reader = model::make_memory_record_batch_reader(std::move(batches));
    mux
      .multiplex(
        std::move(reader), kafka::offset{start_offset}, model::no_timeout, as)
      .get();
    record_multiplexer::finished_files files;
    auto result = std::move(mux).finish(files).get();
    ASSERT_FALSE(result.has_error());
    // We should get a partition between the two batches because the different
    // timestamps
    ASSERT_EQ(files.data_files.size(), 2);
    // We don't require the order of the data files.
    EXPECT_THAT(
      files.data_files,
      testing::UnorderedElementsAre(
        testing::Field(
          &partitioning_writer::partitioned_file::partition_key_path,
          remote_path("redpanda.timestamp_hour=2024-09-13-21")),
        testing::Field(
          &partitioning_writer::partitioned_file::partition_key_path,
          remote_path("redpanda.timestamp_hour=2025-09-14-21"))));
    EXPECT_EQ(files.dlq_files.size(), 0);
}
