/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/catalog_schema_manager.h"
#include "datalake/record_multiplexer.h"
#include "datalake/record_schema_resolver.h"
#include "datalake/record_translator.h"
#include "datalake/table_creator.h"
#include "datalake/table_definition.h"
#include "datalake/tests/catalog_and_registry_fixture.h"
#include "datalake/tests/record_generator.h"
#include "datalake/tests/test_data_writer.h"
#include "iceberg/filesystem_catalog.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/circular_buffer.hh>

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
    "fields": [ { "name": "mylong", "doc": "mylong field doc.", "type": "long" } ]
})";
// v2: v1 schema + several others in the struct.
constexpr std::string_view avro_schema_v2_str = R"({
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
};

class RecordMultiplexerTestBase
  : public datalake::tests::catalog_and_registry_fixture {
public:
    RecordMultiplexerTestBase()
      : schema_mgr(catalog)
      , type_resolver(registry)
      , t_creator(type_resolver, schema_mgr) {}

    // Runs the multiplexer on records generated with cb() based on the test
    // parameters.
    std::optional<record_multiplexer::write_result> mux(
      const records_param& param,
      model::offset o,
      const std::function<void(storage::record_batch_builder&)>& cb,
      bool expect_error = false) {
        auto start_offset = o;
        ss::circular_buffer<model::record_batch> batches;
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
        auto reader = model::make_memory_record_batch_reader(
          std::move(batches));
        record_multiplexer mux(
          ntp,
          topic_rev,
          std::make_unique<test_data_writer_factory>(false),
          schema_mgr,
          type_resolver,
          translator,
          t_creator);
        auto res = reader.consume(std::move(mux), model::no_timeout).get();
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

    catalog_schema_manager schema_mgr;
    record_schema_resolver type_resolver;
    direct_table_creator t_creator;

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
      bool expect_error = false) {
        return RecordMultiplexerTestBase::mux(GetParam(), o, cb, expect_error);
    }
};

TEST_P(RecordMultiplexerParamTest, TestNoSchema) {
    auto start_offset = model::offset{0};
    auto res = mux(
      start_offset,
      [](storage::record_batch_builder& b) {
          b.add_raw_kv(std::nullopt, iobuf::from("foobar"));
      },
      true);
    ASSERT_FALSE(res.has_value());
}

TEST_P(RecordMultiplexerParamTest, TestSimpleAvroRecords) {
    tests::record_generator gen(&registry);
    auto reg_res
      = gen.register_avro_schema("avro_v1", avro_schema_v1_str).get();
    EXPECT_FALSE(reg_res.has_error()) << reg_res.error();

    // Add Avro records.
    auto start_offset = model::offset{0};
    auto res = mux(start_offset, [&gen](storage::record_batch_builder& b) {
        auto res = gen.add_random_avro_record(b, "avro_v1", std::nullopt).get();
        ASSERT_FALSE(res.has_error());
    });
    ASSERT_TRUE(res.has_value());
    const auto& write_res = res.value();
    EXPECT_EQ(write_res.data_files.size(), GetParam().hrs);

    std::unordered_set<int> hrs;
    for (auto& f : write_res.data_files) {
        hrs.emplace(f.hour);
        EXPECT_EQ(f.row_count, GetParam().records_per_hr());
    }
    EXPECT_EQ(hrs.size(), GetParam().hrs);

    // 4 default columns + RootRecord + mylong
    auto schema = get_current_schema();
    EXPECT_EQ(schema->highest_field_id(), 10);
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
    auto res = mux(start_offset, [&gen, &i](storage::record_batch_builder& b) {
        auto res = gen
                     .add_random_avro_record(
                       b, (++i % 2) ? "avro_v1" : "avro_v2", std::nullopt)
                     .get();
        ASSERT_FALSE(res.has_error());
    });
    ASSERT_TRUE(res.has_value());
    const auto& write_res = res.value();

    // There should be twice as many files as normal, since we have twice the
    // schemas.
    EXPECT_EQ(write_res.data_files.size(), 2 * GetParam().hrs);

    std::unordered_set<int> hrs;
    for (auto& f : write_res.data_files) {
        hrs.emplace(f.hour);
        // Each file should have half the records as normal, since we have
        // twice the files.
        EXPECT_EQ(f.row_count, GetParam().records_per_hr() / 2);
    }
    EXPECT_EQ(hrs.size(), GetParam().hrs);
    auto schema = get_current_schema();
    EXPECT_EQ(schema->highest_field_id(), 20);
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
    auto res = mux(
      default_param, start_offset, [&gen](storage::record_batch_builder& b) {
          auto res
            = gen.add_random_avro_record(b, "avro_rp", std::nullopt).get();
          ASSERT_FALSE(res.has_error());
      });
    ASSERT_TRUE(res.has_value());
    const auto& write_res = res.value();
    EXPECT_EQ(write_res.data_files.size(), default_param.hrs);

    std::unordered_set<int> hrs;
    for (auto& f : write_res.data_files) {
        hrs.emplace(f.hour);
        EXPECT_EQ(f.row_count, default_param.records_per_hr());
    }
    EXPECT_EQ(hrs.size(), default_param.hrs);

    // 1 nested redpanda column + 4 default columns + mylong + 1 user redpanda
    // column + 1 nested
    auto schema = get_current_schema();
    EXPECT_EQ(schema->highest_field_id(), 12);

    // The redpanda system fields should include the 'data' column.
    const auto& rp_struct = std::get<iceberg::struct_type>(
      schema->schema_struct.fields[0]->type);
    EXPECT_EQ(6, rp_struct.fields.size());
    EXPECT_EQ("data", rp_struct.fields.back()->name);
}

TEST_F(RecordMultiplexerTest, TestMissingSchema) {
    auto start_offset = model::offset{0};
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
      true);
    ASSERT_FALSE(res.has_value());
}

TEST_F(RecordMultiplexerTest, TestBadData) {
    tests::record_generator gen(&registry);
    auto reg_res
      = gen.register_avro_schema("avro_v1", avro_schema_v1_str).get();
    EXPECT_FALSE(reg_res.has_error()) << reg_res.error();

    auto start_offset = model::offset{0};
    auto res = mux(
      default_param,
      start_offset,
      [](storage::record_batch_builder& b) {
          iobuf buf;
          // Append data with a magic bytes that corresponds to the actual
          // schema.
          buf.append("\0\0\0\0\0\1\0", 7);
          b.add_raw_kv(std::nullopt, std::move(buf));
      },
      true);
    ASSERT_FALSE(res.has_value());
}

TEST_F(RecordMultiplexerTest, TestBadSchemaChange) {
    constexpr std::string_view avro_incompat_schema_str = R"({
        "type": "record",
        "name": "RootRecord",
        "fields": [
            { "name": "wrongname", "doc": "mylong field doc.", "type": "long" },
            { "name": "wrongname2", "doc": "mylong field doc.", "type": "long" }
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
    auto res = mux(
      default_param, start_offset++, [&gen](storage::record_batch_builder& b) {
          auto res
            = gen.add_random_avro_record(b, "avro_v1", std::nullopt).get();
          ASSERT_FALSE(res.has_error());
      });
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res.value().data_files.size(), default_param.hrs);

    // This should have registered the valid schema.
    auto schema = get_current_schema();
    EXPECT_EQ(schema->highest_field_id(), 10);

    // Now try writing with an incompatible schema.
    res = mux(
      default_param,
      start_offset,
      [&gen](storage::record_batch_builder& b) {
          auto res
            = gen.add_random_avro_record(b, "incompat", std::nullopt).get();
          ASSERT_FALSE(res.has_error());
      },
      true);

    // This should successfully write the binary records but not update the
    // schema.
    ASSERT_FALSE(res.has_value());
    schema = get_current_schema();
    EXPECT_EQ(schema->highest_field_id(), 10);
}
