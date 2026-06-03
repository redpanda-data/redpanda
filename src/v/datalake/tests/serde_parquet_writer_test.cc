#include "bytes/bytes.h"
#include "bytes/iostream.h"
#include "datalake/serde_parquet_writer.h"
#include "datalake/tests/test_data.h"
#include "datalake/tests/test_data_writer.h"
#include "iceberg/datatypes.h"
#include "iceberg/tests/value_generator.h"
#include "iceberg/values.h"

#include <seastar/net/byteorder.hh>

#include <gtest/gtest.h>

#include <bit>
#include <cstring>
#include <limits>

TEST(SerdeParquetWriterTest, CheckIfTheWriterWritesData) {
    auto schema = test_schema(iceberg::field_required::no);
    iobuf target;

    datalake::noop_mem_tracker mem_tracker;
    auto writer = datalake::serde_parquet_writer_factory{}
                    .create_writer(
                      schema, make_iobuf_ref_output_stream(target), mem_tracker)
                    .get();

    auto v = iceberg::tests::make_value(
      iceberg::tests::value_spec{.null_pct = 50},
      iceberg::field_type{std::move(schema)});

    auto s_v = std::get<std::unique_ptr<iceberg::struct_value>>(std::move(v));

    ss::abort_source as;
    auto result = writer->add_data_struct(std::move(*s_v), 0, as).get();
    ASSERT_EQ(result, datalake::writer_error::ok);
    auto finish_result = writer->finish().get();

    ASSERT_EQ(finish_result, datalake::writer_error::ok);
    ASSERT_GT(target.size_bytes(), 0);
}

TEST(SerdeParquetWriterTest, ValidateWriterBehaviorOnOOM) {
    auto schema = test_schema(iceberg::field_required::no);
    iobuf target;

    datalake::noop_mem_tracker mem_tracker;
    auto writer = datalake::serde_parquet_writer_factory{}
                    .create_writer(
                      schema, make_iobuf_ref_output_stream(target), mem_tracker)
                    .get();

    auto v = iceberg::tests::make_value(
      iceberg::tests::value_spec{.null_pct = 50},
      iceberg::field_type{std::move(schema)});

    auto s_v = std::get<std::unique_ptr<iceberg::struct_value>>(std::move(v));

    ss::abort_source as;
    mem_tracker.inject_oom_on_next_reserve();
    auto result = writer->add_data_struct(std::move(*s_v), 0, as).get();

    auto finish_result = writer->finish().get();
    ASSERT_EQ(finish_result, datalake::writer_error::ok);

    if (target.size_bytes() > 0) {
        // If the value was written despite the OOM then the writer is required
        // to return `ok`.
        ASSERT_EQ(result, datalake::writer_error::ok);

    } else {
        // Otherwise the result should be some error.
        ASSERT_NE(result, datalake::writer_error::ok);
    }
}

// Verify that column_stats() returns correct min/max bounds, null counts, and
// value counts for INT64 and STRING columns after finish().
TEST(SerdeParquetWriterTest, ColumnStats) {
    using namespace iceberg;

    // Schema: required long (field_id=1), optional string (field_id=2),
    // optional float (field_id=3, mixed NaN+real), optional double
    // (field_id=4, all NaN — bounds must be absent).
    struct_type schema;
    schema.fields.emplace_back(
      nested_field::create(1, "my_long", field_required::yes, long_type{}));
    schema.fields.emplace_back(
      nested_field::create(2, "my_string", field_required::no, string_type{}));
    schema.fields.emplace_back(
      nested_field::create(3, "my_float", field_required::no, float_type{}));
    schema.fields.emplace_back(
      nested_field::create(4, "my_double", field_required::no, double_type{}));

    iobuf target;
    datalake::noop_mem_tracker mem_tracker;
    ss::abort_source as;

    auto writer = datalake::serde_parquet_writer_factory{}
                    .create_writer(
                      schema, make_iobuf_ref_output_stream(target), mem_tracker)
                    .get();

    const float nan_f = std::numeric_limits<float>::quiet_NaN();
    const double nan_d = std::numeric_limits<double>::quiet_NaN();

    // Row 1: long=5, string="banana", float=NaN, double=NaN
    {
        struct_value sv;
        sv.fields.push_back(value{primitive_value{long_value{5}}});
        sv.fields.push_back(
          value{primitive_value{string_value{iobuf::from("banana")}}});
        sv.fields.push_back(value{primitive_value{float_value{nan_f}}});
        sv.fields.push_back(value{primitive_value{double_value{nan_d}}});
        ASSERT_EQ(
          writer->add_data_struct(std::move(sv), 0, as).get(),
          datalake::writer_error::ok);
    }
    // Row 2: long=3, string=null, float=2.0, double=NaN
    {
        struct_value sv;
        sv.fields.push_back(value{primitive_value{long_value{3}}});
        sv.fields.push_back(std::nullopt);
        sv.fields.push_back(value{primitive_value{float_value{2.0f}}});
        sv.fields.push_back(value{primitive_value{double_value{nan_d}}});
        ASSERT_EQ(
          writer->add_data_struct(std::move(sv), 0, as).get(),
          datalake::writer_error::ok);
    }
    // Row 3: long=7, string="apple", float=5.0, double=NaN
    {
        struct_value sv;
        sv.fields.push_back(value{primitive_value{long_value{7}}});
        sv.fields.push_back(
          value{primitive_value{string_value{iobuf::from("apple")}}});
        sv.fields.push_back(value{primitive_value{float_value{5.0f}}});
        sv.fields.push_back(value{primitive_value{double_value{nan_d}}});
        ASSERT_EQ(
          writer->add_data_struct(std::move(sv), 0, as).get(),
          datalake::writer_error::ok);
    }

    ASSERT_EQ(writer->finish().get(), datalake::writer_error::ok);
    auto stats = writer->column_stats();
    ASSERT_FALSE(stats.empty());

    auto find_stat = [&](int32_t fid) -> const datalake::per_column_stats* {
        for (const auto& s : stats) {
            if (s.field_id == fid) {
                return &s;
            }
        }
        return nullptr;
    };

    // INT64 column (field_id=1): min=3, max=7, no nulls.
    const auto* ls = find_stat(1);
    ASSERT_NE(ls, nullptr);
    EXPECT_EQ(ls->value_count, 3);
    EXPECT_EQ(ls->null_value_count, 0);
    EXPECT_GT(ls->column_size_bytes, 0);
    ASSERT_TRUE(ls->lower_bound.has_value());
    ASSERT_TRUE(ls->upper_bound.has_value());
    // Bounds are in Iceberg/Parquet PLAIN format: 8-byte little-endian int64.
    auto le_int64_bytes = [](int64_t v) -> bytes {
        bytes b{bytes::initialized_later{}, sizeof(int64_t)};
        int64_t lev = ss::cpu_to_le(v);
        std::memcpy(b.data(), &lev, sizeof(int64_t));
        return b;
    };
    EXPECT_EQ(*ls->lower_bound, le_int64_bytes(3));
    EXPECT_EQ(*ls->upper_bound, le_int64_bytes(7));

    // STRING column (field_id=2): min="apple", max="banana", 1 null.
    const auto* ss_stat = find_stat(2);
    ASSERT_NE(ss_stat, nullptr);
    EXPECT_EQ(ss_stat->value_count, 3);
    EXPECT_EQ(ss_stat->null_value_count, 1);
    EXPECT_GT(ss_stat->column_size_bytes, 0);
    ASSERT_TRUE(ss_stat->lower_bound.has_value());
    ASSERT_TRUE(ss_stat->upper_bound.has_value());
    EXPECT_EQ(*ss_stat->lower_bound, bytes::from_string("apple"));
    EXPECT_EQ(*ss_stat->upper_bound, bytes::from_string("banana"));

    auto le_float_bytes = [](float v) -> bytes {
        bytes b{bytes::initialized_later{}, sizeof(float)};
        uint32_t bits = ss::cpu_to_le(std::bit_cast<uint32_t>(v));
        std::memcpy(b.data(), &bits, sizeof(float));
        return b;
    };

    // FLOAT column (field_id=3): NaN excluded → min=2.0, max=5.0.
    const auto* fs = find_stat(3);
    ASSERT_NE(fs, nullptr);
    EXPECT_EQ(fs->value_count, 3);
    ASSERT_TRUE(fs->lower_bound.has_value());
    ASSERT_TRUE(fs->upper_bound.has_value());
    EXPECT_EQ(*fs->lower_bound, le_float_bytes(2.0f));
    EXPECT_EQ(*fs->upper_bound, le_float_bytes(5.0f));

    // DOUBLE column (field_id=4): all NaN → bounds must be absent.
    const auto* ds = find_stat(4);
    ASSERT_NE(ds, nullptr);
    EXPECT_EQ(ds->value_count, 3);
    EXPECT_FALSE(ds->lower_bound.has_value());
    EXPECT_FALSE(ds->upper_bound.has_value());
}

// Verify that file-level column stats merge correctly across multiple row
// groups: rows written before flush() and rows written after flush() must both
// contribute to the final min/max/null_count/value_count.
TEST(SerdeParquetWriterTest, ColumnStatsMultipleRowGroups) {
    using namespace iceberg;

    struct_type schema;
    schema.fields.emplace_back(
      nested_field::create(1, "val", field_required::no, long_type{}));

    iobuf target;
    datalake::noop_mem_tracker mem_tracker;
    ss::abort_source as;

    auto writer = datalake::serde_parquet_writer_factory{}
                    .create_writer(
                      schema, make_iobuf_ref_output_stream(target), mem_tracker)
                    .get();

    auto add = [&](std::optional<int64_t> v) {
        struct_value sv;
        if (v) {
            sv.fields.push_back(value{primitive_value{long_value{*v}}});
        } else {
            sv.fields.push_back(std::nullopt);
        }
        ASSERT_EQ(
          writer->add_data_struct(std::move(sv), 0, as).get(),
          datalake::writer_error::ok);
    };

    // Row group 1: values 10, null, 30 → min=10, max=30, 1 null
    add(10);
    add(std::nullopt);
    add(30);
    writer->flush().get();

    // Row group 2: values 5, 20 → min=5, max=20, 0 nulls
    add(5);
    add(20);

    ASSERT_EQ(writer->finish().get(), datalake::writer_error::ok);
    auto stats = writer->column_stats();
    ASSERT_EQ(stats.size(), 1u);

    const auto& s = stats[0];
    EXPECT_EQ(s.field_id, 1);
    EXPECT_EQ(s.value_count, 5);
    EXPECT_EQ(s.null_value_count, 1);

    auto le_int64_bytes = [](int64_t v) -> bytes {
        bytes b{bytes::initialized_later{}, sizeof(int64_t)};
        int64_t lev = ss::cpu_to_le(v);
        std::memcpy(b.data(), &lev, sizeof(int64_t));
        return b;
    };
    // File-level min must come from row group 2 (5), max from row group 1 (30).
    ASSERT_TRUE(s.lower_bound.has_value());
    ASSERT_TRUE(s.upper_bound.has_value());
    EXPECT_EQ(*s.lower_bound, le_int64_bytes(5));
    EXPECT_EQ(*s.upper_bound, le_int64_bytes(30));
}
