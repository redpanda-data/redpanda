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
#include "datalake/parquet_write_config.h"
#include "datalake/tests/test_data.h"
#include "datalake/tests/test_data_writer.h"
#include "iceberg/datatypes.h"
#include "iceberg/tests/value_generator.h"
#include "serde/parquet/writer.h"
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
    ~test_writer() {
        vassert(stream_closed_, "Ensure output stream is closed in all cases");
    }
    ss::future<datalake::writer_error>
    add_data_struct(iceberg::struct_value, size_t, ss::abort_source&) final {
        if (rows_ >= error_after_rows_) {
            co_return datalake::writer_error::file_io_error;
        }
        rows_++;

        co_return datalake::writer_error::ok;
    };

    size_t buffered_bytes() const final { return 0; };
    size_t flushed_bytes() const final { return 0; }
    ss::future<> flush() final { return ss::make_ready_future<>(); }

    ss::future<datalake::writer_error> finish() final {
        co_await os_.close();
        stream_closed_ = true;
        if (error_on_finish_) {
            co_return datalake::writer_error::file_io_error;
        }
        co_return datalake::writer_error::ok;
    }

    size_t error_after_rows_;
    bool error_on_finish_;
    size_t rows_{0};
    bool stream_closed_;
    ss::output_stream<char> os_;
};

struct test_writer_factory : datalake::parquet_ostream_factory {
    explicit test_writer_factory(
      size_t error_after_rows = std::numeric_limits<size_t>::max(),
      bool error_on_finish = false)
      : error_after_rows_(error_after_rows)
      , error_on_finish_(error_on_finish) {}

    ss::future<std::unique_ptr<datalake::parquet_ostream>> create_writer(
      const iceberg::struct_type&,
      const datalake::parquet_write_config&,
      ss::output_stream<char> os,
      datalake::writer_mem_tracker&) final {
        co_return std::make_unique<test_writer>(
          error_after_rows_, error_on_finish_, std::move(os));
    };

    size_t error_after_rows_;
    bool error_on_finish_;
};

struct recording_mem_tracker : datalake::writer_mem_tracker {
    ss::future<datalake::reservation_error>
    reserve_bytes(size_t n, ss::abort_source&) noexcept final {
        last_reserved = n;
        co_return datalake::reservation_error::ok;
    }
    ss::future<> free_bytes(size_t, ss::abort_source&) final {
        return ss::now();
    }
    void release() final {}
    datalake::writer_disk_tracker& disk() final { return disk_.disk(); }

    size_t last_reserved{0};
    datalake::noop_mem_tracker disk_;
};

iceberg::struct_type make_flat_int_schema(size_t n_fields) {
    iceberg::struct_type schema;
    for (size_t i = 0; i < n_fields; ++i) {
        schema.fields.push_back(
          iceberg::nested_field::create(
            static_cast<int32_t>(i + 1),
            fmt::format("f{}", i),
            iceberg::field_required::no,
            iceberg::int_type{}));
    }
    return schema;
}

// A struct of two ints, a list of int, and a map of int to int: five leaves
// behind three top-level fields.
iceberg::struct_type make_nested_schema() {
    iceberg::struct_type inner;
    inner.fields.push_back(
      iceberg::nested_field::create(
        10, "a", iceberg::field_required::no, iceberg::int_type{}));
    inner.fields.push_back(
      iceberg::nested_field::create(
        11, "b", iceberg::field_required::no, iceberg::int_type{}));

    iceberg::struct_type schema;
    schema.fields.push_back(
      iceberg::nested_field::create(
        1, "s", iceberg::field_required::no, std::move(inner)));
    schema.fields.push_back(
      iceberg::nested_field::create(
        2,
        "l",
        iceberg::field_required::no,
        iceberg::list_type::create(
          20, iceberg::field_required::no, iceberg::int_type{})));
    schema.fields.push_back(
      iceberg::nested_field::create(
        3,
        "m",
        iceberg::field_required::no,
        iceberg::map_type::create(
          30,
          iceberg::int_type{},
          31,
          iceberg::field_required::no,
          iceberg::int_type{})));
    return schema;
}

} // namespace

struct LocalFileWriterTest : public testing::Test {
    // Sets up the test fixture.
    void SetUp() final {}

    // Tears down the test fixture.
    void TearDown() final {}

    temporary_dir tmp_dir = temporary_dir("batching_parquet_writer");
    std::filesystem::path file_path = "test_file.parquet";
    std::filesystem::path full_path = tmp_dir.get_path() / file_path;
    datalake::noop_mem_tracker mem_tracker;
    ss::abort_source as;
};

TEST_F(LocalFileWriterTest, TestHappyPath) {
    datalake::local_parquet_file_writer file_writer(
      datalake::local_path(full_path),
      ss::make_shared<test_writer_factory>(),
      mem_tracker);

    auto schema = test_schema(iceberg::field_required::no);
    file_writer.initialize(schema, datalake::parquet_write_config{}).get();

    size_t rows = 1000;
    for (size_t i = 0; i < rows; i++) {
        auto data = iceberg::tests::make_struct_value(
          iceberg::tests::value_spec{},
          test_schema(iceberg::field_required::no));

        auto res = file_writer.add_data_struct(std::move(data), 1000, as).get();
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
      ss::make_shared<test_writer_factory>(100),
      mem_tracker);
    auto schema = test_schema(iceberg::field_required::no);
    file_writer.initialize(schema, datalake::parquet_write_config{}).get();

    size_t rows = 1000;
    for (size_t i = 0; i < rows; i++) {
        auto data = iceberg::tests::make_struct_value(
          iceberg::tests::value_spec{},
          test_schema(iceberg::field_required::no));

        file_writer.add_data_struct(std::move(data), 1000, as).get();
    }

    auto result = file_writer.finish().get();

    ASSERT_TRUE(result.has_error());

    // intermediate file shuld be removed
    ASSERT_FALSE(std::filesystem::exists(full_path));
}

TEST_F(LocalFileWriterTest, TestErrorOnFinish) {
    datalake::local_parquet_file_writer file_writer(
      datalake::local_path(full_path),
      ss::make_shared<test_writer_factory>(5000, true),
      mem_tracker);
    auto schema = test_schema(iceberg::field_required::no);
    file_writer.initialize(schema, datalake::parquet_write_config{}).get();

    size_t rows = 1000;
    for (size_t i = 0; i < rows; i++) {
        auto data = iceberg::tests::make_struct_value(
          iceberg::tests::value_spec{},
          test_schema(iceberg::field_required::no));

        file_writer.add_data_struct(std::move(data), 1000, as).get();
    }

    auto result = file_writer.finish().get();

    ASSERT_TRUE(result.has_error());

    // intermediate file should be removed
    ASSERT_FALSE(std::filesystem::exists(full_path));
}

TEST_F(LocalFileWriterTest, ReservationScalesWithLeafColumns) {
    recording_mem_tracker tracker;
    datalake::local_parquet_file_writer_factory factory(
      datalake::local_path(tmp_dir.get_path()),
      "test-prefix",
      ss::make_shared<test_writer_factory>(),
      tracker);

    auto reserved_for = [&](size_t n_leaves) -> size_t {
        tracker.last_reserved = 0;
        auto res = factory
                     .create_writer(
                       make_flat_int_schema(n_leaves),
                       datalake::parquet_write_config{},
                       as)
                     .get();
        EXPECT_FALSE(res.has_error());
        // Close the writer's stream so its test_writer doesn't assert on drop.
        std::move(res.value())->finish().get();
        // NOTE: The memory reserved for the writer should include more than
        // just the columns.
        EXPECT_GE(
          tracker.last_reserved,
          serde::parquet::writer::estimated_memory(n_leaves));
        return tracker.last_reserved;
    };

    auto r1 = reserved_for(1);
    auto r2 = reserved_for(2);
    auto r4 = reserved_for(4);

    EXPECT_GT(r1, 0u);
    EXPECT_GT(r2, r1);
    EXPECT_GT(r4, r2);

    tracker.last_reserved = 0;
    auto nested_res = factory
                        .create_writer(
                          make_nested_schema(),
                          datalake::parquet_write_config{},
                          as)
                        .get();
    EXPECT_FALSE(nested_res.has_error());
    std::move(nested_res.value())->finish().get();
    EXPECT_EQ(tracker.last_reserved, reserved_for(5));
}
