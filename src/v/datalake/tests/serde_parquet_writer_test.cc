#include "bytes/iostream.h"
#include "datalake/serde_parquet_writer.h"
#include "datalake/tests/test_data.h"
#include "datalake/tests/test_data_writer.h"
#include "iceberg/tests/value_generator.h"

#include <seastar/core/seastar.hh>

#include <gtest/gtest.h>

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
