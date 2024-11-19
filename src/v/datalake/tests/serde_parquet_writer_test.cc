#include "bytes/iostream.h"
#include "datalake/serde_parquet_writer.h"
#include "datalake/tests/test_data.h"
#include "iceberg/tests/value_generator.h"

#include <seastar/core/seastar.hh>

#include <gtest/gtest.h>

TEST(SerdeParquetWriterTest, CheckIfTheWriterWritesData) {
    auto schema = test_schema(iceberg::field_required::no);
    iobuf target;

    auto writer = datalake::serde_parquet_writer_factory{}
                    .create_writer(schema, make_iobuf_ref_output_stream(target))
                    .get();

    auto v = iceberg::tests::make_value(
      iceberg::tests::value_spec{.null_pct = 50},
      iceberg::field_type{std::move(schema)});

    auto s_v = std::get<std::unique_ptr<iceberg::struct_value>>(std::move(v));

    auto result = writer->add_data_struct(std::move(*s_v), 0).get();
    ASSERT_EQ(result, datalake::writer_error::ok);
    auto finish_result = writer->finish().get();

    ASSERT_EQ(finish_result, datalake::writer_error::ok);
    ASSERT_GT(target.size_bytes(), 0);
}
