/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once
#include "base/outcome.h"
#include "datalake/arrow_translator.h"
#include "datalake/arrow_writer.h"
#include "datalake/data_writer_interface.h"
#include "iceberg/datatypes.h"

#include <seastar/core/file.hh>
#include <seastar/core/iostream.hh>

#include <base/seastarx.h>

#include <filesystem>
#include <memory>

namespace datalake {
// batching_parquet_writer ties together the low-level components for iceberg to
// parquet translation to provide a high-level interface for creating parquet
// files from iceberg::value. It:
// 1. Accepts iceberg::value and collects them in an arrow_translator
// 2. Once the row count or size threshold is reached it writes data to the
//    output stream:
//    1. takes a chunk from the arrow_translator
//    2. Adds the chunk to the parquet_writer
//    3. Extracts iobufs from the parquet_writer
//    4. Writes them to the stream
// 4. When finish() is called it flushes all remaining data and closes the
// stream.
class batching_parquet_writer : public parquet_ostream {
public:
    batching_parquet_writer(
      const iceberg::struct_type& schema,
      size_t row_count_threshold,
      size_t byte_count_threshold,
      ss::output_stream<char> output_stream);

    ss::future<writer_error>
    add_data_struct(iceberg::struct_value data, size_t approx_size) override;

    ss::future<writer_error> finish() override;

private:
    ss::future<writer_error> write_row_group();

    // translating
    arrow_translator _iceberg_to_arrow;
    arrow_to_iobuf _arrow_to_iobuf;

    // batching
    size_t _row_count_threshold;
    size_t _byte_count_threshold;
    size_t _row_count = 0;
    size_t _byte_count = 0;
    // Output
    ss::output_stream<char> _output_stream;
};

class batching_parquet_writer_factory : public parquet_ostream_factory {
public:
    batching_parquet_writer_factory(
      size_t row_count_threshold, size_t byte_count_threshold);

    ss::future<std::unique_ptr<parquet_ostream>> create_writer(
      const iceberg::struct_type& schema,
      ss::output_stream<char> output) override;

private:
    size_t _row_count_threshold;
    size_t _byte_count_threshold;
};

} // namespace datalake
