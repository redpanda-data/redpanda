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
#include "datalake/arrow_translator.h"
#include "datalake/data_writer_interface.h"
#include "datalake/parquet_writer.h"
#include "iceberg/datatypes.h"

#include <seastar/core/file.hh>
#include <seastar/core/iostream.hh>
#include <base/seastarx.h>

#include <filesystem>

namespace datalake {
// batching_parquet_writer ties together the low-level components for iceberg to
// parquet translation to provide a high-level interface for creating parquet
// files from iceberg::value. It:
// 1. Opens a ss::file to store the results
// 2. Accepts iceberg::value and collects them in an arrow_translator
// 3. Once the row count or size threshold is reached it writes data to the
//    file:
//    1. takes a chunk from the arrow_translator
//    2. Adds the chunk to the parquet_writer
//    3. Extracts iobufs from the parquet_writer
//    4. Writes them to the open file
// 4. When finish() is called it flushes all remaining data and closes the
// files.
class batching_parquet_writer {
public:
    batching_parquet_writer(
      iceberg::struct_type schema,
      size_t row_count_threshold,
      size_t byte_count_threshold);

    ss::future<> initialize(std::filesystem::path output_file_path);

    // TODO: error type?
    ss::future<> add_data_struct(iceberg::value data, int64_t approx_size);

    ss::future<data_writer_result> finish();

private:
    ss::future<> write_row_group();

    // translating
    arrow_translator _iceberg_to_arrow;
    arrow_to_iobuf _arrow_to_iobuf;

    // batching
    size_t _row_count_threshold;
    size_t _byte_count_threshold;
    size_t _row_count = 0;
    size_t _byte_count = 0;

    // Output
    ss::file _output_file;
    ss::output_stream<char> _output_stream;
};

} // namespace datalake
