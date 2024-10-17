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
#include "datalake/data_writer_interface.h"
#include "datalake/parquet_writer.h"
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
class batching_parquet_writer : public data_writer {
public:
    batching_parquet_writer(
      iceberg::struct_type schema,
      size_t row_count_threshold,
      size_t byte_count_threshold,
      local_path output_file_path);

    ss::future<checked<std::nullopt_t, data_writer_error>> initialize();

    ss::future<data_writer_error>
    add_data_struct(iceberg::struct_value data, int64_t approx_size) override;

    ss::future<result<local_file_metadata, data_writer_error>>
    finish() override;

    // Close the file handle, delete any temporary data and clean up any other
    // state.
    ss::future<> abort();

private:
    ss::future<data_writer_error> write_row_group();

    // translating
    arrow_translator _iceberg_to_arrow;
    arrow_to_iobuf _arrow_to_iobuf;

    // batching
    size_t _row_count_threshold;
    size_t _byte_count_threshold;
    size_t _row_count = 0;
    size_t _byte_count = 0;
    size_t _total_row_count = 0;
    size_t _total_bytes = 0;

    // Output
    local_path _output_file_path;
    ss::file _output_file;
    ss::output_stream<char> _output_stream;
};

class batching_parquet_writer_factory : public data_writer_factory {
public:
    batching_parquet_writer_factory(
      local_path base_directory,
      ss::sstring file_name_prefix,
      size_t row_count_threshold,
      size_t byte_count_threshold);

    ss::future<result<std::unique_ptr<data_writer>, data_writer_error>>
    create_writer(iceberg::struct_type schema) override;

private:
    local_path create_filename() const;

    local_path _base_directory;
    ss::sstring _file_name_prefix;
    size_t _row_count_threshold;
    size_t _byte_count_threshold;
};

} // namespace datalake
