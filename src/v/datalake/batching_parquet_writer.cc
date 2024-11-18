/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/batching_parquet_writer.h"

#include "base/vlog.h"
#include "bytes/iostream.h"
#include "datalake/arrow_translator.h"
#include "datalake/data_writer_interface.h"
#include "datalake/logger.h"

#include <seastar/core/file-types.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/coroutine/as_future.hh>

#include <exception>
#include <memory>
#include <utility>

namespace datalake {

batching_parquet_writer::batching_parquet_writer(
  const iceberg::struct_type& schema,
  size_t row_count_threshold,
  size_t byte_count_threshold,
  ss::output_stream<char> output)
  : _iceberg_to_arrow(schema)
  , _arrow_to_iobuf(_iceberg_to_arrow.build_arrow_schema())
  , _row_count_threshold{row_count_threshold}
  , _byte_count_threshold{byte_count_threshold}
  , _output_stream(std::move(output)) {}

ss::future<writer_error> batching_parquet_writer::add_data_struct(
  iceberg::struct_value data, size_t approx_size) {
    bool error = false;
    try {
        _iceberg_to_arrow.add_data(std::move(data));
    } catch (...) {
        vlog(
          datalake_log.error,
          "Error adding data value to Arrow table: {}",
          std::current_exception());
        error = true;
    }
    if (error) {
        co_return writer_error::parquet_conversion_error;
    }
    _row_count++;
    _byte_count += approx_size;

    if (
      _row_count >= _row_count_threshold
      || _byte_count > _byte_count_threshold) {
        co_return co_await write_row_group();
    }

    co_return writer_error::ok;
}

ss::future<writer_error> batching_parquet_writer::finish() {
    auto write_result = co_await write_row_group();
    if (write_result != writer_error::ok) {
        co_return writer_error::ok;
    }
    bool error = false;
    iobuf out;
    try {
        out = _arrow_to_iobuf.close_and_take_iobuf();

    } catch (...) {
        vlog(
          datalake_log.error,
          "Error closing arrow_to_iobuf stream: {}",
          std::current_exception());
        error = true;
    }
    if (error) {
        co_return writer_error::parquet_conversion_error;
    }

    try {
        co_await write_iobuf_to_output_stream(std::move(out), _output_stream);
        co_await _output_stream.close();
    } catch (...) {
        vlog(
          datalake_log.error,
          "Error closing output stream: {}",
          std::current_exception());
        error = true;
    }
    if (error) {
        co_return writer_error::file_io_error;
    }

    co_return writer_error::ok;
}

ss::future<writer_error> batching_parquet_writer::write_row_group() {
    if (_row_count == 0) {
        // This can happen if finish() is called when there is no new data.
        co_return writer_error::ok;
    }
    bool error = false;
    iobuf out;
    try {
        auto chunk = _iceberg_to_arrow.take_chunk();
        _row_count = 0;
        _byte_count = 0;
        _arrow_to_iobuf.add_arrow_array(chunk);
        out = _arrow_to_iobuf.take_iobuf();
    } catch (...) {
        vlog(
          datalake_log.error,
          "Error converting Arrow to Parquet iobuf: {}",
          std::current_exception());
        error = true;
    }
    if (error) {
        co_return writer_error::parquet_conversion_error;
    }
    try {
        co_await write_iobuf_to_output_stream(std::move(out), _output_stream);
    } catch (...) {
        vlog(
          datalake_log.error,
          "Error writing to output stream: {}",
          std::current_exception());
        error = true;
    }
    if (error) {
        co_return writer_error::file_io_error;
    }
    co_return writer_error::ok;
}

batching_parquet_writer_factory::batching_parquet_writer_factory(
  size_t row_count_threshold, size_t byte_count_threshold)
  : _row_count_threshold{row_count_threshold}
  , _byte_count_threshold{byte_count_threshold} {}

ss::future<std::unique_ptr<parquet_ostream>>
batching_parquet_writer_factory::create_writer(
  const iceberg::struct_type& schema, ss::output_stream<char> output) {
    co_return std::make_unique<batching_parquet_writer>(
      std::move(schema),
      _row_count_threshold,
      _byte_count_threshold,
      std::move(output));
}
} // namespace datalake
