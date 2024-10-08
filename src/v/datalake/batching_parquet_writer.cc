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

#include "bytes/iostream.h"
#include "datalake/arrow_translator.h"
#include "datalake/data_writer_interface.h"

#include <seastar/core/file-types.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/coroutine/as_future.hh>

#include <cstdint>

namespace datalake {

batching_parquet_writer::batching_parquet_writer(
  iceberg::struct_type schema,
  size_t row_count_threshold,
  size_t byte_count_threshold)
  : _iceberg_to_arrow(std::move(schema))
  , _arrow_to_iobuf(_iceberg_to_arrow.build_arrow_schema())
  , _row_count_threshold{row_count_threshold}
  , _byte_count_threshold{byte_count_threshold} {}

ss::future<data_writer_error>
batching_parquet_writer::initialize(std::filesystem::path output_file_path) {
    _output_file_path = std::move(output_file_path);
    try {
        _output_file = co_await ss::open_file_dma(
          _output_file_path.string(),
          ss::open_flags::create | ss::open_flags::truncate
            | ss::open_flags::wo);
    } catch (...) {
        co_return data_writer_error::file_io_error;
    }
    bool error = false;
    try {
        _output_stream = co_await ss::make_file_output_stream(_output_file);
    } catch (...) {
        error = true;
    }
    if (error) {
        co_await _output_file.close();
        co_return data_writer_error::file_io_error;
    }

    _result.file_path = _output_file_path.string();
    co_return data_writer_error::ok;
}

ss::future<data_writer_error> batching_parquet_writer::add_data_struct(
  iceberg::struct_value data, int64_t approx_size) {
    bool error = false;
    try {
        _iceberg_to_arrow.add_data(std::move(data));
    } catch (...) {
        error = true;
    }
    if (error) {
        co_await abort();
        co_return data_writer_error::parquet_conversion_error;
    }
    _row_count++;
    _byte_count += approx_size;

    if (
      _row_count >= _row_count_threshold
      || _byte_count > _byte_count_threshold) {
        co_return co_await write_row_group();
    }
    co_return data_writer_error::ok;
}

ss::future<result<data_writer_result, data_writer_error>>
batching_parquet_writer::finish() {
    auto write_result = co_await write_row_group();
    if (write_result != data_writer_error::ok) {
        co_await abort();
        co_return write_result;
    }
    bool error = false;
    iobuf out;
    try {
        out = _arrow_to_iobuf.close_and_take_iobuf();
        _result.file_size_bytes += out.size_bytes();
    } catch (...) {
        error = true;
    }
    if (error) {
        co_await abort();
        co_return data_writer_error::parquet_conversion_error;
    }

    try {
        co_await write_iobuf_to_output_stream(std::move(out), _output_stream);
        co_await _output_stream.close();
    } catch (...) {
        error = true;
    }
    if (error) {
        co_await abort();
        co_return data_writer_error::file_io_error;
    }

    co_return _result;
}

ss::future<data_writer_error> batching_parquet_writer::write_row_group() {
    if (_row_count == 0) {
        // This can happen if finish() is called when there is no new data.
        co_return data_writer_error::ok;
    }
    bool error = false;
    iobuf out;
    try {
        auto chunk = _iceberg_to_arrow.take_chunk();
        _result.record_count += _row_count;
        _row_count = 0;
        _byte_count = 0;
        _arrow_to_iobuf.add_arrow_array(chunk);
        out = _arrow_to_iobuf.take_iobuf();
        _result.file_size_bytes += out.size_bytes();
    } catch (...) {
        error = true;
    }
    if (error) {
        co_await abort();
        co_return data_writer_error::parquet_conversion_error;
    }
    try {
        co_await write_iobuf_to_output_stream(std::move(out), _output_stream);
    } catch (...) {
        error = true;
    }
    if (error) {
        co_await abort();
        co_return data_writer_error::file_io_error;
    }
    co_return data_writer_error::ok;
}

ss::future<> batching_parquet_writer::abort() {
    co_await _output_stream.close();
    auto exists = co_await ss::file_exists(_output_file_path.c_str());
    if (exists) {
        co_await ss::remove_file(_output_file_path.c_str());
    }
}

} // namespace datalake
