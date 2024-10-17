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

#include <cstdint>
#include <exception>
#include <memory>
#include <utility>

namespace datalake {

batching_parquet_writer::batching_parquet_writer(
  iceberg::struct_type schema,
  size_t row_count_threshold,
  size_t byte_count_threshold,
  local_path output_file_path)
  : _iceberg_to_arrow(std::move(schema))
  , _arrow_to_iobuf(_iceberg_to_arrow.build_arrow_schema())
  , _row_count_threshold{row_count_threshold}
  , _byte_count_threshold{byte_count_threshold}
  , _output_file_path(std::move(output_file_path)) {}

ss::future<checked<std::nullopt_t, data_writer_error>>
batching_parquet_writer::initialize() {
    vlog(datalake_log.info, "Writing Parquet file to {}", _output_file_path);
    try {
        _output_file = co_await ss::open_file_dma(
          _output_file_path().string(),
          ss::open_flags::create | ss::open_flags::truncate
            | ss::open_flags::wo);
    } catch (...) {
        vlog(
          datalake_log.error,
          "Error opening output file {} - {}",
          _output_file_path,
          std::current_exception());
        co_return data_writer_error::file_io_error;
    }
    bool error = false;
    try {
        _output_stream = co_await ss::make_file_output_stream(_output_file);
    } catch (...) {
        vlog(
          datalake_log.error,
          "Error making output stream for file {} - {}",
          _output_file_path,
          std::current_exception());
        error = true;
    }
    if (error) {
        co_await _output_file.close();
        co_return data_writer_error::file_io_error;
    }

    co_return std::nullopt;
}

ss::future<data_writer_error> batching_parquet_writer::add_data_struct(
  iceberg::struct_value data, int64_t approx_size) {
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

ss::future<result<local_file_metadata, data_writer_error>>
batching_parquet_writer::finish() {
    local_file_metadata file_meta;
    auto write_result = co_await write_row_group();
    if (write_result != data_writer_error::ok) {
        co_await abort();
        co_return write_result;
    }
    bool error = false;
    iobuf out;
    try {
        out = _arrow_to_iobuf.close_and_take_iobuf();
        _total_bytes += out.size_bytes();

    } catch (...) {
        vlog(
          datalake_log.error,
          "Error closing arrow_to_iobuf stream: {}",
          std::current_exception());
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
        vlog(
          datalake_log.error,
          "Error closing output stream: {}",
          std::current_exception());
        error = true;
    }
    if (error) {
        co_await abort();
        co_return data_writer_error::file_io_error;
    }

    co_return local_file_metadata{
      .path = _output_file_path,
      .row_count = _total_row_count,
      .size_bytes = _total_bytes,
    };
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
        _total_row_count += _row_count;
        _row_count = 0;
        _byte_count = 0;
        _arrow_to_iobuf.add_arrow_array(chunk);
        out = _arrow_to_iobuf.take_iobuf();
        _total_bytes += out.size_bytes();
    } catch (...) {
        vlog(
          datalake_log.error,
          "Error converting Arrow to Parquet iobuf: {}",
          std::current_exception());
        error = true;
    }
    if (error) {
        co_await abort();
        co_return data_writer_error::parquet_conversion_error;
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
        co_await abort();
        co_return data_writer_error::file_io_error;
    }
    co_return data_writer_error::ok;
}

ss::future<> batching_parquet_writer::abort() {
    co_await _output_stream.close();
    auto exists = co_await ss::file_exists(_output_file_path().string());
    if (exists) {
        co_await ss::remove_file(_output_file_path().string());
    }
}

batching_parquet_writer_factory::batching_parquet_writer_factory(
  local_path base_directory,
  ss::sstring file_name_prefix,
  size_t row_count_threshold,
  size_t byte_count_threshold)
  : _base_directory{std::move(base_directory)}
  , _file_name_prefix{std::move(file_name_prefix)}
  , _row_count_threshold{row_count_threshold}
  , _byte_count_threshold{byte_count_threshold} {}

local_path batching_parquet_writer_factory::create_filename() const {
    return local_path{
      _base_directory()
      / fmt::format("{}-{}.parquet", _file_name_prefix, uuid_t::create())};
}
ss::future<result<std::unique_ptr<data_writer>, data_writer_error>>
batching_parquet_writer_factory::create_writer(iceberg::struct_type schema) {
    auto ret = std::make_unique<batching_parquet_writer>(
      std::move(schema),
      _row_count_threshold,
      _byte_count_threshold,
      create_filename());

    auto result = co_await ret->initialize();
    if (result.has_error()) {
        co_return result.error();
    }
    co_return ret;
}
} // namespace datalake
