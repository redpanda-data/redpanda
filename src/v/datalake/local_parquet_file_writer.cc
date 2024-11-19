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

#include "base/vlog.h"
#include "datalake/logger.h"

#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/coroutine/as_future.hh>

namespace datalake {

local_parquet_file_writer::local_parquet_file_writer(
  local_path output_file_path,
  ss::shared_ptr<parquet_ostream_factory> writer_factory)
  : _output_file_path(std::move(output_file_path))
  , _writer_factory(std::move(writer_factory)) {}

ss::future<checked<std::nullopt_t, writer_error>>
local_parquet_file_writer::initialize(const iceberg::struct_type& schema) {
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
        co_return writer_error::file_io_error;
    }

    auto fut = co_await ss::coroutine::as_future(
      ss::make_file_output_stream(_output_file));

    if (fut.failed()) {
        vlog(
          datalake_log.error,
          "Error making output stream for file {} - {}",
          _output_file_path,
          fut.get_exception());
        co_await _output_file.close();
        co_return writer_error::file_io_error;
    }

    _writer = co_await _writer_factory->create_writer(
      schema, std::move(fut.get()));
    _initialized = true;
    co_return std::nullopt;
}

ss::future<writer_error> local_parquet_file_writer::add_data_struct(
  iceberg::struct_value data, int64_t sz) {
    if (!_initialized) {
        co_return writer_error::file_io_error;
    }
    auto write_result = co_await _writer->add_data_struct(std::move(data), sz);
    if (write_result != writer_error::ok) {
        vlog(
          datalake_log.warn,
          "Error writing data to file {} - {}",
          _output_file_path,
          write_result);

        co_await abort();
        co_return write_result;
    }
    _raw_bytes_count += sz;
    _row_count++;

    co_return writer_error::ok;
}

ss::future<result<local_file_metadata, writer_error>>
local_parquet_file_writer::finish() {
    if (!_initialized) {
        co_return writer_error::file_io_error;
    }
    auto result = co_await _writer->finish();
    if (result != writer_error::ok) {
        co_await abort();
        co_return result;
    }
    _initialized = false;
    try {
        auto f_size = co_await ss::file_size(_output_file_path().string());

        co_return local_file_metadata{
          .path = _output_file_path,
          .row_count = _row_count,
          .size_bytes = f_size,
        };
    } catch (...) {
        vlog(
          datalake_log.warn,
          "Error querying parquet file {} size - {}",
          _output_file_path,
          std::current_exception());
        co_return writer_error::file_io_error;
    }
}

ss::future<> local_parquet_file_writer::abort() {
    if (!_initialized) {
        co_return;
    }
    co_await _output_file.close();
    auto exists = co_await ss::file_exists(_output_file_path().string());
    if (exists) {
        co_await ss::remove_file(_output_file_path().string());
    }
    _initialized = false;
}

local_path local_parquet_file_writer_factory::create_filename() const {
    return local_path{
      _base_directory()
      / fmt::format("{}-{}.parquet", _file_name_prefix, uuid_t::create())};
}

local_parquet_file_writer_factory::local_parquet_file_writer_factory(
  local_path base_directory,
  ss::sstring file_name_prefix,
  ss::shared_ptr<parquet_ostream_factory> writer_factory)
  : _base_directory(std::move(base_directory))
  , _file_name_prefix(std::move(file_name_prefix))
  , _writer_factory(std::move(writer_factory)) {}

ss::future<result<std::unique_ptr<parquet_file_writer>, writer_error>>
local_parquet_file_writer_factory::create_writer(
  const iceberg::struct_type& schema) {
    auto writer = std::make_unique<local_parquet_file_writer>(
      create_filename(), _writer_factory);

    auto res = co_await writer->initialize(schema);
    if (res.has_error()) {
        co_return res.error();
    }
    co_return std::move(writer);
}

} // namespace datalake
