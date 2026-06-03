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

#include "base/units.h"
#include "base/vlog.h"
#include "datalake/logger.h"

#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/coroutine/as_future.hh>

namespace datalake {

local_parquet_file_writer::local_parquet_file_writer(
  local_path output_file_path,
  ss::shared_ptr<parquet_ostream_factory> writer_factory,
  writer_mem_tracker& mem_tracker)
  : _output_file_path(std::move(output_file_path))
  , _writer_factory(std::move(writer_factory))
  , _mem_tracker(mem_tracker) {}

ss::future<checked<std::nullopt_t, writer_error>>
local_parquet_file_writer::initialize(const iceberg::struct_type& schema) {
    vlog(datalake_log.info, "Writing Parquet file to {}", _output_file_path);
    ss::file output_file;
    try {
        output_file = co_await ss::open_file_dma(
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
      ss::make_file_output_stream(std::move(output_file)));

    if (fut.failed()) {
        auto ex = fut.get_exception();
        vlog(
          datalake_log.error,
          "Error making output stream for file {} - {}",
          _output_file_path,
          ex);
        co_return writer_error::file_io_error;
    }

    _writer = co_await _writer_factory->create_writer(
      schema, std::move(fut.get()), _mem_tracker);
    _initialized = true;
    co_return std::nullopt;
}

ss::future<writer_error> local_parquet_file_writer::add_data_struct(
  iceberg::struct_value data, int64_t sz, ss::abort_source& as) {
    if (!_initialized) {
        co_return writer_error::file_io_error;
    }
    if (_error != writer_error::ok) {
        co_return _error;
    }
    auto write_result = co_await _writer->add_data_struct(
      std::move(data), sz, as);
    if (write_result != writer_error::ok) {
        vlogl(
          datalake_log,
          is_recoverable_error(write_result) ? ss::log_level::debug
                                             : ss::log_level::warn,
          "Error writing data to file {} - {}",
          _output_file_path,
          write_result);
        _error = write_result;
        co_return write_result;
    }
    _raw_bytes_count += sz;
    _row_count++;

    co_return writer_error::ok;
}

size_t local_parquet_file_writer::buffered_bytes() const {
    return _writer->buffered_bytes();
}

size_t local_parquet_file_writer::flushed_bytes() const {
    return _writer->flushed_bytes();
}

ss::future<writer_error> local_parquet_file_writer::flush() {
    if (!_initialized) {
        co_return writer_error::flush_error;
    }
    if (_error != writer_error::ok && !is_recoverable_error(_error)) {
        co_return _error;
    }
    auto result = co_await ss::coroutine::as_future(_writer->flush());
    if (result.failed()) {
        auto ex = result.get_exception();
        vlog(datalake_log.warn, "Error flushing {}: {}", _output_file_path, ex);
        co_return writer_error::flush_error;
    }
    co_return writer_error::ok;
}

ss::future<result<local_file_metadata, writer_error>>
local_parquet_file_writer::finish() {
    if (!_initialized) {
        co_return writer_error::file_io_error;
    }
    _initialized = false;
    auto writer_ec = writer_error::ok;
    try {
        writer_ec = co_await _writer->finish();
    } catch (...) {
        vlog(
          datalake_log.warn,
          "Error closing writer instance {} for path {}",
          std::current_exception(),
          _output_file_path);
        writer_ec = writer_error::file_io_error;
    }
    if (is_recoverable_error(_error) && !is_recoverable_error(writer_ec)) {
        _error = writer_ec;
    }
    if (!is_recoverable_error(_error)) {
        auto exists = co_await ss::file_exists(_output_file_path().string());
        if (exists) {
            co_await ss::remove_file(_output_file_path().string());
        }
        co_return _error;
    }
    try {
        auto f_size = co_await ss::file_size(_output_file_path().string());
        auto col_stats = _writer->column_stats();

        co_return local_file_metadata{
          .path = _output_file_path,
          .row_count = _row_count,
          .size_bytes = f_size,
          .column_stats = std::move(col_stats),
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

local_path local_parquet_file_writer_factory::create_filename() const {
    return local_path{
      _base_directory()
      / fmt::format("{}-{}.parquet", _file_name_prefix, uuid_t::create())};
}

local_parquet_file_writer_factory::local_parquet_file_writer_factory(
  local_path base_directory,
  ss::sstring file_name_prefix,
  ss::shared_ptr<parquet_ostream_factory> writer_factory,
  writer_mem_tracker& mem_tracker)
  : _base_directory(std::move(base_directory))
  , _file_name_prefix(std::move(file_name_prefix))
  , _writer_factory(std::move(writer_factory))
  , _mem_tracker(mem_tracker) {}

ss::future<result<std::unique_ptr<parquet_file_writer>, writer_error>>
local_parquet_file_writer_factory::create_writer(
  const iceberg::struct_type& schema, ss::abort_source& as) {
    // There is a per writer cost associated which includes stuff like
    // - local path string
    // - associated partition key
    // - schema
    // - stats tracked about the writer
    // - data structure overhead
    //
    // This limit is in place to avoid an explosion of writer instances,
    // example partition_by(offset) which creates a writer per offset.
    //
    // Additionally one other contributor per writer is the buffer used
    // in the output stream which defaults to 8_KiB, which is only released
    // on output stream close().
    //
    // TODO: This is just a conservative estimate to prevent pathological cases
    // of too many writers, needs empirical evaluation to determine the correct
    // sizing.
    static constexpr size_t WRITER_RESERVATION_OVERHEAD = 10_KiB;
    auto reservation_err = co_await _mem_tracker.reserve_bytes(
      WRITER_RESERVATION_OVERHEAD, as);
    if (reservation_err != reservation_error::ok) {
        co_return map_to_writer_error(reservation_err);
    }
    auto writer = std::make_unique<local_parquet_file_writer>(
      create_filename(), _writer_factory, _mem_tracker);

    auto res = co_await writer->initialize(schema);
    if (res.has_error()) {
        co_return res.error();
    }
    co_return std::move(writer);
}

} // namespace datalake
