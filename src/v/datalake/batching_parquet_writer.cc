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

ss::future<>
batching_parquet_writer::initialize(std::filesystem::path output_file_path) {
    _output_file = co_await ss::open_file_dma(
      output_file_path.string(),
      ss::open_flags::create | ss::open_flags::truncate | ss::open_flags::wo);

    _output_stream = co_await ss::make_file_output_stream(_output_file);
}

ss::future<void> batching_parquet_writer::add_data_struct(
  iceberg::value data, int64_t approx_size) {
    _iceberg_to_arrow.add_data(std::move(data));
    _row_count++;
    _byte_count += approx_size;

    if (
      _row_count >= _row_count_threshold
      || _byte_count > _byte_count_threshold) {
        co_await write_row_group();
    }
}

ss::future<data_writer_result> batching_parquet_writer::finish() {
    // TODO: fill in result structure
    return write_row_group()
      .then([this] {
          data_writer_result res;
          iobuf out = _arrow_to_iobuf.close_and_take_iobuf();

          return write_iobuf_to_output_stream(std::move(out), _output_stream)
            .then([res] { return res; });
      })
      .finally([this] { return _output_stream.close(); });
}

ss::future<> batching_parquet_writer::write_row_group() {
    if (_row_count == 0) {
        // This can happen if finish() is called when there is no new data.
        co_return;
    }
    auto chunk = _iceberg_to_arrow.take_chunk();
    _row_count = 0;
    _byte_count = 0;
    _arrow_to_iobuf.add_arrow_array(chunk);
    iobuf out = _arrow_to_iobuf.take_iobuf();
    co_await write_iobuf_to_output_stream(std::move(out), _output_stream);
}

} // namespace datalake
