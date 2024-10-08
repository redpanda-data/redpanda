/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/parquet_writer.h"

#include "bytes/iobuf.h"
#include "datalake/data_writer_interface.h"

#include <arrow/array/array_base.h>
#include <arrow/chunked_array.h>
#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#include <iostream>
#include <memory>
#include <stdexcept>

namespace datalake {

class arrow_to_iobuf::iobuf_output_stream : public arrow::io::OutputStream {
public:
    iobuf_output_stream() = default;

    ~iobuf_output_stream() override = default;

    // Close the stream cleanly.
    arrow::Status Close() override {
        _closed = true;
        return arrow::Status::OK();
    }

    // Return the position in this stream
    arrow::Result<int64_t> Tell() const override { return _position; }

    // Return whether the stream is closed
    bool closed() const override { return _closed; }

    arrow::Status Write(const void* data, int64_t nbytes) override {
        _current_iobuf.append(reinterpret_cast<const uint8_t*>(data), nbytes);
        _position += nbytes;
        return arrow::Status::OK();
    }

    // TODO: implement this to avoid copying data multiple times
    // virtual Status Write(const std::shared_ptr<Buffer>& data);

    // Take the data from the iobuf and clear the internal state.
    iobuf take_iobuf() { return std::exchange(_current_iobuf, {}); }

private:
    iobuf _current_iobuf;
    bool _closed = false;
    int64_t _position = 0;
};

arrow_to_iobuf::arrow_to_iobuf(std::shared_ptr<arrow::Schema> schema) {
    // TODO: make the compression algorithm configurable.
    std::shared_ptr<parquet::WriterProperties> writer_props
      = parquet::WriterProperties::Builder()
          .compression(arrow::Compression::SNAPPY)
          ->build();

    // Opt to store Arrow schema for easier reads back into Arrow
    std::shared_ptr<parquet::ArrowWriterProperties> arrow_props
      = parquet::ArrowWriterProperties::Builder().store_schema()->build();

    _ostream = std::make_shared<iobuf_output_stream>();

    auto writer_result = parquet::arrow::FileWriter::Open(
      *schema,
      arrow::default_memory_pool(),
      _ostream,
      std::move(writer_props),
      std::move(arrow_props));
    if (!writer_result.ok()) {
        throw std::runtime_error(fmt::format(
          "Failed to create Arrow writer: {}", writer_result.status()));
    }
    _writer = std::move(writer_result.ValueUnsafe());
}

void arrow_to_iobuf::add_arrow_array(std::shared_ptr<arrow::Array> data) {
    arrow::ArrayVector data_av = {std::move(data)};

    std::shared_ptr<arrow::ChunkedArray> chunked_data
      = std::make_shared<arrow::ChunkedArray>(std::move(data_av));
    auto table_result = arrow::Table::FromChunkedStructArray(chunked_data);
    if (!table_result.ok()) {
        throw std::runtime_error(fmt::format(
          "Failed to create arrow table: {}", table_result.status()));
    }
    auto table = table_result.ValueUnsafe();
    auto write_result = _writer->WriteTable(*table);
    if (!write_result.ok()) {
        throw std::runtime_error(fmt::format(
          "Failed to write arrow table: {}", write_result.ToString()));
    }
}

iobuf arrow_to_iobuf::take_iobuf() { return _ostream->take_iobuf(); }

iobuf arrow_to_iobuf::close_and_take_iobuf() {
    auto status = _writer->Close();
    if (!status.ok()) {
        throw std::runtime_error(
          fmt::format("Failed to close FileWriter: {}", status.ToString()));
    }
    return take_iobuf();
}
} // namespace datalake
