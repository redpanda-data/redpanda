#include "datalake/arrow_writing_consumer.h"

#include "datalake/protobuf_to_arrow_converter.h"

#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>

#include <arrow/array/builder_binary.h>
#include <arrow/io/file.h>
#include <arrow/table.h>
#include <arrow/type_fwd.h>
#include <parquet/arrow/writer.h>

#include <exception>
#include <memory>

datalake::arrow_writing_consumer::arrow_writing_consumer(
  std::filesystem::path local_file_path, std::string protobuf_schema)
  : _local_file_path(std::move(local_file_path)) {
    try {
        _table_builder = std::make_unique<proto_to_arrow_converter>(
          protobuf_schema);
    } catch (const std::exception& e) {
        // Couldn't build a table builder, fall back to schemaless
        // TODO: Log this
    }

    _field_key = arrow::field("Key", arrow::binary());
    _field_value = arrow::field("Value", arrow::binary());

    // TODO: use the timestamp type? Iceberg does not support unsigned integers.
    _field_timestamp = arrow::field("Timestamp", arrow::uint64());

    if (_table_builder) {
        _schema = _table_builder->build_schema();
    } else {
        _schema = arrow::schema({_field_key, _field_value, _field_timestamp});
    }

    // Initialize output file
    std::filesystem::create_directories(_local_file_path.parent_path());

    // TODO: use compression. Originally I set it to SNAPPY because that's what
    // the example code in the docs uses, but that was throwing a NotImplemented
    // error at runtime. Pick a compresseion algorithm and ensure our Arrow
    // library is compiled with it.
    std::shared_ptr<parquet::WriterProperties> props
      = parquet::WriterProperties::Builder()
          .compression(arrow::Compression::UNCOMPRESSED)
          ->build();

    // Opt to store Arrow schema for easier reads back into Arrow
    std::shared_ptr<parquet::ArrowWriterProperties> arrow_props
      = parquet::ArrowWriterProperties::Builder().store_schema()->build();

    auto outfile_result = arrow::io::FileOutputStream::Open(_local_file_path);
    if (!outfile_result.ok()) {
        _ok = outfile_result.status();
        return;
    }
    std::shared_ptr<arrow::io::FileOutputStream> outfile
      = outfile_result.ValueUnsafe();

    auto file_writer_result = parquet::arrow::FileWriter::Open(
      *_schema.get(),
      arrow::default_memory_pool(),
      outfile,
      props,
      arrow_props);

    if (!file_writer_result.ok()) {
        _ok = file_writer_result.status();
        return;
    }
    _file_writer = std::move(file_writer_result.ValueUnsafe());
}

ss::future<ss::stop_iteration>
datalake::arrow_writing_consumer::operator()(model::record_batch batch) {
    arrow::BinaryBuilder key_builder;
    arrow::BinaryBuilder value_builder;
    arrow::UInt64Builder timestamp_builder;
    if (batch.compressed()) {
        _compressed_batches++;

        // FIXME: calling internal method of storage module seems like a red
        // flag.
        batch = storage::internal::maybe_decompress_batch_sync(batch);
    } else {
        _uncompressed_batches++;
    }
    batch.for_each_record(
      [this, &batch, &key_builder, &value_builder, &timestamp_builder](
        model::record&& record) {
          std::string key;
          std::string value;
          key = iobuf_to_string(record.key());

          // TODO: Factor out the schemaless code into a schemaless
          // table builder.
          if (record.has_value()) {
              value = iobuf_to_string(record.value());
          }

          _ok = key_builder.Append(key);
          if (!_ok.ok()) {
              return ss::stop_iteration::yes;
          }

          _ok = value_builder.Append(value);
          if (!_ok.ok()) {
              return ss::stop_iteration::yes;
          }

          _ok = timestamp_builder.Append(
            batch.header().first_timestamp.value() + record.timestamp_delta());
          if (!_ok.ok()) {
              return ss::stop_iteration::yes;
          }

          if (_table_builder != nullptr) {
              _table_builder->add_message(value);
          }

          _total_row_count++;
          _unwritten_row_count++;
          return ss::stop_iteration::no;
      });
    if (!_ok.ok()) {
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::yes);
    }

    std::shared_ptr<arrow::Array> key_array;
    std::shared_ptr<arrow::Array> value_array;
    std::shared_ptr<arrow::Array> timestamp_array;

    auto&& key_builder_result = key_builder.Finish();
    // Arrow ASSIGN_OR_RAISE macro doesn't actually raise, it returns a
    // not-ok value. Expanding the macro and editing ensures we get the
    // correct return type.
    _ok = key_builder_result.status();
    if (!_ok.ok()) {
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::yes);
    } else {
        key_array = std::move(key_builder_result).ValueUnsafe();
    }

    auto&& value_builder_result = value_builder.Finish();
    _ok = value_builder_result.status();
    if (!_ok.ok()) {
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::yes);
    } else {
        value_array = std::move(value_builder_result).ValueUnsafe();
    }

    auto&& timestamp_builder_result = timestamp_builder.Finish();
    _ok = timestamp_builder_result.status();
    if (!_ok.ok()) {
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::yes);
    } else {
        timestamp_array = std::move(timestamp_builder_result).ValueUnsafe();
    }

    _key_vector.push_back(key_array);
    _value_vector.push_back(value_array);
    _timestamp_vector.push_back(timestamp_array);

    if (_unwritten_row_count > _row_group_size) {
        if (!write_row_group()) {
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::yes);
        }
    }

    return ss::make_ready_future<ss::stop_iteration>(ss::stop_iteration::no);
}

ss::future<arrow::Status> datalake::arrow_writing_consumer::end_of_stream() {
    if (!_ok.ok()) {
        co_return _ok;
    }
    write_row_group();

    auto close_result = _file_writer->Close();
    if (!close_result.ok()) {
        _ok = close_result;
    }

    co_return _ok;
}

bool datalake::arrow_writing_consumer::write_row_group() {
    if (_unwritten_row_count == 0) {
        return true;
    }

    std::shared_ptr<arrow::Table> table;
    if (_table_builder == nullptr) {
        // TODO: debug log: using schemaless table builder
        table = get_schemaless_table();
    } else {
        // TODO: debug log: using schemaful table builder
        _table_builder->finish_batch();
        table = _table_builder->build_table();
    }
    if (table == nullptr) {
        return false;
    }

    auto write_result = _file_writer->WriteTable(*table.get());
    if (!write_result.ok()) {
        _ok = write_result;
        return false;
    }

    _key_vector.clear();
    _value_vector.clear();
    _timestamp_vector.clear();
    _unwritten_row_count = 0;

    return true;
}

std::shared_ptr<arrow::Table>
datalake::arrow_writing_consumer::get_schemaless_table() {
    if (!_ok.ok()) {
        return nullptr;
    }
    // Create a ChunkedArray
    std::shared_ptr<arrow::ChunkedArray> key_chunks
      = std::make_shared<arrow::ChunkedArray>(_key_vector);
    std::shared_ptr<arrow::ChunkedArray> value_chunks
      = std::make_shared<arrow::ChunkedArray>(_value_vector);
    std::shared_ptr<arrow::ChunkedArray> timestamp_chunks
      = std::make_shared<arrow::ChunkedArray>(_timestamp_vector);

    // Create a table
    return arrow::Table::Make(
      _schema,
      {key_chunks, value_chunks, timestamp_chunks},
      key_chunks->length());
}

uint32_t datalake::arrow_writing_consumer::iobuf_to_uint32(const iobuf& buf) {
    auto kbegin = iobuf::byte_iterator(buf.cbegin(), buf.cend());
    auto kend = iobuf::byte_iterator(buf.cend(), buf.cend());
    std::vector<uint8_t> key_bytes;
    while (kbegin != kend) {
        key_bytes.push_back(*kbegin);
        ++kbegin;
    }
    return *reinterpret_cast<const uint32_t*>(key_bytes.data());
}

std::string
datalake::arrow_writing_consumer::iobuf_to_string(const iobuf& buf) {
    auto vbegin = iobuf::byte_iterator(buf.cbegin(), buf.cend());
    auto vend = iobuf::byte_iterator(buf.cend(), buf.cend());
    std::string value;
    // Byte iterators don't work with the string constructor.
    while (vbegin != vend) {
        value += *vbegin;
        ++vbegin;
    }
    return value;
}
