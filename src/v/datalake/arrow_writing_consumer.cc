#include "datalake/arrow_writing_consumer.h"

ss::future<ss::stop_iteration>
datalake::arrow_writing_consumer::operator()(model::record_batch batch) {
    arrow::StringBuilder key_builder;
    arrow::StringBuilder value_builder;
    arrow::UInt64Builder timestamp_builder;
    arrow::UInt64Builder offset_builder;
    if (batch.compressed()) {
        _compressed_batches++;

        // FIXME: calling internal method of storage module seems like a red
        // flag.
        batch = storage::internal::maybe_decompress_batch_sync(batch);
    } else {
        _uncompressed_batches++;
    }
    batch.for_each_record([this,
                           &batch,
                           &key_builder,
                           &value_builder,
                           &timestamp_builder,
                           &offset_builder](model::record&& record) {
        std::string key;
        std::string value;
        key = iobuf_to_string(record.key());

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

        // FIXME(jcipar): this is not the correct way to compute offsets. The
        // new log reader config has a `translate_offsets` options that
        // automatically does the translation. After rebasing, use that.
        _ok = offset_builder.Append(
          int64_t(batch.header().base_offset) + record.offset_delta());
        if (!_ok.ok()) {
            return ss::stop_iteration::yes;
        }

        _rows++;
        return ss::stop_iteration::no;
    });
    if (!_ok.ok()) {
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::yes);
    }

    std::shared_ptr<arrow::Array> key_array;
    std::shared_ptr<arrow::Array> value_array;
    std::shared_ptr<arrow::Array> offset_array;
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

    auto&& offset_builder_result = offset_builder.Finish();
    _ok = offset_builder_result.status();
    if (!_ok.ok()) {
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::yes);
    } else {
        offset_array = std::move(offset_builder_result).ValueUnsafe();
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
    _offset_vector.push_back(offset_array);
    _timestamp_vector.push_back(timestamp_array);

    return ss::make_ready_future<ss::stop_iteration>(ss::stop_iteration::no);
}

template<typename ArrowType>
class get_arrow_value : public arrow::ScalarVisitor {};

std::shared_ptr<arrow::Table> datalake::arrow_writing_consumer::get_table() {
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
    std::shared_ptr<arrow::ChunkedArray> offset_chunks
      = std::make_shared<arrow::ChunkedArray>(_offset_vector);

    // Create a table
    return arrow::Table::Make(
      _schema,
      {key_chunks, value_chunks, timestamp_chunks, offset_chunks},
      key_chunks->length());
}

ss::future<std::shared_ptr<arrow::Table>>
datalake::arrow_writing_consumer::end_of_stream() {
    if (!_ok.ok()) {
        co_return nullptr;
    }
    if (_key_vector.size() == 0 || _rows == 0) {
        // FIXME: use a different return type for this.
        // See the note in ntp_archiver_service::do_upload_segment when
        // calling write_parquet.
        _ok = arrow::Status::UnknownError("No Data");
        co_return nullptr;
    }
    co_return this->get_table();
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

datalake::arrow_writing_consumer::arrow_writing_consumer() {
    // For now these could be local variables in end_of_stream, but in
    // the future we will have the constructor take a schema argument.
    //
    // FIXME: should these be binary columns to avoid issues when we can't
    // encode as utf8? The Parquet library will happily output binary in
    // these columns, and a reader will get an exception trying to read the
    // file.
    _field_key = arrow::field("Key", arrow::utf8());
    _field_value = arrow::field("Value", arrow::utf8());
    _field_timestamp = arrow::field(
      "Timestamp", arrow::uint64()); // FIXME: timestamp type?
    _field_offset = arrow::field("Offset", arrow::uint64());
    _schema = arrow::schema(
      {_field_key, _field_value, _field_timestamp, _field_offset});
}
