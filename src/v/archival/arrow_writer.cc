#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/types.h"
#include "cluster/partition.h"
#include "model/fundamental.h"

#include <seastar/util/file.hh>

#include <archival/arrow_writer.h>
#include <arrow/type_fwd.h>

#include <filesystem>
#include <string_view>

ss::future<bool> datalake::write_parquet(
  const std::filesystem::path inner_path,
  ss::shared_ptr<storage::log> log,
  model::offset starting_offset,
  model::offset ending_offset) {
    storage::log_reader_config reader_cfg(
      starting_offset,
      ending_offset,
      0,
      4096,
      ss::default_priority_class(),
      model::record_batch_type::raft_data,
      std::nullopt,
      std::nullopt);

    auto reader = co_await log->make_reader(reader_cfg);

    std::string_view topic_name = model::topic_view(
      log->config().ntp().tp.topic);

    std::filesystem::path path = std::filesystem::path("/tmp/parquet_files")
                                 / topic_name / inner_path;
    arrow_writing_consumer consumer(path);
    arrow::Status result = co_await reader.consume(
      std::move(consumer), model::no_timeout);
    co_return result.ok();
}

bool datalake::is_datalake_topic(cluster::partition& partition) {
    std::string_view topic = model::topic_view(
      partition.log()->config().ntp().tp.topic);

    return topic.starts_with("experimental_datalake_");
}

ss::future<cloud_storage::upload_result> datalake::put_parquet_file(
  const cloud_storage_clients::bucket_name& bucket,
  const std::string_view topic_name,
  const std::filesystem::path& inner_path,
  cloud_storage::remote& remote,
  retry_chain_node& rtc,
  retry_chain_logger& rtclog) {
    std::filesystem::path local_path
      = std::filesystem::path("/tmp/parquet_files") / topic_name / inner_path;

    std::filesystem::path remote_path = std::filesystem::path(
                                          "experimental/parquet_files")
                                        / topic_name / inner_path;

    bool exists = co_await ss::file_exists(local_path.string());
    if (!exists) {
        vlog(rtclog.error, "Local Parquet file does not exist: {}", local_path);
        co_return cloud_storage::upload_result::failed;
    }

    iobuf file_buf;
    ss::output_stream<char> iobuf_ostream = make_iobuf_ref_output_stream(
      file_buf);

    try {
        co_await ss::util::with_file_input_stream(
          local_path,
          [&iobuf_ostream](
            ss::input_stream<char>& file_istream) -> ss::future<> {
              return ss::copy<char>(file_istream, iobuf_ostream);
          });
        co_await iobuf_ostream.close();

        auto ret = co_await remote.upload_object(
          {.transfer_details
           = {.bucket = bucket, .key = cloud_storage_clients::object_key(remote_path), .parent_rtc = rtc},
           .type = cloud_storage::upload_type::object,
           .payload = std::move(file_buf)});
        co_return ret;
    } catch (...) {
        vlog(
          rtclog.error,
          "Failed to upload parquet file: {} to {}",
          local_path,
          remote_path);
        co_return cloud_storage::upload_result::failed;
    }
}

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

arrow::Status datalake::arrow_writing_consumer::write_file() {
    if (!_ok.ok()) {
        return _ok;
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
    std::shared_ptr<arrow::Table> table = arrow::Table::Make(
      _schema,
      {key_chunks, value_chunks, timestamp_chunks, offset_chunks},
      key_chunks->length());

    // Write it out
    // In the future we may want to return the arrow table and let the
    // caller write it out however they want. This would make it easy
    // to support other arrow-compatible output formats like ORC.
    std::filesystem::create_directories(_local_file_name.parent_path());
    std::shared_ptr<arrow::io::FileOutputStream> outfile;

    ARROW_ASSIGN_OR_RAISE(
      outfile, arrow::io::FileOutputStream::Open(_local_file_name));
    PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(
      *table, arrow::default_memory_pool(), outfile));
    PARQUET_THROW_NOT_OK(outfile->Close());
    return arrow::Status::OK();
}

ss::future<arrow::Status> datalake::arrow_writing_consumer::end_of_stream() {
    if (!_ok.ok()) {
        co_return _ok;
    }
    if (_key_vector.size() == 0 || _rows == 0) {
        // FIXME: use a different return type for this.
        // See the note in ntp_archiver_service::do_upload_segment when
        // calling write_parquet.
        co_return arrow::Status::UnknownError("No Data");
    }
    // FIXME: Creating and destroying sharded_thread_workers is supposed
    // to be rare. The docs for the class suggest doing creating it once
    // during application startup.
    ssx::sharded_thread_worker thread_worker;
    co_await thread_worker.start({.name = "parquet"});
    auto result = co_await thread_worker.submit(
      [this]() -> arrow::Status { return this->write_file(); });
    co_await thread_worker.stop();
    co_return result;
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

datalake::arrow_writing_consumer::arrow_writing_consumer(
  std::filesystem::path file_name)
  : _local_file_name(std::move(file_name)) {
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
