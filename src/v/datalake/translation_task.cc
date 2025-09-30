/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/translation_task.h"

#include "datalake/logger.h"
#include "datalake/record_multiplexer.h"
#include "iceberg/values_bytes.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/loop.hh>
#include <seastar/core/seastar.hh>
namespace datalake {

using namespace std::chrono_literals;

namespace {

translation_task::errc map_error_code(cloud_data_io::errc errc) {
    switch (errc) {
    case cloud_data_io::errc::file_io_error:
        return translation_task::errc::file_io_error;
    case cloud_data_io::errc::cloud_op_error:
    case cloud_data_io::errc::cloud_op_timeout:
        return translation_task::errc::cloud_io_error;
    }
}

translation_task::errc map_error_code(writer_error errc) {
    switch (errc) {
    case writer_error::ok:
        // translation task does not have an equivalent for no error
        vassert(
          false, "map error code expects an errored state, got :{}", errc);
    case writer_error::parquet_conversion_error:
    case writer_error::file_io_error:
        return translation_task::errc::file_io_error;
    case writer_error::no_data:
        return translation_task::errc::no_data;
    case writer_error::flush_error:
        return translation_task::errc::flush_error;
    case writer_error::oom_error:
        return translation_task::errc::oom_error;
    case writer_error::time_limit_exceeded:
        return translation_task::errc::time_limit_exceeded;
    case writer_error::shutting_down:
        return translation_task::errc::shutting_down;
    case writer_error::out_of_disk:
        return translation_task::errc::out_of_disk;
    case writer_error::unknown_error:
        return translation_task::errc::file_io_error;
    case writer_error::retryable_type_resolution_error:
        return translation_task::errc::type_resolution_error;
    }
}

ss::future<checked<remote_path, translation_task::errc>> execute_single_upload(
  prefix_logger& logger,
  cloud_data_io& _cloud_io,
  const partitioning_writer::partitioned_file& file,
  retry_chain_node& parent_rcn,
  lazy_abort_source& lazy_as) {
    auto file_remote_path = remote_path{
      file.data_location / file.partition_key_path
      / file.local_file.path().filename()};

    // (Approximate because I'm ignoring backoff) ~5 retries at 1
    // MiB/s with 10s overhead for acquiring/waiting/connecting a connection
    // from the client pool. Very conservative.
    const auto expected_upload_duration
      = 5 * (10s + file.local_file.size_bytes / 1_MiB * 1s);

    auto upload_rcn = retry_chain_node(
      expected_upload_duration, 100ms, &parent_rcn);

    auto result = co_await _cloud_io.upload_data_file(
      file.local_file, file_remote_path, upload_rcn, lazy_as);
    if (result.has_error()) {
        vlog(
          logger.warn,
          "error uploading file {} to {} - {}",
          file.local_file,
          file_remote_path,
          result.error());

        co_return map_error_code(result.error());
    }

    co_return file_remote_path;
}

ss::future<checked<std::nullopt_t, translation_task::errc>>
delete_local_data_files(
  prefix_logger& logger,
  const chunked_vector<partitioning_writer::partitioned_file>& files) {
    using ret_t = checked<std::nullopt_t, translation_task::errc>;
    return ss::max_concurrent_for_each(
             files,
             16,
             [&logger](const partitioning_writer::partitioned_file& file) {
                 vlog(
                   logger.trace,
                   "removing local data file: {}",
                   file.local_file);
                 return ss::remove_file(file.local_file.path().string());
             })
      .then([] { return ret_t(std::nullopt); })
      .handle_exception([&logger](const std::exception_ptr& e) {
          vlog(logger.warn, "error deleting local data files - {}", e);
          return ret_t(translation_task::errc::file_io_error);
      });
}

ss::future<checked<std::nullopt_t, translation_task::errc>>
delete_data_and_dlq_files(
  prefix_logger& log, const record_multiplexer::finished_files& files) {
    auto [data_result, dlq_result] = co_await ss::when_all_succeed(
      delete_local_data_files(log, files.data_files),
      delete_local_data_files(log, files.dlq_files));

    if (data_result.has_error()) {
        vlog(
          log.warn,
          "error deleting local data files - {}",
          data_result.error());
        co_return data_result.error();
    }
    if (dlq_result.has_error()) {
        vlog(
          log.warn, "error deleting local dlq files - {}", data_result.error());
        co_return dlq_result.error();
    }
    co_return std::nullopt;
}

ss::future<
  checked<chunked_vector<coordinator::data_file>, translation_task::errc>>
upload_files(
  prefix_logger& logger,
  cloud_data_io& _cloud_io,
  const chunked_vector<partitioning_writer::partitioned_file>& files,
  translation_task::custom_partitioning_enabled is_custom_partitioning_enabled,
  retry_chain_node& rcn,
  lazy_abort_source& lazy_as) {
    chunked_vector<coordinator::data_file> ret;
    ret.reserve(files.size());

    std::optional<translation_task::errc> upload_error;
    for (auto& file : files) {
        auto r = co_await execute_single_upload(
          logger, _cloud_io, file, rcn, lazy_as);

        if (r.has_error()) {
            vlog(
              logger.warn,
              "error uploading file {} to object store - {}",
              file.local_file,
              r.error());
            upload_error = r.error();
            /**
             * For now we value simplicity, therefore in case of cloud error we
             * invalidate the whole translation i.e. we are going to cleanup all
             * the local data files and remote files that were already
             * successfully uploaded. Coordinator will simply retry translating
             * the same range
             */
            break;
        }

        chunked_vector<std::optional<bytes>> pk_fields;
        pk_fields.reserve(file.partition_key.val->fields.size());
        for (const auto& field : file.partition_key.val->fields) {
            if (field) {
                pk_fields.emplace_back(value_to_bytes(field.value()));
            } else {
                pk_fields.emplace_back(std::nullopt);
            }
        }

        coordinator::data_file uploaded{
          .remote_path = r.value()().string(),
          .row_count = file.local_file.row_count,
          .file_size_bytes = file.local_file.size_bytes,
          .table_schema_id = file.schema_id,
          .partition_spec_id = file.partition_spec_id,
          .partition_key = std::move(pk_fields),
        };

        if (!is_custom_partitioning_enabled) {
            // Upgrade is still in progress, write out the hour value for old
            // versions.
            uploaded.hour_deprecated = get_hour(file.partition_key);
        }

        ret.push_back(std::move(uploaded));
    }

    auto delete_result = co_await delete_local_data_files(logger, files);
    // for now we simply ignore the local deletion failures
    if (delete_result.has_error()) {
        vlog(
          logger.warn,
          "error deleting local data files - {}",
          delete_result.error());
    }

    if (upload_error) {
        // in this case we delete any successfully uploaded remote files before
        // returning a result
        chunked_vector<remote_path> files_to_delete;
        for (auto& data_file : ret) {
            files_to_delete.emplace_back(data_file.remote_path);
        }
        // TODO: add mechanism for cleaning up orphaned files that may be left
        // behind when delete operation failed or was aborted.
        auto remote_del_result = co_await _cloud_io.delete_data_files(
          std::move(files_to_delete), rcn);
        if (remote_del_result.has_error()) {
            vlog(
              logger.warn,
              "error deleting remote data files - {}",
              remote_del_result.error());
        }
        co_return *upload_error;
    }

    co_return ret;
}

} // namespace
translation_task::translation_task(
  const model::ntp& ntp,
  model::revision_id topic_revision,
  std::unique_ptr<parquet_file_writer_factory> writer_factory,
  cloud_data_io& cloud_io,
  features::feature_table* features,
  schema_manager& schema_mgr,
  type_resolver& type_resolver,
  record_translator& record_translator,
  table_creator& table_creator,
  model::iceberg_invalid_record_action invalid_record_action,
  location_provider location_provider,
  translation_probe& probe)
  : _log(datalake_log, fmt::format("{}", ntp))
  , _cloud_io(&cloud_io)
  , _schema_mgr(&schema_mgr)
  , _type_resolver(&type_resolver)
  , _record_translator(&record_translator)
  , _table_creator(&table_creator)
  , _invalid_record_action(invalid_record_action)
  , _location_provider(std::move(location_provider))
  , _translation_probe(&probe)
  , _multiplexer(
      ntp,
      topic_revision,
      std::move(writer_factory),
      *_schema_mgr,
      *_type_resolver,
      *_record_translator,
      *_table_creator,
      _invalid_record_action,
      _location_provider,
      *_translation_probe,
      features) {}

ss::future<> translation_task::translate_once(
  model::record_batch_reader reader,
  kafka::offset start_offset,
  ss::abort_source& as) {
    return _multiplexer.multiplex(
      std::move(reader),
      start_offset,
      _read_timeout + model::timeout_clock::now(),
      as);
}

size_t translation_task::flushed_bytes() const {
    return _multiplexer.flushed_bytes();
}

ss::future<checked<void, translation_task::errc>> translation_task::flush() {
    auto result = co_await _multiplexer.flush_writers();
    if (result != writer_error::ok) {
        vlog(_log.debug, "error flushing writers: {}", result);
        co_return map_error_code(result);
    }
    co_return outcome::success();
}

std::optional<kafka::offset> translation_task::last_translated_offset() const {
    return _multiplexer.last_translated_offset();
}

ss::future<
  checked<coordinator::translated_offset_range, translation_task::errc>>
translation_task::finish(
  custom_partitioning_enabled is_custom_partitioning_enabled,
  retry_chain_node& rcn,
  ss::abort_source& as) && {
    record_multiplexer::finished_files files;
    auto mux_result = co_await std::move(_multiplexer).finish(files);
    if (mux_result.has_error()) {
        auto mux_err = mux_result.error();
        vlog(
          _log.warn,
          "Error writing data files - {}, deleting {} data files and {} DLQ "
          "files",
          mux_result.error(),
          files.data_files.size(),
          files.dlq_files.size());
        [[maybe_unused]] auto _ = co_await delete_data_and_dlq_files(
          _log, files);
        co_return map_error_code(mux_err);
    }
    auto write_result = std::move(mux_result).value();
    if (datalake_log.is_enabled(seastar::log_level::trace)) {
        vlog(
          _log.trace,
          "translation result base offset: {}, last offset: {}, data files: "
          "{}, dlq files: {}",
          write_result.start_offset,
          write_result.last_offset,
          files.data_files.size(),
          files.dlq_files.size());
    }

    size_t rows_added = 0;
    size_t bytes_added = 0;
    for (const auto& file : files.data_files) {
        rows_added += file.local_file.row_count;
        bytes_added += file.local_file.size_bytes;
    }
    _translation_probe->on_translation_finished(
      files.data_files.size(), rows_added, bytes_added, files.dlq_files.size());

    coordinator::translated_offset_range ret{
      .start_offset = write_result.start_offset,
      .last_offset = write_result.last_offset,
      .kafka_bytes_processed = write_result.kafka_bytes_processed,
    };

    lazy_abort_source lazy_as{[&as]() {
        return as.abort_requested()
                 ? std::make_optional("translation task stop requested")
                 : std::nullopt;
    }};

    // Data files.
    {
        auto upload_res = co_await upload_files(
          _log,
          *_cloud_io,
          files.data_files,
          is_custom_partitioning_enabled,
          rcn,
          lazy_as);
        if (upload_res.has_error()) {
            co_return upload_res.error();
        }
        ret.files = std::move(upload_res.value());
    }

    // DLQ files.
    {
        auto dlq_upload_res = co_await upload_files(
          _log,
          *_cloud_io,
          files.dlq_files,
          is_custom_partitioning_enabled,
          rcn,
          lazy_as);
        if (dlq_upload_res.has_error()) {
            co_return dlq_upload_res.error();
        }
        ret.dlq_files = std::move(dlq_upload_res.value());
    }

    co_return ret;
}

ss::future<checked<std::nullopt_t, translation_task::errc>>
translation_task::discard() && {
    record_multiplexer::finished_files files;
    auto mux_result = co_await std::move(_multiplexer).finish(files);
    if (mux_result.has_error()) {
        vlog(
          _log.warn,
          "Error writing data files - {}, deleting {} data files and {} DLQ "
          "files",
          mux_result.error(),
          files.data_files.size(),
          files.dlq_files.size());
        [[maybe_unused]] auto _ = co_await delete_data_and_dlq_files(
          _log, files);
        co_return errc::file_io_error;
    }
    co_return co_await delete_data_and_dlq_files(_log, files);
}

size_t translation_task::buffered_bytes() const {
    return _multiplexer.buffered_bytes();
}

std::ostream& operator<<(std::ostream& o, translation_task::errc ec) {
    switch (ec) {
    case translation_task::errc::file_io_error:
        return o << "local file IO error";
    case translation_task::errc::cloud_io_error:
        return o << "cloud IO error";
    case translation_task::errc::flush_error:
        return o << "writer flush error";
    case translation_task::errc::no_data:
        return o << "no data to translate";
    case translation_task::errc::oom_error:
        return o << "memory exhausted";
    case translation_task::errc::time_limit_exceeded:
        return o << "time limit exceeded";
    case translation_task::errc::shutting_down:
        return o << "shutting down";
    case translation_task::errc::out_of_disk:
        return o << "disk exhausted";
    case translation_task::errc::type_resolution_error:
        return o << "type resolution error";
    }
}
} // namespace datalake
