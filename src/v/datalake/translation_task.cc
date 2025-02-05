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

#include <seastar/core/loop.hh>
#include <seastar/core/seastar.hh>
namespace datalake {
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

ss::future<checked<remote_path, translation_task::errc>> execute_single_upload(
  cloud_data_io& _cloud_io,
  const partitioning_writer::partitioned_file& file,
  const remote_path& remote_path_prefix,
  retry_chain_node& parent_rcn,
  lazy_abort_source& lazy_as) {
    auto file_remote_path = remote_path{
      file.table_location / remote_path_prefix
      / file.local_file.path().filename()};

    auto result = co_await _cloud_io.upload_data_file(
      file.local_file, file_remote_path, parent_rcn, lazy_as);
    if (result.has_error()) {
        vlog(
          datalake_log.warn,
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
  const chunked_vector<partitioning_writer::partitioned_file>& files) {
    using ret_t = checked<std::nullopt_t, translation_task::errc>;
    return ss::max_concurrent_for_each(
             files,
             16,
             [](const partitioning_writer::partitioned_file& file) {
                 vlog(
                   datalake_log.trace,
                   "removing local data file: {}",
                   file.local_file);
                 return ss::remove_file(file.local_file.path().string());
             })
      .then([] { return ret_t(std::nullopt); })
      .handle_exception([](const std::exception_ptr& e) {
          vlog(datalake_log.warn, "error deleting local data files - {}", e);
          return ret_t(translation_task::errc::file_io_error);
      });
}

ss::future<
  checked<chunked_vector<coordinator::data_file>, translation_task::errc>>
upload_files(
  cloud_data_io& _cloud_io,
  const chunked_vector<partitioning_writer::partitioned_file>& files,
  translation_task::custom_partitioning_enabled is_custom_partitioning_enabled,
  const remote_path& remote_path_prefix,
  retry_chain_node& rcn,
  lazy_abort_source& lazy_as) {
    chunked_vector<coordinator::data_file> ret;
    ret.reserve(files.size());

    std::optional<translation_task::errc> upload_error;
    for (auto& file : files) {
        auto r = co_await execute_single_upload(
          _cloud_io, file, remote_path_prefix, rcn, lazy_as);

        if (r.has_error()) {
            vlog(
              datalake_log.warn,
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

    auto delete_result = co_await delete_local_data_files(files);
    // for now we simply ignore the local deletion failures
    if (delete_result.has_error()) {
        vlog(
          datalake_log.warn,
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
              datalake_log.warn,
              "error deleting remote data files - {}",
              remote_del_result.error());
        }
        co_return *upload_error;
    }

    co_return ret;
}

} // namespace
translation_task::translation_task(
  cloud_data_io& cloud_io,
  schema_manager& schema_mgr,
  type_resolver& type_resolver,
  record_translator& record_translator,
  table_creator& table_creator,
  model::iceberg_invalid_record_action invalid_record_action,
  location_provider location_provider,
  translation_probe& probe)
  : _cloud_io(&cloud_io)
  , _schema_mgr(&schema_mgr)
  , _type_resolver(&type_resolver)
  , _record_translator(&record_translator)
  , _table_creator(&table_creator)
  , _invalid_record_action(invalid_record_action)
  , _location_provider(std::move(location_provider))
  , _translation_probe(&probe) {}

ss::future<
  checked<coordinator::translated_offset_range, translation_task::errc>>
translation_task::translate(
  const model::ntp& ntp,
  model::revision_id topic_revision,
  std::unique_ptr<parquet_file_writer_factory> writer_factory,
  custom_partitioning_enabled is_custom_partitioning_enabled,
  model::record_batch_reader reader,
  const remote_path& remote_path_prefix,
  retry_chain_node& rcn,
  lazy_abort_source& lazy_as) {
    record_multiplexer mux(
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
      lazy_as);
    // Write local files
    auto mux_result = co_await std::move(reader).consume(
      std::move(mux), _read_timeout + model::timeout_clock::now());

    if (mux_result.has_error()) {
        vlog(
          datalake_log.warn,
          "Error writing data files - {}",
          mux_result.error());
        co_return errc::file_io_error;
    }
    auto write_result = std::move(mux_result).value();
    if (datalake_log.is_enabled(seastar::log_level::trace)) {
        vlog(
          datalake_log.trace,
          "translation result base offset: {}, last offset: {}, data files: {}",
          write_result.start_offset,
          write_result.last_offset,
          fmt::join(write_result.data_files, ", "));
    }

    coordinator::translated_offset_range ret{
      .start_offset = write_result.start_offset,
      .last_offset = write_result.last_offset,
    };

    // Data files.
    {
        auto upload_res = co_await upload_files(
          *_cloud_io,
          write_result.data_files,
          is_custom_partitioning_enabled,
          remote_path_prefix,
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
          *_cloud_io,
          write_result.dlq_files,
          is_custom_partitioning_enabled,
          remote_path_prefix,
          rcn,
          lazy_as);
        if (dlq_upload_res.has_error()) {
            co_return dlq_upload_res.error();
        }
        ret.dlq_files = std::move(dlq_upload_res.value());
    }

    co_return ret;
}

std::ostream& operator<<(std::ostream& o, translation_task::errc ec) {
    switch (ec) {
    case translation_task::errc::file_io_error:
        return o << "local file IO error";
    case translation_task::errc::cloud_io_error:
        return o << "cloud IO error";
    }
}
} // namespace datalake
