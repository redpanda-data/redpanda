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

#include <seastar/core/loop.hh>
#include <seastar/core/seastar.hh>
namespace datalake {
namespace {
remote_path calculate_remote_path(
  const local_path& local_file_path, const remote_path& remote_path_prefix) {
    auto f_name = local_file_path().filename();
    return remote_path{remote_path_prefix() / f_name};
}

translation_task::errc map_error_code(cloud_data_io::errc errc) {
    switch (errc) {
    case cloud_data_io::errc::file_io_error:
        return translation_task::errc::file_io_error;
    case cloud_data_io::errc::cloud_op_error:
    case cloud_data_io::errc::cloud_op_timeout:
        return translation_task::errc::cloud_io_error;
    }
}

} // namespace
translation_task::translation_task(
  cloud_data_io& cloud_io,
  schema_manager& schema_mgr,
  type_resolver& type_resolver,
  record_translator& record_translator,
  table_creator& table_creator)
  : _cloud_io(&cloud_io)
  , _schema_mgr(&schema_mgr)
  , _type_resolver(&type_resolver)
  , _record_translator(&record_translator)
  , _table_creator(&table_creator) {}

ss::future<
  checked<coordinator::translated_offset_range, translation_task::errc>>
translation_task::translate(
  const model::ntp& ntp,
  model::revision_id topic_revision,
  std::unique_ptr<parquet_file_writer_factory> writer_factory,
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
      *_table_creator);
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
    vlog(
      datalake_log.trace,
      "translation result base offset: {}, last offset: {}, data files: {}",
      write_result.start_offset,
      write_result.last_offset,
      fmt::join(write_result.data_files, ", "));

    coordinator::translated_offset_range ret{
      .start_offset = write_result.start_offset,
      .last_offset = write_result.last_offset,
    };
    ret.files.reserve(write_result.data_files.size());
    std::optional<errc> upload_error;
    // TODO: parallelize uploads
    for (auto& lf_meta : write_result.data_files) {
        auto r = co_await execute_single_upload(
          lf_meta, remote_path_prefix, rcn, lazy_as);
        if (r.has_error()) {
            vlog(
              datalake_log.warn,
              "error uploading file {} to object store - {}",
              lf_meta,
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
        ret.files.push_back(coordinator::data_file{
          .remote_path = r.value()().string(),
          .row_count = lf_meta.row_count,
          .file_size_bytes = lf_meta.size_bytes,
          .hour = lf_meta.hour,
        });
    }

    auto delete_result = co_await delete_local_data_files(
      write_result.data_files);
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
        for (auto& data_file : ret.files) {
            files_to_delete.emplace_back(data_file.remote_path);
        }
        // TODO: add mechanism for cleaning up orphaned files that may be left
        // behind when delete operation failed or was aborted.
        auto remote_del_result = co_await _cloud_io->delete_data_files(
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
ss::future<checked<remote_path, translation_task::errc>>
translation_task::execute_single_upload(
  const local_file_metadata& lf_meta,
  const remote_path& remote_path_prefix,
  retry_chain_node& parent_rcn,
  lazy_abort_source& lazy_as) {
    auto remote_path = calculate_remote_path(lf_meta.path, remote_path_prefix);
    auto result = co_await _cloud_io->upload_data_file(
      lf_meta, remote_path, parent_rcn, lazy_as);
    if (result.has_error()) {
        vlog(
          datalake_log.warn,
          "error uploading file {} to {} - {}",
          lf_meta,
          remote_path,
          result.error());

        co_return map_error_code(result.error());
    }

    co_return remote_path;
}
ss::future<checked<std::nullopt_t, translation_task::errc>>
translation_task::delete_local_data_files(
  const chunked_vector<local_file_metadata>& files) {
    using ret_t = checked<std::nullopt_t, translation_task::errc>;
    return ss::max_concurrent_for_each(
             files,
             16,
             [](const local_file_metadata& lf_meta) {
                 vlog(
                   datalake_log.trace, "removing local data file: {}", lf_meta);
                 return ss::remove_file(lf_meta.path().string());
             })
      .then([] { return ret_t(std::nullopt); })
      .handle_exception([](const std::exception_ptr& e) {
          vlog(datalake_log.warn, "error deleting local data files - {}", e);
          return ret_t(errc::file_io_error);
      });
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
