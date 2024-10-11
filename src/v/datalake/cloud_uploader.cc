/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/cloud_uploader.h"

#include "base/outcome.h"
#include "base/seastarx.h"
#include "cloud_io/remote.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/types.h"
#include "data_file.h"
#include "datalake/data_writer_interface.h"
#include "datalake/logger.h"
#include "model/fundamental.h"
#include "storage/segment_reader.h"
#include "utils/lazy_abort_source.h"
#include "utils/retry_chain_node.h"
#include "utils/stream_provider.h"

#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>

#include <exception>

namespace datalake {

namespace {
struct one_time_stream_provider : public stream_provider {
    explicit one_time_stream_provider(ss::input_stream<char> s)
      : _st(std::move(s)) {}
    ss::input_stream<char> take_stream() override {
        auto tmp = std::exchange(_st, std::nullopt);
        return std::move(tmp.value());
    }
    ss::future<> close() override {
        if (_st.has_value()) {
            return _st->close().then([this] { _st = std::nullopt; });
        }
        return ss::now();
    }
    std::optional<ss::input_stream<char>> _st;
};

} // namespace

// Modeled after cloud_storage::remote::upload_controller_snapshot
ss::future<result<coordinator::data_file, data_writer_error>>
cloud_uploader::upload_data_file(
  datalake::local_data_file local_file,
  std::filesystem::path remote_filename,
  retry_chain_node& rtc_parent,
  lazy_abort_source& lazy_abort_source) {
    std::filesystem::path remote_path = (_remote_directory / remote_filename);
    ss::sstring remote_uri = fmt::format(
      "s3://{}/{}", _bucket(), remote_path.string());
    vlog(
      datalake_log.info,
      "Uploading {} to {}",
      local_file.local_path(),
      remote_uri);

    ss::file file;
    try {
        file = co_await ss::open_file_dma(
          local_file.local_path().string(), ss::open_flags::ro);
    } catch (...) {
        vlog(
          datalake_log.error,
          "Failed to open file for upload {}: {}",
          local_file.local_path(),
          std::current_exception());
        co_return data_writer_error::file_io_error;
    }
    auto reset_stream = [&file] {
        using provider_t = std::unique_ptr<stream_provider>;
        ss::file_input_stream_options opts;
        return ss::make_ready_future<provider_t>(
          std::make_unique<one_time_stream_provider>(
            ss::make_file_input_stream(file, opts)));
    };

    auto upload_result = co_await _cloud_io.upload_stream(
      {
        .bucket = _bucket,
        .key = cloud_storage_clients::object_key(remote_path),
        .parent_rtc = rtc_parent,
      },
      local_file.file_size_bytes,
      reset_stream,
      lazy_abort_source,
      "datalake parquet upload",
      std::nullopt);

    if (upload_result == cloud_storage::upload_result::success) {
        coordinator::data_file remote_file{
          .remote_path = remote_uri,
          .row_count = local_file.row_count,
          .file_size_bytes = local_file.file_size_bytes,
          .hour = local_file.hour,
        };
        co_return remote_file;
    } else {
        vlog(datalake_log.error, "Failed to upload: {}", upload_result);
        co_return data_writer_error::cloud_io_error;
    }
}
ss::future<result<chunked_vector<coordinator::data_file>, data_writer_error>>
cloud_uploader::upload_multiple(
  chunked_vector<local_data_file> local_files,
  retry_chain_node& rtc_parent,
  lazy_abort_source& lazy_abort_source) {
    chunked_vector<coordinator::data_file> results;
    // TODO: check lazy_abort_source when needed
    for (const auto& file : local_files) {
        auto res = co_await upload_data_file(
          file, file.file_path.string(), rtc_parent, lazy_abort_source);
        if (!res.has_value()) {
            co_await cleanup_local_files(std::move(local_files));
            co_await cleanup_remote_files(std::move(results), rtc_parent);
            co_return res.error();
        }
        results.push_back(res.value());
    }
    co_await cleanup_local_files(std::move(local_files));
    co_return results;
}
ss::future<> cloud_uploader::cleanup_local_files(
  chunked_vector<local_data_file> local_files) {
    for (const auto& file : local_files) {
        try {
            vlog(
              datalake_log.debug, "Removing local file {}", file.local_path());
            co_await ss::remove_file(file.local_path().string());
        } catch (...) {
            vlog(
              datalake_log.error,
              "Error removing local file {}: {}",
              file.local_path(),
              std::current_exception());
        }
    }
}
ss::future<> cloud_uploader::cleanup_remote_files(
  chunked_vector<coordinator::data_file> remote_files,
  retry_chain_node& rtc_parent) {
    std::deque<cloud_storage_clients::object_key> keys;
    std::filesystem::path bucket_path(fmt::format("s3://{}", _bucket));
    for (const auto& file : remote_files) {
        // TODO: should this use the same rtc and abort_source?
        // It seems like we want to clean up even if an abort was issued?
        std::filesystem::path remote_path(file.remote_path);
        ss::sstring key_string
          = remote_path.lexically_relative(bucket_path).string();
        keys.emplace_back(key_string);
        vlog(
          datalake_log.info, "Deleting remote file {} {}", _bucket, key_string);
    }

    auto result = co_await _cloud_io.delete_objects(
      cloud_storage_clients::bucket_name{_bucket},
      std::move(keys),
      rtc_parent,
      [](size_t) {});

    if (result == cloud_storage::upload_result::success) {
        vlog(
          datalake_log.debug, "Successfully cleaned up remote files on error");
    } else {
        vlog(datalake_log.error, "Error cleaning up remote files: {}", result);
    }
}
} // namespace datalake
