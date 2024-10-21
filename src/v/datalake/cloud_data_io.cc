/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/cloud_data_io.h"

#include "cloud_io/remote.h"
#include "datalake/logger.h"
#include "utils/lazy_abort_source.h"
#include "utils/retry_chain_node.h"
#include "utils/stream_provider.h"

#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/coroutine/as_future.hh>

namespace datalake {

namespace {
// TODO: deduplicate, expose from cloud storage
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

checked<std::nullopt_t, cloud_data_io::errc>
map_upload_result(cloud_io::upload_result result) {
    switch (result) {
    case cloud_io::upload_result::success:
        return std::nullopt;
    case cloud_io::upload_result::timedout:
        return cloud_data_io::errc::cloud_op_timeout;
    case cloud_io::upload_result::failed:
    case cloud_io::upload_result::cancelled:
        return cloud_data_io::errc::cloud_op_error;
    }
}
} // namespace

// Modeled after cloud_storage::remote::upload_controller_snapshot
ss::future<checked<std::nullopt_t, cloud_data_io::errc>>
cloud_data_io::upload_data_file(
  const local_file_metadata& local_file,
  const remote_path& remote_path,
  retry_chain_node& rtc_parent,
  lazy_abort_source& lazy_abort_source) {
    vlog(
      datalake_log.trace,
      "Uploading data file {} to: {}",
      local_file,
      remote_path);

    auto file_fut = co_await ss::coroutine::as_future(
      ss::open_file_dma(local_file.path().string(), ss::open_flags::ro));

    if (file_fut.failed()) {
        vlog(
          datalake_log.error,
          "Failed to open file for upload {}: {}",
          local_file.path(),
          file_fut.get_exception());
        co_return errc::file_io_error;
    }
    ss::file file = std::move(file_fut.get());

    auto reset_stream = [&file] {
        using provider_t = std::unique_ptr<stream_provider>;
        ss::file_input_stream_options opts;
        return ss::make_ready_future<provider_t>(
          std::make_unique<one_time_stream_provider>(
            ss::make_file_input_stream(file, opts)));
    };

    auto upload_result_fut = co_await ss::coroutine::as_future(
      _cloud_io->upload_stream(
        cloud_io::transfer_details{
          .bucket = _bucket,
          .key = cloud_storage_clients::object_key(remote_path),
          .parent_rtc = rtc_parent,
        },
        local_file.size_bytes,
        reset_stream,
        lazy_abort_source,
        "datalake data file",
        std::nullopt));
    if (upload_result_fut.failed()) {
        vlog(
          datalake_log.warn,
          "Uploading file {} failed with an exception - {}",
          remote_path,
          upload_result_fut.get_exception());
        co_return errc::cloud_op_error;
    }
    auto upload_result = upload_result_fut.get();

    if (upload_result != cloud_io::upload_result::success) {
        vlog(
          datalake_log.warn,
          "Uploading file {} failed - {}",
          remote_path,
          upload_result);
        co_return map_upload_result(upload_result).error();
    }
    co_return std::nullopt;
}

ss::future<checked<std::nullopt_t, cloud_data_io::errc>>
cloud_data_io::delete_data_files(
  chunked_vector<remote_path> files_to_delete, retry_chain_node& rcn_parent) {
    chunked_vector<cloud_storage_clients::object_key> keys;
    keys.reserve(files_to_delete.size());
    std::transform(
      files_to_delete.begin(),
      files_to_delete.end(),
      std::back_inserter(keys),
      [](remote_path path) {
          return cloud_storage_clients::object_key{std::move(path)};
      });

    auto r_fut = co_await ss::coroutine::as_future(_cloud_io->delete_objects(
      _bucket, std::move(keys), rcn_parent, [](size_t) {}));
    if (r_fut.failed()) {
        vlog(
          datalake_log.warn,
          "Exception thrown while removing remote data files - {}",
          r_fut.get_exception());
        co_return errc::cloud_op_error;
    }

    co_return map_upload_result(r_fut.get());
}

std::ostream& operator<<(std::ostream& o, cloud_data_io::errc ec) {
    switch (ec) {
    case cloud_data_io::errc::file_io_error:
        return o << "cloud operation local file io error";
    case cloud_data_io::errc::cloud_op_error:
        return o << "cloud operation error";
    case cloud_data_io::errc::cloud_op_timeout:
        return o << "cloud operation timeout";
    }
}
} // namespace datalake
