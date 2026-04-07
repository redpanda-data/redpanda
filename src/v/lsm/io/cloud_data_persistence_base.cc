/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "lsm/io/cloud_data_persistence_base.h"

#include "cloud_io/remote.h"
#include "config/configuration.h"
#include "lsm/core/exceptions.h"
#include "lsm/core/internal/files.h"
#include "ssx/future-util.h"

#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/coroutine/as_future.hh>

namespace lsm::io {

retry_chain_node make_cloud_rtc(ss::abort_source& as) {
    constexpr auto timeout = std::chrono::seconds(10);
    auto backoff
      = config::shard_local_cfg().cloud_storage_initial_backoff_ms.value();
    return retry_chain_node{as, timeout, backoff};
}

bool check_cloud_result(cloud_io::download_result result) {
    switch (result) {
    case cloud_io::download_result::success:
        return true;
    case cloud_io::download_result::notfound:
        return false;
    case cloud_io::download_result::timedout:
        throw io_error_exception(
          std::make_error_code(std::errc::timed_out),
          "timeout reading from cloud storage");
    case cloud_io::download_result::failed:
        throw io_error_exception("failed to read from cloud storage");
    }
}

void check_cloud_result(cloud_io::upload_result result) {
    switch (result) {
    case cloud_io::upload_result::success:
        return;
    case cloud_io::upload_result::timedout:
        throw io_error_exception(
          std::make_error_code(std::errc::timed_out),
          "timeout writing to cloud storage");
    case cloud_io::upload_result::failed:
        throw io_error_exception("failed to write to cloud storage");
    case cloud_io::upload_result::cancelled:
        throw io_error_exception("cloud storage write cancelled");
    }
}

cloud_storage_clients::object_key join_cloud_key(
  const cloud_storage_clients::object_key& prefix, std::string_view suffix) {
    return cloud_storage_clients::object_key(prefix() / suffix);
}

one_time_stream_provider::one_time_stream_provider(ss::input_stream<char> s)
  : _st(std::move(s)) {}

ss::input_stream<char> one_time_stream_provider::take_stream() {
    auto tmp = std::exchange(_st, std::nullopt);
    return std::move(tmp.value());
}

ss::future<> one_time_stream_provider::close() {
    if (_st.has_value()) {
        return _st->close().then([this] { _st = std::nullopt; });
    }
    return ss::now();
}

ss::future<> upload_file(
  cloud_io::remote& remote,
  ss::abort_source& as,
  const cloud_storage_clients::bucket_name& bucket,
  const cloud_storage_clients::object_key& cloud_key,
  const std::filesystem::path& local_path,
  size_t written) {
    auto root = make_cloud_rtc(as);
    lazy_abort_source las{[&as] {
        return as.abort_requested() ? std::make_optional("abort requested")
                                    : std::nullopt;
    }};
    auto result = co_await remote.upload_stream(
      {
        .bucket = bucket,
        .key = cloud_key,
        .parent_rtc = root,
      },
      written,
      [&local_path]() {
          return ss::open_file_dma(local_path.native(), ss::open_flags::ro)
            .then([](ss::file file) -> std::unique_ptr<stream_provider> {
                ss::file_input_stream_options opts{.read_ahead = 1};
                auto stream = ss::make_file_input_stream(std::move(file), opts);
                return std::make_unique<one_time_stream_provider>(
                  std::move(stream));
            });
      },
      las,
      "SST file upload",
      std::nullopt);
    check_cloud_result(result);
}

void throw_as_lsm_ex(std::exception_ptr ex, ss::sstring msg) {
    try {
        std::rethrow_exception(ex);
    } catch (const std::system_error& e) {
        throw io_error_exception(e.code(), "{}: {}", msg, e);
    } catch (...) {
        if (ssx::is_shutdown_exception(ex)) {
            throw abort_requested_exception("{}: {}", msg, ex);
        }
        throw io_error_exception("{}: {}", msg, ex);
    }
}

cloud_data_persistence_base::cloud_data_persistence_base(
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  cloud_storage_clients::object_key prefix)
  : _remote(remote)
  , _bucket(std::move(bucket))
  , _prefix(std::move(prefix)) {}

ss::future<> cloud_data_persistence_base::remove_file(internal::file_handle h) {
    _as.check();
    auto _ = _gate.hold();
    auto rtc = make_cloud_rtc(_as);
    auto filename = internal::sst_file_name(h);

    cloud_io::upload_result result{};
    try {
        co_await remove_file_locally(filename);
        result = co_await _remote->delete_object({
          .bucket = _bucket,
          .key = cloud_key(filename),
          .parent_rtc = rtc,
        });
    } catch (const std::system_error& e) {
        if (e.code() != std::errc::no_such_file_or_directory) {
            throw io_error_exception(e.code(), "io error removing file: {}", e);
        }
    } catch (...) {
        auto ex = std::current_exception();
        if (ssx::is_shutdown_exception(ex)) {
            throw abort_requested_exception(
              "shutdown exception while removing file: {}", ex);
        }
        throw io_error_exception("io error removing file: {}", ex);
    }
    check_cloud_result(result);
}

ss::coroutine::experimental::generator<internal::file_handle>
cloud_data_persistence_base::list_files() {
    _as.check();
    auto _ = _gate.hold();
    auto rtc = make_cloud_rtc(_as);
    cloud_io::list_result result = cloud_storage_clients::error_outcome::fail;
    try {
        result = co_await _remote->list_objects(_bucket, rtc, _prefix);
    } catch (...) {
        auto ex = std::current_exception();
        if (ssx::is_shutdown_exception(ex)) {
            throw abort_requested_exception(
              "shutdown exception while listing files: {}", ex);
        }
        throw io_error_exception("io error listing files: {}", ex);
    }
    if (result.has_error()) {
        throw io_error_exception("io error listing files: {}", result.error());
    }
    for (const auto& item : result.value().contents) {
        auto suffix = std::filesystem::path(item.key).lexically_relative(
          _prefix());
        if (suffix.has_parent_path() || !suffix.has_filename()) {
            continue;
        }
        auto file_id = internal::parse_sst_file_name(
          suffix.filename().native());
        if (!file_id) {
            continue;
        }
        co_yield *file_id;
        _as.check();
    }
}

ss::future<> cloud_data_persistence_base::close() {
    _as.request_abort_ex(
      abort_requested_exception("cloud persistence layer shutdown"));
    co_await _gate.close();
}

cloud_storage_clients::object_key
cloud_data_persistence_base::cloud_key(std::string_view name) {
    return join_cloud_key(_prefix, name);
}

} // namespace lsm::io
