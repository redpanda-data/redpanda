// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/manifest_io.h"

#include "base/vlog.h"
#include "bytes/iobuf.h"
#include "cloud_storage_clients/types.h"
#include "iceberg/logger.h"
#include "iceberg/manifest.h"
#include "iceberg/manifest_avro.h"
#include "iceberg/manifest_list.h"
#include "iceberg/manifest_list_avro.h"
#include "ssx/future-util.h"
#include "utils/retry_chain_node.h"

#include <seastar/coroutine/as_future.hh>
#include <seastar/util/noncopyable_function.hh>

#include <exception>

using namespace std::chrono_literals;

namespace iceberg {

template<typename T>
ss::future<checked<T, manifest_io::errc>> manifest_io::download_object(
  const std::filesystem::path& path,
  std::string_view display_str,
  ss::noncopyable_function<T(iobuf)> parse) {
    retry_chain_node retry(
      io_.as(), ss::lowres_clock::duration{30s}, {}, retry_strategy::disallow);
    iobuf buf;
    auto res = co_await ss::coroutine::as_future(io_.download_object({
        .transfer_details = {
          .bucket = bucket_,
          .key = cloud_storage_clients::object_key{path},
          .parent_rtc = retry,
        },
        .display_str = display_str,
        .payload = buf,
    }));
    if (res.failed()) {
        const auto ex = res.get_exception();
        const auto msg = fmt::format(
          "Exception while performing {} upload: {}", display_str, ex);
        if (ssx::is_shutdown_exception(ex)) {
            vlog(log.debug, "{}", msg);
            co_return errc::shutting_down;
        }
        vlog(log.error, "{}", msg);
        co_return errc::failed;
    }
    switch (res.get()) {
        using enum cloud_io::download_result;
    case success:
        // Fallthrough to handle the bytes below.
        break;
    case timedout:
        co_return errc::timedout;
    case notfound:
        // Not found is not a retriable error.
        [[fallthrough]];
    case failed:
        co_return errc::failed;
    }
    try {
        co_return parse(std::move(buf));
    } catch (...) {
        auto ex = std::current_exception();
        vlog(log.error, "Exception while parsing manifest: {}", ex);
        co_return errc::failed;
    }
}

ss::future<checked<manifest, manifest_io::errc>> manifest_io::download_manifest(
  const manifest_path& path, const partition_key_type& pk_type) {
    co_return co_await download_object<manifest>(
      path(), "iceberg::manifest", [&pk_type](iobuf b) {
          return parse_manifest(pk_type, std::move(b));
      });
}

ss::future<checked<manifest_list, manifest_io::errc>>
manifest_io::download_manifest_list(const manifest_list_path& path) {
    return download_object<manifest_list>(
      path(), "iceberg::manifest_list", parse_manifest_list);
}

template<typename T>
ss::future<std::optional<manifest_io::errc>> manifest_io::upload_object(
  const std::filesystem::path& path,
  const T& t,
  std::string_view display_str,
  ss::noncopyable_function<iobuf(const T&)> serialize) {
    auto buf = serialize(t);
    retry_chain_node retry(
      io_.as(), ss::lowres_clock::duration{30s}, {}, retry_strategy::disallow);
    auto res = co_await ss::coroutine::as_future(io_.upload_object({
        .transfer_details = {
          .bucket = bucket_,
          .key = cloud_storage_clients::object_key{path},
          .parent_rtc = retry,
        },
        .display_str = display_str,
        .payload = std::move(buf),
    }));
    if (res.failed()) {
        const auto ex = res.get_exception();
        const auto msg = fmt::format(
          "Exception while performing {} upload: {}", display_str, ex);
        if (ssx::is_shutdown_exception(ex)) {
            vlog(log.debug, "{}", msg);
            co_return errc::shutting_down;
        }
        vlog(log.error, "{}", msg);
        co_return errc::failed;
    }
    switch (res.get()) {
        using enum cloud_io::upload_result;
    case success:
        co_return std::nullopt;
    case cancelled:
        co_return errc::shutting_down;
    case failed:
        co_return errc::failed;
    case timedout:
        co_return errc::timedout;
    }
}

ss::future<std::optional<manifest_io::errc>>
manifest_io::upload_manifest(const manifest_path& path, const manifest& m) {
    return upload_object<manifest>(
      path(), m, "iceberg::manifest", [](const manifest& m) {
          return serialize_avro(m);
      });
}

ss::future<std::optional<manifest_io::errc>> manifest_io::upload_manifest_list(
  const manifest_list_path& path, const manifest_list& m) {
    return upload_object<manifest_list>(
      path(), m, "iceberg::manifest_list", [](const manifest_list& m) {
          return serialize_avro(m);
      });
}

} // namespace iceberg
