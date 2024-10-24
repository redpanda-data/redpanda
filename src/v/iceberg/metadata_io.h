// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "base/outcome.h"
#include "base/vlog.h"
#include "bytes/iobuf.h"
#include "cloud_io/remote.h"
#include "cloud_storage_clients/types.h"
#include "iceberg/logger.h"
#include "ssx/future-util.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/future.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/noncopyable_function.hh>

namespace iceberg {

using namespace std::chrono_literals;
class metadata_io {
public:
    enum class errc {
        // The call has timed out. Callers should retry, if allowed.
        timedout = 1,

        // The call failed for some reason that doesn't appear to be retriable.
        failed,

        // The call failed because the system is shutting down.
        shutting_down,
    };
    metadata_io(cloud_io::remote& io, cloud_storage_clients::bucket_name b)
      : io_(io)
      , bucket_(std::move(b)) {}
    ~metadata_io() = default;

protected:
    template<typename T>
    ss::future<checked<T, errc>> download_object(
      const std::filesystem::path& path,
      std::string_view display_str,
      ss::noncopyable_function<T(iobuf)> parse) {
        retry_chain_node retry(
          io_.as(),
          ss::lowres_clock::duration{30s},
          {},
          retry_strategy::disallow);
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

    template<typename T>
    ss::future<checked<size_t, errc>> upload_object(
      const std::filesystem::path& path,
      const T& t,
      std::string_view display_str,
      ss::noncopyable_function<iobuf(const T&)> serialize) {
        auto buf = serialize(t);
        auto uploaded_size_bytes = buf.size_bytes();
        retry_chain_node retry(
          io_.as(),
          ss::lowres_clock::duration{30s},
          {},
          retry_strategy::disallow);
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
            co_return uploaded_size_bytes;
        case cancelled:
            co_return errc::shutting_down;
        case failed:
            co_return errc::failed;
        case timedout:
            co_return errc::timedout;
        }
    }

    ss::future<checked<bool, metadata_io::errc>> object_exists(
      const std::filesystem::path& path, std::string_view display_str) {
        retry_chain_node retry(
          io_.as(),
          ss::lowres_clock::duration{30s},
          {},
          retry_strategy::disallow);
        auto res = co_await ss::coroutine::as_future(io_.object_exists(
          bucket_,
          cloud_storage_clients::object_key{path},
          retry,
          display_str));
        if (res.failed()) {
            const auto ex = res.get_exception();
            const auto msg = fmt::format(
              "Exception while performing {} existence check: {}",
              display_str,
              ex);
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
            co_return true;
        case timedout:
            co_return errc::timedout;
        case notfound:
            co_return false;
        case failed:
            co_return errc::failed;
        }
    }

protected:
    cloud_io::remote& io_;
    const cloud_storage_clients::bucket_name bucket_;
};

} // namespace iceberg
