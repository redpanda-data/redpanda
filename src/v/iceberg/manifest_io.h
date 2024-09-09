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
#include "bytes/iobuf.h"
#include "cloud_io/remote.h"
#include "iceberg/manifest.h"
#include "iceberg/manifest_list.h"
#include "model/fundamental.h"
#include "utils/named_type.h"

#include <seastar/core/future.hh>
#include <seastar/util/noncopyable_function.hh>

#include <system_error>

namespace iceberg {

using manifest_path
  = named_type<std::filesystem::path, struct manifest_path_tag>;
using manifest_list_path
  = named_type<std::filesystem::path, struct manifest_path_tag>;

class manifest_io {
public:
    enum class errc {
        // The call has timed out. Callers should retry, if allowed.
        timedout = 1,

        // The call failed for some reason that doesn't appear to be retriable.
        failed,

        // The call failed because the system is shutting down.
        shutting_down,
    };
    explicit manifest_io(
      cloud_io::remote& io, cloud_storage_clients::bucket_name b)
      : io_(io)
      , bucket_(b) {}
    ~manifest_io() = default;

    ss::future<checked<manifest, errc>>
    download_manifest(const manifest_path& path);
    ss::future<checked<manifest_list, errc>>
    download_manifest_list(const manifest_list_path& path);

    ss::future<std::optional<errc>>
    upload_manifest(const manifest_path& path, const manifest&);
    ss::future<std::optional<errc>>
    upload_manifest_list(const manifest_list_path& path, const manifest_list&);

private:
    template<typename T>
    ss::future<checked<T, errc>> download_object(
      const std::filesystem::path& path,
      std::string_view display_str,
      ss::noncopyable_function<T(iobuf)>);

    template<typename T>
    ss::future<std::optional<manifest_io::errc>> upload_object(
      const std::filesystem::path& path,
      const T&,
      std::string_view display_str,
      ss::noncopyable_function<iobuf(const T&)>);

    cloud_io::remote& io_;
    const cloud_storage_clients::bucket_name bucket_;
};

} // namespace iceberg
