// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/manifest_io.h"

#include "bytes/iobuf.h"
#include "iceberg/manifest.h"
#include "iceberg/manifest_avro.h"
#include "iceberg/manifest_list.h"
#include "iceberg/manifest_list_avro.h"
#include "ssx/future-util.h"

#include <seastar/coroutine/as_future.hh>
#include <seastar/util/noncopyable_function.hh>

#include <exception>

using namespace std::chrono_literals;

namespace iceberg {
ss::future<checked<manifest, metadata_io::errc>> manifest_io::download_manifest(
  const uri& uri, const partition_key_type& pk_type) {
    auto path_res = from_uri(uri);
    if (path_res.has_error()) {
        co_return path_res.error();
    }

    co_return co_await download_manifest(
      manifest_path(path_res.value()), pk_type);
}

ss::future<checked<manifest_list, metadata_io::errc>>
manifest_io::download_manifest_list(const uri& uri) {
    auto path_res = from_uri(uri);
    if (path_res.has_error()) {
        co_return path_res.error();
    }

    co_return co_await download_manifest_list(
      manifest_list_path{path_res.value()});
}

ss::future<checked<manifest, metadata_io::errc>> manifest_io::download_manifest(
  const manifest_path& path, const partition_key_type& pk_type) {
    co_return co_await download_object<manifest>(
      path(), "iceberg::manifest", [&pk_type](iobuf b) {
          return parse_manifest(pk_type, std::move(b));
      });
}

ss::future<checked<manifest_list, metadata_io::errc>>
manifest_io::download_manifest_list(const manifest_list_path& path) {
    return download_object<manifest_list>(
      path(), "iceberg::manifest_list", parse_manifest_list);
}

ss::future<checked<size_t, metadata_io::errc>>
manifest_io::upload_manifest(const manifest_path& path, const manifest& m) {
    return upload_object<manifest>(
      path().string(), m, "iceberg::manifest", [](const manifest& m) {
          return serialize_avro(m);
      });
}

ss::future<checked<size_t, metadata_io::errc>>
manifest_io::upload_manifest(const uri& uri, const manifest& m) {
    auto path_res = from_uri(uri);
    if (path_res.has_error()) {
        return ssx::now<checked<size_t, metadata_io::errc>>(path_res.error());
    }
    return upload_manifest(manifest_path(path_res.value()), m);
}

ss::future<checked<size_t, metadata_io::errc>>
manifest_io::upload_manifest_list(
  const manifest_list_path& path, const manifest_list& m) {
    return upload_object<manifest_list>(
      path().string(), m, "iceberg::manifest_list", [](const manifest_list& m) {
          return serialize_avro(m);
      });
}

ss::future<checked<size_t, metadata_io::errc>>
manifest_io::upload_manifest_list(const uri& uri, const manifest_list& m) {
    auto path_res = from_uri(uri);
    if (path_res.has_error()) {
        return ssx::now<checked<size_t, metadata_io::errc>>(path_res.error());
    }
    return upload_manifest_list(manifest_list_path(path_res.value()), m);
}

} // namespace iceberg
