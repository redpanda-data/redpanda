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
ss::future<checked<manifest, metadata_io::errc>>
manifest_io::download_manifest_uri(
  const ss::sstring& uri, const partition_key_type& pk_type) {
    auto path = manifest_path(from_uri(uri));
    co_return co_await download_manifest(path, pk_type);
}

ss::future<checked<manifest_list, metadata_io::errc>>
manifest_io::download_manifest_list_uri(const ss::sstring& uri) {
    auto path = manifest_list_path(from_uri(uri));
    co_return co_await download_manifest_list(path);
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
      path(), m, "iceberg::manifest", [](const manifest& m) {
          return serialize_avro(m);
      });
}

ss::future<checked<size_t, metadata_io::errc>>
manifest_io::upload_manifest_list(
  const manifest_list_path& path, const manifest_list& m) {
    return upload_object<manifest_list>(
      path(), m, "iceberg::manifest_list", [](const manifest_list& m) {
          return serialize_avro(m);
      });
}

ss::sstring manifest_io::to_uri(const std::filesystem::path& p) const {
    return fmt::format("{}{}", uri_base(), p.native());
}

std::filesystem::path manifest_io::from_uri(const ss::sstring& s) const {
    const auto base = uri_base();
    if (!s.starts_with(base)) {
        return std::filesystem::path{s};
    }
    return std::filesystem::path{s.substr(base.size())};
}

ss::sstring manifest_io::uri_base() const {
    return fmt::format("s3://{}/", bucket_);
}

} // namespace iceberg
