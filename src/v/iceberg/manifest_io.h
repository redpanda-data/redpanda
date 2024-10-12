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
#include "cloud_io/remote.h"
#include "cloud_storage_clients/types.h"
#include "iceberg/manifest.h"
#include "iceberg/manifest_list.h"
#include "iceberg/metadata_io.h"
#include "iceberg/partition_key_type.h"
#include "utils/named_type.h"

#include <seastar/core/future.hh>
#include <seastar/util/noncopyable_function.hh>

namespace iceberg {

using manifest_path
  = named_type<std::filesystem::path, struct manifest_path_tag>;
using manifest_list_path
  = named_type<std::filesystem::path, struct manifest_path_tag>;

class manifest_io : public metadata_io {
public:
    explicit manifest_io(
      cloud_io::remote& io, cloud_storage_clients::bucket_name b)
      : metadata_io(io, std::move(b)) {}
    ~manifest_io() = default;

    ss::future<checked<manifest, metadata_io::errc>> download_manifest(
      const manifest_path& path, const partition_key_type& pk_type);
    ss::future<checked<manifest, metadata_io::errc>> download_manifest_uri(
      const ss::sstring& uri, const partition_key_type& pk_type);

    ss::future<checked<manifest_list, metadata_io::errc>>
    download_manifest_list(const manifest_list_path& path);
    ss::future<checked<manifest_list, metadata_io::errc>>
    download_manifest_list_uri(const ss::sstring& uri);

    ss::future<checked<size_t, metadata_io::errc>>
    upload_manifest(const manifest_path& path, const manifest&);
    ss::future<checked<size_t, metadata_io::errc>>
    upload_manifest_list(const manifest_list_path& path, const manifest_list&);

    ss::sstring to_uri(const std::filesystem::path& p) const;

private:
    // TODO: make URIs less fragile with an explicit type?
    // E.g. s3://bucket/
    ss::sstring uri_base() const;

    // E.g. s3://bucket/path/to/file => path/to/file
    // Leaves the path as is if it doesn't match the expected URI base.
    std::filesystem::path from_uri(const ss::sstring& s) const;
};

} // namespace iceberg
