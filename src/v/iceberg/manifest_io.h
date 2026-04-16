/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
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

    ss::future<checked<manifest, metadata_io::errc>>
    download_manifest(const manifest_path& path);
    ss::future<checked<manifest, metadata_io::errc>>
    download_manifest(const uri& uri);

    ss::future<checked<manifest_list, metadata_io::errc>>
    download_manifest_list(const manifest_list_path& path);
    ss::future<checked<manifest_list, metadata_io::errc>>
    download_manifest_list(const uri& uri);

    ss::future<checked<size_t, metadata_io::errc>>
    upload_manifest(const manifest_path& path, const manifest&);
    ss::future<checked<size_t, metadata_io::errc>>
    upload_manifest_list(const manifest_list_path& path, const manifest_list&);
    ss::future<checked<size_t, metadata_io::errc>>
    upload_manifest(const uri& path, const manifest&);

    ss::future<checked<size_t, metadata_io::errc>>
    upload_manifest_list(const uri& path, const manifest_list&);

    ss::future<checked<iobuf, metadata_io::errc>>
    download_object_bytes(const uri& uri);

    ss::future<checked<size_t, metadata_io::errc>>
    upload_object_bytes(const uri& uri, iobuf data);
};

} // namespace iceberg
