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
#include "iceberg/metadata_io.h"
#include "iceberg/table_metadata.h"
#include "utils/named_type.h"

namespace iceberg {

using table_metadata_path
  = named_type<std::filesystem::path, struct table_metadata_path_tag>;
using version_hint_path
  = named_type<std::filesystem::path, struct table_metadata_path_tag>;

class table_io : public metadata_io {
public:
    table_io(cloud_io::remote& io, cloud_storage_clients::bucket_name b)
      : metadata_io(io, std::move(b)) {}

    ss::future<checked<table_metadata, metadata_io::errc>>
    download_table_meta(const table_metadata_path& path);

    ss::future<checked<int, metadata_io::errc>>
    download_version_hint(const version_hint_path& path);

    ss::future<checked<size_t, metadata_io::errc>>
    upload_table_meta(const table_metadata_path& path, const table_metadata&);

    ss::future<checked<size_t, metadata_io::errc>>
    upload_version_hint(const version_hint_path& path, int version);

    ss::future<checked<bool, metadata_io::errc>>
    version_hint_exists(const version_hint_path& path);
};

} // namespace iceberg
