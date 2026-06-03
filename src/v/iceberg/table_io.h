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
#include "iceberg/metadata_io.h"
#include "iceberg/table_metadata.h"
#include "utils/named_type.h"

namespace iceberg {

// /<table-location>/
using table_location_path
  = named_type<std::filesystem::path, struct table_location_path_tag>;
// /<table-location>/metadata/
using metadata_location_path
  = named_type<std::filesystem::path, struct metadata_location_path_tag>;
// /<table-location>/metadata/v0.metadata.json
using table_metadata_path
  = named_type<std::filesystem::path, struct table_metadata_path_tag>;
// /<table-location>/metadata/version-hint.text
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

    ss::future<checked<std::nullopt_t, metadata_io::errc>>
    delete_all_metadata(const metadata_location_path& path);

    ss::future<checked<std::nullopt_t, metadata_io::errc>>
    delete_all_table_files(const table_location_path& path);

private:
    ss::future<checked<std::nullopt_t, metadata_io::errc>>
    delete_objects_with_prefix(const std::filesystem::path& prefix);
};

} // namespace iceberg
