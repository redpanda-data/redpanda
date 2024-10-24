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
#include "utils/named_type.h"

#include <filesystem>
namespace datalake {
/**
 * Definitions of local and remote paths, as the name indicates the local path
 * is always pointing to the location on local disk wheras the remote path is a
 * path of the object in the object store.
 */
using local_path = named_type<std::filesystem::path, struct local_path_tag>;
using remote_path = named_type<std::filesystem::path, struct remote_path_tag>;

/**
 * Simple type describing local parquet file metadata with its path and basic
 * statistics
 */
struct local_file_metadata {
    local_path path;
    size_t row_count = 0;
    size_t size_bytes = 0;
    int hour = 0;

    friend std::ostream&
    operator<<(std::ostream& o, const local_file_metadata& r);
};
} // namespace datalake
