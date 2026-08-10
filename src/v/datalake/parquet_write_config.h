/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "iceberg/table_metadata.h"

#include <optional>

namespace datalake {

/// Parquet writer configuration parsed from Iceberg write.parquet.* table
/// properties.
struct parquet_write_config {
    /// Compress column data with zstd. Set to false by
    /// write.parquet.compression-codec = "uncompressed".
    bool compress = true;

    static parquet_write_config
    from_properties(const std::optional<iceberg::table_properties_t>&);
};

} // namespace datalake
