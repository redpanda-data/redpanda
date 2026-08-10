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

#include "container/chunked_vector.h"
#include "iceberg/table_metadata.h"

#include <seastar/core/sstring.hh>

#include <optional>

namespace datalake {

/// Parquet writer configuration parsed from Iceberg write.parquet.* table
/// properties.
struct parquet_write_config {
    /// Compress column data with zstd. Set to false by
    /// write.parquet.compression-codec = "uncompressed".
    bool compress = true;

    static constexpr size_t default_bloom_filter_ndv = 10'000;
    // ~12 MiB per bloom filter at 9.6 bits/NDV.
    static constexpr size_t max_bloom_filter_ndv = 10'000'000;

    /// Per-column bloom filter configuration. Only columns explicitly
    /// enabled via write.parquet.bloom-filter-enabled.column.<col>=true
    /// appear here. The NDV comes from bloom-filter-ndv.column.<col> or
    /// default_bloom_filter_ndv if unset.
    struct bloom_column {
        ss::sstring name;
        size_t ndv;
    };
    chunked_vector<bloom_column> bloom_filter_columns;

    static parquet_write_config
    from_properties(const std::optional<iceberg::table_properties_t>&);
};

} // namespace datalake
