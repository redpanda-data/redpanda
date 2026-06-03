/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/units.h"
#include "container/chunked_vector.h"
#include "serde/parquet/metadata.h"
#include "serde/parquet/schema.h"
#include "serde/parquet/value.h"

#include <seastar/core/iostream.hh>

namespace serde::parquet {

// Statistics about the current file being built.
struct file_stats {
    // The size of the file flushed to the output stream - does not include the
    // current row group buffered in memory, nor the footer (until close is
    // called).
    int64_t flushed_size = 0;

    // The amount of memory currently buffered in the current row group.
    // When calling flush_row_group, this is reset to 0.
    int64_t buffered_size = 0;
};

// A parquet file writer for seastar.
class writer {
public:
    struct options {
        // The schema for values written to the writer.
        // This schema does not need to be indexed, the writer will do that.
        schema_element schema;
        // Metadata to write into the parquet file.
        chunked_vector<std::pair<ss::sstring, ss::sstring>> metadata;
        // Information used when filling out the `created_by` metadata.
        ss::sstring version = "latest";
        ss::sstring build = "dev";
        // If true, compress the parquet column chunks using zstd compression
        bool compress = false;
        // The target size for how much data within *each* column we buffer
        // before flushing/encoding/compressing the data before a row group
        // being flushed (since columns can have multiple pages within a row
        // group). Ecosystem libraries tend to default between 256Kib-1MiB
        static constexpr int64_t default_page_size = 512_KiB;
        int64_t page_buffer_size = default_page_size;

        // Row groups are flushed to disk internally if they exceed this size.
        static constexpr int64_t default_row_group_size = 128_MiB;
        int64_t row_group_size = default_row_group_size;

        // Max byte length for BYTE_ARRAY column statistics. Values exceeding
        // this are truncated with is_exact=false. 0 disables truncation.
        // Fixed-size types (int32, int64, float, etc.) are always exact.
        // Matches Arrow's DEFAULT_MAX_STATISTICS_SIZE.
        static constexpr int32_t default_max_stats_length = 4096;
        int32_t max_stats_truncate_length = default_max_stats_length;

        // Expected number of distinct values per column per row group, used
        // to size bloom filters for non-boolean columns. 0 disables bloom
        // filters. When enabled, default_bloom_filter_ndv (~12 KiB per
        // column) is a reasonable starting point: filters are automatically
        // discarded at flush if the fill ratio indicates FPP > ~10%, which
        // occurs at roughly 1.5× the configured NDV (~15K for the default).
        static constexpr size_t default_bloom_filter_ndv = 10'000;
        size_t bloom_filter_ndv = 0;
    };

    // Create a new parquet file writer using the given options that
    // writes to the output stream.
    //
    // NOTE: the writer owns the output stream.
    writer(options, ss::output_stream<char>);
    writer(const writer&) = delete;
    writer& operator=(const writer&) = delete;
    writer(writer&&) noexcept;
    writer& operator=(writer&&) noexcept;
    ~writer() noexcept;

    // Initialize the writer. Must be called before calling `write_row` or
    // `close`.
    ss::future<> init();

    // Write the record as a row to the parquet file. This value must exactly
    // match the provided schema.
    //
    // This method may not be called concurrently with other methods on this
    // class.
    ss::future<file_stats> write_row(group_value);

    // The current stats on the file being written.
    //
    // This can be used to monitor the current file size.
    //
    // This can also be used to account for memory usage and flush a row group
    // when the memory usage is over some limit.
    file_stats stats() const;

    // Flush the current row group to the output stream, creating a new row
    // group.
    //
    // This may only be called if there is at least a single row in the group.
    //
    // This method may not be called concurrently with other methods on this
    // class.
    ss::future<> flush_row_group();

    // Aggregated per-column statistics for the entire file.
    //
    // Entries appear in schema DFS order, one per leaf column. Must be called
    // after close().
    struct file_column_stats {
        /// Iceberg field id, if present in the schema element.
        std::optional<int32_t> field_id;
        /// Min/max bounds and null count accumulated across all row groups.
        statistics bounds;
        /// Total number of values (including nulls) across all row groups.
        int64_t value_count = 0;
        /// Total compressed size in bytes across all row groups.
        int64_t column_size_bytes = 0;
    };
    chunked_vector<file_column_stats> column_file_stats();

    // Close the writer by writing the parquet file footer. Then flush/close the
    // underlying stream that is being written too.
    //
    // This method will automatically flush the current row group if there are
    // any rows buffered in it.
    //
    // This method may not be called concurrently with other methods on this
    // class.
    //
    // The resulting future must be awaited before destroying this object.
    ss::future<file_metadata> close();

private:
    class impl;
    std::unique_ptr<impl> _impl;
};

} // namespace serde::parquet
