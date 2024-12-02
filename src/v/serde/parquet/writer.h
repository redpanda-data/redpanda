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

#include "container/fragmented_vector.h"
#include "serde/parquet/schema.h"
#include "serde/parquet/value.h"

#include <seastar/core/iostream.hh>

namespace serde::parquet {

// Statistics about the current row group.
//
// These are mainly provided to limit memory usage.
struct row_group_stats {
    int64_t rows = 0;
    uint64_t memory_usage = 0;
};

// Statistics about the current file being built.
struct file_stats {
    // Note these are only the number of rows flushed
    // to the output stream (so does not include the number
    // in the current row group)
    int64_t rows = 0;

    // The size of the file flushed to the output stream - does not include the
    // current row group buffered in memory, nor the footer (until close is
    // called).
    uint64_t size = 0;

    // Additional information about the current row group buffered in memory.
    row_group_stats current_row_group;
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
        // TODO(parquet): add settings around buffer settings, etc.
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
    ss::future<> write_row(group_value);

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
    ss::future<> close();

private:
    class impl;
    std::unique_ptr<impl> _impl;
};

} // namespace serde::parquet
