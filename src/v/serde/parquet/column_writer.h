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

#include "container/chunked_vector.h"
#include "serde/parquet/metadata.h"
#include "serde/parquet/value.h"

#include <seastar/core/future.hh>

namespace serde::parquet {

// A serialized page for a column along with the page header metadata
// as that is used when creating the metadata for the entire column.
struct data_page {
    // The unencoded header for this page.
    page_header header;
    // The size of the encoded header.
    int64_t serialized_header_size;
    // This serialized data already includes the header encoded in
    // Apache Thrift format.
    iobuf serialized;
};

// All the accumulated data when a column_writer is flushed.
struct flushed_pages {
    // All the pages that were flushed together into a single row group.
    chunked_vector<data_page> pages;
    // Stats for all flushed pages
    statistics stats;
};

// A writer for a single column of parquet data.
class column_writer {
public:
    class impl;

    // Options for changing how a column writer behaves.
    struct options {
        // If true, use zstd compression for the column data.
        bool compress;
    };

    explicit column_writer(const schema_element&, options);
    column_writer(const column_writer&) = delete;
    column_writer& operator=(const column_writer&) = delete;
    column_writer(column_writer&&) noexcept;
    column_writer& operator=(column_writer&&) noexcept;
    ~column_writer() noexcept;

    // Add a value to this column along with it's repetition level and
    // definition level.
    //
    // `value` here is only allowed to be the same value as `value_type`
    // or `null` if there are non-required nodes in the schema ancestor
    // nodes.
    //
    // Use `shred_record` to get the value and levels from an arbitrary value.
    //
    // Return the current information about the column after a value is written.
    void add(value, rep_level, def_level);

    // The memory usage of this entire column.
    int64_t memory_usage() const;

    // The memory usage of the current page being built.
    int64_t current_page_memory_usage() const;

    ss::future<> next_page();

    // Flush the pages that have been accumulated so far in the column.
    //
    // This also resets the writer to be able to start writing a new column
    // (within another row group).
    ss::future<flushed_pages> flush_pages();

private:
    std::unique_ptr<impl> _impl;
};

} // namespace serde::parquet
