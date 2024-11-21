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

#include "serde/parquet/metadata.h"
#include "serde/parquet/value.h"

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

// The delta in stats when a value is written to a column.
//
// This is used to account memory usage at the row group level.
struct incremental_column_stats {
    uint64_t memory_usage;
};

// A writer for a single column of parquet data.
class column_writer {
public:
    class impl;

    // Options for changing how a column writer behaves.
    struct options {
        // If true, use zstd compression for the column data.
        bool compress = false;
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
    incremental_column_stats add(value, rep_level, def_level);

    // Flush the currently buffered values to a page.
    //
    // This also resets the writer to be able to start writing a new page.
    ss::future<data_page> flush_page();

private:
    std::unique_ptr<impl> _impl;
};

} // namespace serde::parquet
