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
        // TODO(parquet): add settings around buffer settings, compression, etc.
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
    // The returned future must be awaited *before* calling write_row again with
    // another value.
    ss::future<> write_row(group_value);

    // Close the writer by writing the parquet file footer. Then flush/close the
    // underlying stream that is being written too.
    //
    // The resulting future must be awaited before destroying this object.
    ss::future<> close();

private:
    class impl;
    std::unique_ptr<impl> _impl;
};

} // namespace serde::parquet
