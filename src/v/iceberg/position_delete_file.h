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
#include "iceberg/equality_delete_file.h"
#include "iceberg/partition_key.h"
#include "iceberg/uri.h"

#include <seastar/core/future.hh>

namespace iceberg {

struct position_delete_entry {
    uri file_path;
    int64_t pos;
    partition_key partition;
};

/// \brief Write a position delete file in parquet format from sorted entries.
///
/// Entries MUST be sorted by file_path then pos (Iceberg spec requirement).
/// The output parquet schema is:
///   file_path (required BYTE_ARRAY, string logical type)
///   pos       (required INT64)
ss::future<delete_file_result> write_position_delete_file(
  chunked_vector<position_delete_entry> deletes, bool compress = false);

} // namespace iceberg
