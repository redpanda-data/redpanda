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

#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_io.h"
#include "iceberg/parquet_reader.h"
#include "iceberg/table_metadata.h"
#include "serde/parquet/value.h"

#include <seastar/core/future.hh>

namespace datalake::tests {

/// \brief A manifest entry's data_file annotated with its sequence number.
struct sequenced_file {
    iceberg::data_file file;
    int64_t seq_num;
};

/// \brief Read a position delete file, returning sorted positions
/// keyed by the referenced data file path.
ss::future<chunked_hash_map<ss::sstring, iceberg::position_delete_set>>
read_position_deletes(
  iceberg::manifest_io& io, const iceberg::data_file& del_file);

/// \brief Read an equality delete file into an equality_delete_set.
///
/// Uses the equality_ids from the data_file manifest entry to
/// determine which field IDs form the key.
ss::future<iceberg::equality_delete_set> read_equality_deletes(
  iceberg::manifest_io& io, const iceberg::data_file& del_file);

/// \brief Read all surviving records from a committed Iceberg table.
///
/// Applies position and equality deletes with spec-correct sequence
/// number filtering:
///   - equality deletes apply when del.seq > data.seq
///   - position deletes apply when del.seq >= data.seq
///
/// This implements merge-on-read as described in the Iceberg spec
/// (DeleteFileIndex.java in the reference implementation).
ss::future<chunked_vector<serde::parquet::group_value>> read_iceberg_table(
  iceberg::manifest_io& io, const iceberg::table_metadata& table);

} // namespace datalake::tests
