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
#include "iceberg/manifest_io.h"
#include "iceberg/merge_append_action.h"
#include "iceberg/position_delete_file.h"
#include "iceberg/row_delta_action.h"
#include "iceberg/table_metadata.h"
#include "iceberg/uri.h"
#include "serde/parquet/value.h"

#include <seastar/core/future.hh>

#include <functional>

namespace iceberg {

enum class delete_commit_strategy {
    equality_deletes,
    position_deletes,
};

using read_file_fn = std::function<ss::future<iobuf>(const uri&)>;

/// Extract key column values from data files by reading only the
/// key columns (via projection).
ss::future<chunked_vector<serde::parquet::group_value>> extract_keys(
  const table_metadata& table,
  const chunked_vector<file_to_append>& files,
  const chunked_vector<nested_field::id_t>& key_field_ids,
  read_file_fn read_file);

/// Scan existing data files for rows matching the given keys.
/// Uses column stats to prune candidate files. Returns sorted
/// (file_path, position) pairs for matching rows.
ss::future<chunked_vector<position_delete_entry>> find_matching_positions(
  const table_metadata& table,
  manifest_io& mio,
  const chunked_vector<serde::parquet::group_value>& keys,
  const chunked_vector<nested_field::id_t>& key_field_ids,
  const chunked_hash_set<ss::sstring>& exclude_file_paths,
  read_file_fn read_file);

struct pending_delete {
    file_to_delete file;
    iobuf data;
};

/// Write position delete files from matching positions.
/// Returns pending deletes whose data must be uploaded before commit.
ss::future<chunked_vector<pending_delete>> make_position_deletes(
  const table_metadata& table,
  chunked_vector<position_delete_entry> positions,
  bool compress = false);

/// Write equality delete files from key values.
ss::future<chunked_vector<file_to_delete>> make_equality_deletes(
  const table_metadata& table,
  const chunked_vector<serde::parquet::group_value>& keys,
  const chunked_vector<nested_field::id_t>& key_field_ids,
  bool compress = false);

} // namespace iceberg
