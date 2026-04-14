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

#include "base/outcome.h"
#include "iceberg/action.h"
#include "iceberg/manifest.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_io.h"
#include "iceberg/manifest_list.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"

namespace iceberg {

/// Container for metadata required to build manifest_file::partitions (the
/// field summaries for each partition key field).
///
/// Unlike the field_summary in manifest_file, which stores bytes per bound,
/// this is value-comparable by maintaining the bounds as values instead of
/// serialized bytes. Note that only values that are the same primitive_value
/// variant are directly comparable.
struct field_summary_val {
    using list_t = chunked_vector<field_summary_val>;
    static list_t empty_summaries(size_t num_fields);
    field_summary release_with_bytes() &&;

    bool contains_null{false};
    std::optional<bool> contains_nan;
    std::optional<primitive_value> lower_bound;
    std::optional<primitive_value> upper_bound;
};

/// Context containing fields resolved from table metadata that pertain to
/// the new snapshot created by an action.
struct table_snapshot_ctx {
    const uuid_t& commit_uuid;
    const snapshot_id snap_id;
    const sequence_number seq_num;
};

/// Callback type for generating unique manifest numbers within an action.
using manifest_num_gen = ss::noncopyable_function<size_t()>;

/// Uploads the given manifest entries as a new manifest file.
///
/// Returns the size of the resulting uploaded file.
ss::future<checked<size_t, metadata_io::errc>> upload_as_manifest(
  manifest_io& io,
  const uri& path,
  const schema& schema,
  const partition_spec& pspec,
  manifest_content_type content_type,
  chunked_vector<manifest_entry> entries);

/// Merges existing manifest files with new entries, computing file/row
/// counts, building partition summaries, and uploading the result.
///
/// \p content_type controls both the manifest metadata content type and
/// the manifest_file_content tag on the resulting manifest_file.
ss::future<checked<manifest_file, action::errc>> merge_mfiles(
  manifest_io& io,
  const table_metadata& table,
  manifest_num_gen gen_manifest_num,
  manifest_content_type content_type,
  chunked_vector<manifest_file> to_merge,
  chunked_vector<manifest_entry> added_entries,
  std::optional<schema::id_t> max_added_schema_id,
  const partition_spec& pspec,
  const table_snapshot_ctx& ctx);

/// Decides whether to merge manifests or just add a new one, based on
/// the \p min_to_merge threshold.
///
/// Returns the resulting list of manifest files: size 1 when merging,
/// or the original size + 1 otherwise.
ss::future<checked<chunked_vector<manifest_file>, action::errc>>
maybe_merge_mfiles_and_new_entries(
  manifest_io& io,
  const table_metadata& table,
  manifest_num_gen gen_manifest_num,
  manifest_content_type content_type,
  size_t min_to_merge,
  chunked_vector<manifest_file> to_merge,
  chunked_vector<manifest_entry> new_entries,
  std::optional<schema::id_t> max_added_schema_id,
  const partition_spec& pspec,
  const table_snapshot_ctx& ctx);

} // namespace iceberg
