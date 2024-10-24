// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "base/outcome.h"
#include "iceberg/action.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_io.h"
#include "iceberg/manifest_list.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"

namespace iceberg {

// Container for a metadata required to build manifest_file::partitions (the
// field summaries for each partition key field).
//
// Unlike the field_summary in manifest_file, which stores bytes per bound,
// this is value-comparable by maintaining the bounds as values instead of
// serialized bytes.
struct field_summary_val {
    using list_t = chunked_vector<field_summary_val>;
    // Creates a list of field summaries meant to summarize the fields of the
    // given partition key type.
    static list_t empty_summaries(const partition_key_type&);
    // Returns this summary with the bounds converted to bytes.
    field_summary release_with_bytes() &&;

    bool contains_null{false};
    std::optional<bool> contains_nan;
    std::optional<primitive_value> lower_bound;
    std::optional<primitive_value> upper_bound;
};

// An action that builds and uploads metadata to append a given list of data
// files to the table's latest snapshot, merging together existing manifests if
// there are too many.
//
// The state that is built by this action is only observable by Iceberg clients
// if the resulting update is successfully committed to the catalog.
//
// Other Iceberg action implementations retry appends some configurable number
// of times. Retries here are left to the caller, who is expected to
// periodically append uncommitted files with a new transaction.
//
// Does not deduplicate new data files against files referenced by existing
// manifests. This is left to the caller, if desired.
//
// TODO: currently throws for tables with multiple partition specs.
// TODO: doesn't clean up any wasted (e.g. on error) manifest files.
// TODO: shouldn't be too difficult to parallelize IO.
class merge_append_action : public action {
public:
    static constexpr size_t default_min_to_merge_new_files = 100;
    static constexpr size_t default_target_size_bytes = 8_MiB;
    merge_append_action(
      manifest_io& io,
      const table_metadata& table,
      chunked_vector<data_file> files,
      size_t min_to_merge_new_files = default_min_to_merge_new_files,
      size_t mfile_target_size_bytes = default_target_size_bytes)
      : io_(io)
      , table_(table)
      , commit_uuid_(uuid_t::create())
      , min_to_merge_new_files_(min_to_merge_new_files)
      , mfile_target_size_bytes_(mfile_target_size_bytes)
      , new_data_files_(std::move(files)) {}

protected:
    ss::future<action_outcome> build_updates() && final;

private:
    // Context containing various fields resolved from the table metadata. The
    // fields here all pertain to the new snapshot created by this action.
    struct table_snapshot_ctx {
        const uuid_t& commit_uuid;
        const schema& schema;
        const partition_spec& pspec;
        const partition_key_type& pk_type;
        const snapshot_id snap_id;
        const sequence_number seq_num;
    };

    // Returns a number that can be used to uniquely identify the next manifest
    // upload within this action.
    size_t generate_manifest_num() { return next_manifest_num_++; }

    // Uploads the given manifest entries as a new manifest, returning the size
    // of the resulting file.
    ss::future<checked<size_t, metadata_io::errc>> upload_as_manifest(
      const manifest_path& path,
      const schema& schema,
      const partition_spec& pspec,
      chunked_vector<manifest_entry> entries);

    // Takes the given list of manifest files and merges them with the given
    // new data files if the list of files is long enough, or just adds a new
    // manifest for the new data files otherwise.
    //
    // Returns the resulting list of manifest files, which will be size 1 in
    // the merging case, or the input size + 1 otherwise.
    ss::future<checked<chunked_vector<manifest_file>, metadata_io::errc>>
    maybe_merge_mfiles_and_new_data(
      chunked_vector<manifest_file> to_merge,
      chunked_vector<data_file> new_data_files,
      const table_snapshot_ctx& ctx);

    // Takes the given list of manifest files and merges them with the optional
    // new manifest entries (i.e. data file metadata).
    ss::future<checked<manifest_file, metadata_io::errc>> merge_mfiles(
      chunked_vector<manifest_file> to_merge,
      const table_snapshot_ctx& ctx,
      field_summary_val::list_t added_summaries = {},
      chunked_vector<manifest_entry> added_entries = {},
      size_t added_rows = 0);

    // Takes the given manifest list and bin-packs them to reduce the number of
    // manifests, adding new data files either to a new manifest or the latest
    // bin if the number of files in the bin has reached a threshold.
    //
    // Returns the resulting list of manifest files, which should encompass all
    // data from the latest snapshot + new data files, and can be written as a
    // new manifest list and committed as a new snapshot.
    ss::future<checked<chunked_vector<manifest_file>, metadata_io::errc>>
    pack_mlist_and_new_data(
      const table_snapshot_ctx& ctx,
      manifest_list old_mlist,
      chunked_vector<data_file> new_data_files);

private:
    manifest_io& io_;
    const table_metadata& table_;
    const uuid_t commit_uuid_;

    // The size in number of manifest files at which the _latest_ bin (the one
    // new files are added to) should be merged.
    const size_t min_to_merge_new_files_;

    // The target size in bytes to bin-pack manifest files.
    const size_t mfile_target_size_bytes_;

    size_t next_manifest_num_{0};
    chunked_vector<data_file> new_data_files_;
};

} // namespace iceberg
