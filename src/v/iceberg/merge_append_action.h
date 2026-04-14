/*
 * Copyright 2024 Redpanda Data, Inc.
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
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_io.h"
#include "iceberg/manifest_list.h"
#include "iceberg/manifest_utils.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"

namespace iceberg {

// File with schema and partition spec information, used as input to
// merge_append_action.
struct file_to_append {
    data_file file;
    schema::id_t schema_id;
    partition_spec::id_t partition_spec_id;
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
// TODO: doesn't clean up any wasted (e.g. on error) manifest files.
// TODO: shouldn't be too difficult to parallelize IO.
class merge_append_action : public action {
public:
    static constexpr size_t default_min_to_merge_new_files = 100;
    static constexpr size_t default_target_size_bytes = 8_MiB;
    merge_append_action(
      manifest_io& io,
      const table_metadata& table,
      chunked_vector<file_to_append> files,
      chunked_vector<std::pair<ss::sstring, ss::sstring>> snapshot_props = {},
      std::optional<ss::sstring> tag_name = std::nullopt,
      std::optional<int64_t> tag_expiration_ms = std::nullopt)
      : io_(io)
      , table_(table)
      , commit_uuid_(uuid_t::create())
      , new_data_files_(std::move(files))
      , snapshot_props_(std::move(snapshot_props))
      , tag_name_(std::move(tag_name))
      , tag_expiration_ms_(tag_expiration_ms) {}

protected:
    ss::future<action_outcome> build_updates() && final;

private:
    size_t generate_manifest_num() { return next_manifest_num_++; }

    manifest_io& io_;
    const table_metadata& table_;
    const uuid_t commit_uuid_;

    size_t next_manifest_num_{0};
    chunked_vector<file_to_append> new_data_files_;
    chunked_vector<std::pair<ss::sstring, ss::sstring>> snapshot_props_;
    std::optional<ss::sstring> tag_name_;
    std::optional<int64_t> tag_expiration_ms_;
};

} // namespace iceberg
