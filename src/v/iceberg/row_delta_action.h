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

#include "iceberg/action.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_io.h"
#include "iceberg/manifest_utils.h"
#include "iceberg/merge_append_action.h"
#include "iceberg/table_metadata.h"

namespace iceberg {

struct file_to_delete {
    data_file file;
    schema::id_t schema_id;
    partition_spec::id_t partition_spec_id;
};

/// An action that commits both data files and delete files to a table's
/// latest snapshot. Data files are handled identically to merge_append_action
/// (bin-packing and merging existing data manifests). Delete files are written
/// to a new delete manifest without merging into existing delete manifests.
///
/// Produces snapshot operations:
///   - overwrite: when both data and delete files are present
///   - delete_data: when only delete files are present
///   - append: when only data files are present
class row_delta_action : public action {
public:
    row_delta_action(
      manifest_io& io,
      const table_metadata& table,
      chunked_vector<file_to_append> data_files,
      chunked_vector<file_to_delete> delete_files,
      chunked_vector<std::pair<ss::sstring, ss::sstring>> snapshot_props = {},
      std::optional<ss::sstring> tag_name = std::nullopt,
      std::optional<int64_t> tag_expiration_ms = std::nullopt);

protected:
    ss::future<action_outcome> build_updates() && final;

private:
    size_t generate_manifest_num() { return next_manifest_num_++; }

    manifest_io& io_;
    const table_metadata& table_;
    const uuid_t commit_uuid_;
    size_t next_manifest_num_{0};
    chunked_vector<file_to_append> new_data_files_;
    chunked_vector<file_to_delete> new_delete_files_;
    chunked_vector<std::pair<ss::sstring, ss::sstring>> snapshot_props_;
    std::optional<ss::sstring> tag_name_;
    std::optional<int64_t> tag_expiration_ms_;
};

} // namespace iceberg
