/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "config/node_config.h"
#include "coproc/script_context_router.h"
#include "coproc/types.h"

#include <filesystem>

namespace coproc {

inline std::filesystem::path offsets_snapshot_path() {
    return config::node().data_directory().path
           / ".coprocessor_offset_checkpoints";
}

using all_routes = absl::flat_hash_map<coproc::script_id, routes_t>;

/// Reads the snapshot on disk (if one exists) and returns an initialized
/// ntp_context_cache with all proper storage::logs and stored offsets
ss::future<all_routes> recover_offsets(storage::simple_snapshot_manager&);

/// Writes all offsets to disk using the snapshot manager
ss::future<> save_offsets(storage::simple_snapshot_manager&, all_routes);

} // namespace coproc
