/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "cluster/partition_manager.h"
#include "coproc/ntp_context.h"

#include <filesystem>

namespace coproc {

inline std::filesystem::path offsets_snapshot_path() {
    return config::shard_local_cfg().data_directory().path
           / ".coprocessor_offset_checkpoints";
}

/// Reads the snapshot on disk (if one exists) and returns an initialized
/// ntp_context_cache with all proper storage::logs and stored offsets
ss::future<ntp_context_cache>
recover_offsets(storage::simple_snapshot_manager&, cluster::partition_manager&);

/// Writes all offsets to disk using the snapshot manager
ss::future<>
save_offsets(storage::simple_snapshot_manager&, const ntp_context_cache&);

} // namespace coproc
