/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cloud_storage/manifest.h"
#include "cloud_storage/remote.h"
#include "cluster/persisted_stm.h"
#include "raft/log_eviction_stm.h"
#include "utils/mutex.h"
#include "utils/prefix_logger.h"
#include "utils/retry_chain_node.h"

#include <seastar/util/log.hh>

namespace cluster {

/// This replicated state machine allows storing archival manifest (a set of
/// segments archived to cloud storage) in the archived partition log itself.
/// This is needed to 1) avoid querying cloud storage on partition startup and
/// 2) to replicate metadata to raft followers so that they can decide which
/// segments can be safely evicted.
class archival_metadata_stm final : public persisted_stm {
public:
    explicit archival_metadata_stm(
      raft::consensus*, cloud_storage::remote& remote, ss::logger& logger);

    /// If log_eviction_stm is set, archival_metadata_stm will only allow
    /// eviction of archived segments.
    void set_log_eviction_stm(
      ss::lw_shared_ptr<raft::log_eviction_stm> log_eviction_stm) {
        _log_eviction_stm = std::move(log_eviction_stm);
    }

    /// Add the difference between manifests to the raft log, replicate it and
    /// wait until it is applied to the STM.
    ss::future<bool>
    add_segments(const cloud_storage::manifest&, retry_chain_node&);

    /// A set of archived segments.
    const cloud_storage::manifest& manifest() const { return _manifest; }

    ss::future<> stop() override;

private:
    ss::future<bool>
    do_add_segments(const cloud_storage::manifest&, retry_chain_node&);

    ss::future<> apply(model::record_batch batch) override;
    ss::future<> handle_eviction() override;

    ss::future<> apply_snapshot(stm_snapshot_header, iobuf&&) override;
    ss::future<stm_snapshot> take_snapshot() override;

    struct segment;
    struct add_segment_cmd;
    struct snapshot;

    static std::vector<segment>
    segments_from_manifest(const cloud_storage::manifest& manifest);

    void apply_add_segment(const segment& segment);

private:
    prefix_logger _logger;

    mutex _lock;

    cloud_storage::manifest _manifest;
    model::offset _start_offset;
    model::offset _last_offset;

    cloud_storage::remote& _cloud_storage_api;
    ss::abort_source _download_as;

    ss::lw_shared_ptr<raft::log_eviction_stm> _log_eviction_stm;
};

} // namespace cluster
