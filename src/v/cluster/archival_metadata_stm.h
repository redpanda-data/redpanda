/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote.h"
#include "cluster/persisted_stm.h"
#include "model/metadata.h"
#include "utils/mutex.h"
#include "utils/prefix_logger.h"

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

    /// Add the difference between manifests to the raft log, replicate it and
    /// wait until it is applied to the STM.
    ss::future<std::error_code> add_segments(
      const cloud_storage::partition_manifest&,
      ss::lowres_clock::time_point deadline,
      std::optional<std::reference_wrapper<ss::abort_source>> = std::nullopt);

    /// A set of archived segments. NOTE: manifest can be out-of-date if this
    /// node is not leader; or if the STM hasn't yet performed sync; or if the
    /// node has lost leadership. But it will contain segments successfully
    /// added with `add_segments`.
    const cloud_storage::partition_manifest& manifest() const {
        return _manifest;
    }

    ss::future<> stop() override;

    static ss::future<> make_snapshot(
      const storage::ntp_config& ntp_cfg,
      const cloud_storage::partition_manifest& m,
      model::offset insync_offset);

private:
    ss::future<std::error_code> do_add_segments(
      const cloud_storage::partition_manifest&,
      ss::lowres_clock::time_point deadline,
      std::optional<std::reference_wrapper<ss::abort_source>>);

    ss::future<> apply(model::record_batch batch) override;
    ss::future<> handle_eviction() override;

    ss::future<> apply_snapshot(stm_snapshot_header, iobuf&&) override;
    ss::future<stm_snapshot> take_snapshot() override;
    model::offset max_collectible_offset() override;

    struct segment;
    struct add_segment_cmd;
    struct snapshot;

    static std::vector<segment>
    segments_from_manifest(const cloud_storage::partition_manifest& manifest);

    void apply_add_segment(const segment& segment);

private:
    prefix_logger _logger;

    mutex _lock;

    cloud_storage::partition_manifest _manifest;
    model::offset _start_offset;
    model::offset _last_offset;

    cloud_storage::remote& _cloud_storage_api;
    ss::abort_source _download_as;
};

} // namespace cluster
