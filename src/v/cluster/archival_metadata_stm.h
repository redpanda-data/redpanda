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

#include "cloud_storage/fwd.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/types.h"
#include "cluster/persisted_stm.h"
#include "features/fwd.h"
#include "model/metadata.h"
#include "model/record.h"
#include "storage/record_batch_builder.h"
#include "utils/mutex.h"
#include "utils/prefix_logger.h"

#include <seastar/util/log.hh>

#include <functional>
#include <system_error>

namespace cluster {

namespace details {
/// This class is supposed to be implemented in unit tests.
class archival_metadata_stm_accessor;
} // namespace details

class archival_metadata_stm;

/// Batch builder allows to combine different archival_metadata_stm commands
/// together in a single record batch
class command_batch_builder {
public:
    command_batch_builder(
      archival_metadata_stm& stm,
      ss::lowres_clock::time_point deadline,
      std::optional<std::reference_wrapper<ss::abort_source>> = std::nullopt);
    command_batch_builder(const command_batch_builder&) = delete;
    command_batch_builder(command_batch_builder&&) = default;
    command_batch_builder& operator=(const command_batch_builder&) = delete;
    command_batch_builder& operator=(command_batch_builder&&) = default;
    ~command_batch_builder() = default;
    /// Add segments to the batch
    command_batch_builder&
      add_segments(std::vector<cloud_storage::segment_meta>);
    /// Add cleanup_metadata command to the batch
    command_batch_builder& cleanup_metadata();
    /// Add truncate command to the batch
    command_batch_builder& truncate(model::offset start_rp_offset);
    /// Replicate the configuration batch
    ss::future<std::error_code> replicate();

private:
    std::reference_wrapper<archival_metadata_stm> _stm;
    storage::record_batch_builder _builder;
    ss::lowres_clock::time_point _deadline;
    std::optional<std::reference_wrapper<ss::abort_source>> _as;
    ss::gate::holder _holder;
};

/// This replicated state machine allows storing archival manifest (a set of
/// segments archived to cloud storage) in the archived partition log itself.
/// This is needed to 1) avoid querying cloud storage on partition startup and
/// 2) to replicate metadata to raft followers so that they can decide which
/// segments can be safely evicted.
class archival_metadata_stm final : public persisted_stm {
    friend class details::archival_metadata_stm_accessor;

public:
    friend class command_batch_builder;

    explicit archival_metadata_stm(
      raft::consensus*,
      cloud_storage::remote& remote,
      features::feature_table&,
      ss::logger& logger,
      ss::shared_ptr<util::mem_tracker> partition_mem_tracker = nullptr);

    /// Add segments to the raft log, replicate them and
    /// wait until it is applied to the STM.
    ss::future<std::error_code> add_segments(
      std::vector<cloud_storage::segment_meta>,
      ss::lowres_clock::time_point deadline,
      std::optional<std::reference_wrapper<ss::abort_source>> = std::nullopt);

    /// Truncate local snapshot
    ///
    /// This method doesn't actually delete the entries from the snapshot
    /// but advances the start offset stored inside it. After that the segments
    /// which are referenced by the entries below 'start_rp_offset' can be
    /// safely removed from the remote storage.
    ss::future<std::error_code> truncate(
      model::offset start_rp_offset,
      ss::lowres_clock::time_point deadline,
      std::optional<std::reference_wrapper<ss::abort_source>> = std::nullopt);

    /// Removes replaced and truncated segments from the snapshot
    ss::future<std::error_code> cleanup_metadata(
      ss::lowres_clock::time_point deadline,
      std::optional<std::reference_wrapper<ss::abort_source>> = std::nullopt);

    /// A set of archived segments. NOTE: manifest can be out-of-date if this
    /// node is not leader; or if the STM hasn't yet performed sync; or if the
    /// node has lost leadership. But it will contain segments successfully
    /// added with `add_segments`.
    const cloud_storage::partition_manifest& manifest() const;

    ss::future<> stop() override;

    static ss::future<> make_snapshot(
      const storage::ntp_config& ntp_cfg,
      const cloud_storage::partition_manifest& m,
      model::offset insync_offset);

    using persisted_stm::sync;

    model::offset get_start_offset() const;
    model::offset get_last_offset() const;

    // Return list of all segments that has to be
    // removed from S3.
    std::vector<cloud_storage::partition_manifest::lw_segment_meta>
    get_segments_to_cleanup() const;

    /// Create batch builder that can be used to combine and replicate multipe
    /// STM commands together
    command_batch_builder batch_start(
      ss::lowres_clock::time_point deadline,
      std::optional<std::reference_wrapper<ss::abort_source>> = std::nullopt);

    /// Acquire the lock that prevents modification of the manifest.
    auto acquire_manifest_lock() { return _manifest_lock.get_units(); }

private:
    bool cleanup_needed() const;

    ss::future<std::error_code> do_add_segments(
      std::vector<cloud_storage::segment_meta>,
      ss::lowres_clock::time_point deadline,
      std::optional<std::reference_wrapper<ss::abort_source>>);

    ss::future<std::error_code> do_truncate(
      model::offset,
      ss::lowres_clock::time_point,
      std::optional<std::reference_wrapper<ss::abort_source>>);

    ss::future<std::error_code> do_cleanup_metadata(
      ss::lowres_clock::time_point,
      std::optional<std::reference_wrapper<ss::abort_source>>);

    ss::future<std::error_code> do_replicate_commands(
      model::record_batch,
      ss::lowres_clock::time_point,
      std::optional<std::reference_wrapper<ss::abort_source>>);

    ss::future<> apply(model::record_batch batch) override;
    ss::future<> handle_eviction() override;

    ss::future<> apply_snapshot(stm_snapshot_header, iobuf&&) override;
    ss::future<stm_snapshot> take_snapshot() override;
    model::offset max_collectible_offset() override;

    struct segment;
    struct start_offset;
    struct add_segment_cmd;
    struct truncate_cmd;
    struct update_start_offset_cmd;
    struct cleanup_metadata_cmd;
    struct snapshot;

    friend segment segment_from_meta(const cloud_storage::segment_meta& meta);

    static fragmented_vector<segment>
    segments_from_manifest(const cloud_storage::partition_manifest& manifest);

    static fragmented_vector<segment> replaced_segments_from_manifest(
      const cloud_storage::partition_manifest& manifest);

    void apply_add_segment(const segment& segment);
    void apply_truncate(const start_offset& so);
    void apply_cleanup_metadata();
    void apply_update_start_offset(const start_offset& so);

private:
    prefix_logger _logger;

    mutex _lock;
    mutex _manifest_lock;

    ss::shared_ptr<cloud_storage::partition_manifest> _manifest;

    cloud_storage::remote& _cloud_storage_api;
    features::feature_table& _feature_table;
    ss::abort_source _download_as;
};

} // namespace cluster
