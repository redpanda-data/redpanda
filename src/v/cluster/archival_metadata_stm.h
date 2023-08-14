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

#include <seastar/core/circular_buffer.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>

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
      ss::abort_source& as);
    command_batch_builder(const command_batch_builder&) = delete;
    command_batch_builder(command_batch_builder&&) = default;
    command_batch_builder& operator=(const command_batch_builder&) = delete;
    // command_batch_builder& operator=(command_batch_builder&&) = default;
    ~command_batch_builder() = default;
    /// Reset the manifest.
    command_batch_builder& reset_metadata();
    /// Add segments to the batch
    command_batch_builder&
      add_segments(std::vector<cloud_storage::segment_meta>);
    /// Add cleanup_metadata command to the batch
    command_batch_builder& cleanup_metadata();
    /// Add mark_clean command to the batch
    command_batch_builder& mark_clean(model::offset);
    /// Add truncate command to the batch
    command_batch_builder& truncate(model::offset start_rp_offset);
    /// Update the kafka start offset override.
    command_batch_builder&
    update_start_kafka_offset(kafka::offset start_kafka_offset);
    /// Add spillover command to the batch
    command_batch_builder& spillover(const cloud_storage::segment_meta& meta);
    /// Add truncate-archive-init command to the batch
    command_batch_builder& truncate_archive_init(
      model::offset start_rp_offset, model::offset_delta delta);
    /// Add truncate-archive-commit command to the batch
    command_batch_builder&
    /// Totally replace the manifest
    cleanup_archive(model::offset start_rp_offset, uint64_t removed_size_bytes);
    command_batch_builder& replace_manifest(iobuf);
    /// Replicate the configuration batch
    ss::future<std::error_code> replicate();

private:
    std::reference_wrapper<archival_metadata_stm> _stm;
    storage::record_batch_builder _builder;
    ss::lowres_clock::time_point _deadline;
    ss::abort_source& _as;
    ss::gate::holder _holder;
};

/// This replicated state machine allows storing archival manifest (a set of
/// segments archived to cloud storage) in the archived partition log itself.
/// This is needed to 1) avoid querying cloud storage on partition startup and
/// 2) to replicate metadata to raft followers so that they can decide which
/// segments can be safely evicted.
class archival_metadata_stm final : public persisted_stm<> {
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
      std::optional<model::offset> clean_offset,
      ss::lowres_clock::time_point deadline,
      ss::abort_source&);

    /// Truncate local snapshot by moving start_offset forward
    ///
    /// This method doesn't actually delete the entries from the snapshot
    /// but advances the start offset stored inside it. After that the segments
    /// which are referenced by the entries below 'start_rp_offset' can be
    /// safely removed from the remote storage.
    ss::future<std::error_code> truncate(
      model::offset start_rp_offset,
      ss::lowres_clock::time_point deadline,
      ss::abort_source&);
    ss::future<std::error_code> truncate(
      kafka::offset start_kafka_offset,
      ss::lowres_clock::time_point deadline,
      ss::abort_source&);

    /// Truncate local snapshot
    ///
    /// This method removes entries from the local snapshot and moves
    /// start_rp_offset forward. The entries are supposed to be moved to archive
    /// by the caller.
    ss::future<std::error_code> spillover(
      const cloud_storage::segment_meta& manifest_meta,
      ss::lowres_clock::time_point deadline,
      ss::abort_source&);

    /// Truncate archive area
    ///
    /// This method moves archive_start_offset forward.
    ss::future<std::error_code> truncate_archive_init(
      model::offset start_rp_offset,
      model::offset_delta delta,
      ss::lowres_clock::time_point deadline,
      ss::abort_source&);

    /// Removes replaced and truncated segments from the snapshot
    ss::future<std::error_code>
    cleanup_metadata(ss::lowres_clock::time_point deadline, ss::abort_source&);

    /// Propagates archive_clean_offset forward
    ss::future<std::error_code> cleanup_archive(
      model::offset start_rp_offset,
      uint64_t removed_size_bytes,
      ss::lowres_clock::time_point deadline,
      ss::abort_source&);

    /// Declare that the manifest is clean as of a particular insync_offset:
    /// the insync offset should have been captured before starting the upload,
    /// as the offset at end of upload might have changed.
    ss::future<std::error_code>
    mark_clean(ss::lowres_clock::time_point, model::offset, ss::abort_source&);

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

    static ss::circular_buffer<model::record_batch>
    serialize_manifest_as_batches(
      model::offset base_offset, const cloud_storage::partition_manifest& m);

    /// Create log segment with manifest data on disk
    ///
    /// \param ntp_cfg is an ntp config of the partition
    /// \param base_offset is a base offset of the new segment
    /// \param term is a term of the new segment
    /// \param manifest is a manifest which is used as a source
    static ss::future<> create_log_segment_with_config_batches(
      const storage::ntp_config& ntp_cfg,
      model::offset base_offset,
      model::term_id term,
      const cloud_storage::partition_manifest& manifest);

    using persisted_stm::sync;

    model::offset get_start_offset() const;
    model::offset get_last_offset() const;
    model::offset get_archive_start_offset() const;
    model::offset get_archive_clean_offset() const;
    kafka::offset get_start_kafka_offset() const;

    // Return list of all segments that has to be
    // removed from S3.
    std::vector<cloud_storage::partition_manifest::lw_segment_meta>
    get_segments_to_cleanup() const;

    /// Create batch builder that can be used to combine and replicate multipe
    /// STM commands together
    command_batch_builder
    batch_start(ss::lowres_clock::time_point deadline, ss::abort_source&);

    enum class state_dirty : uint8_t { dirty, clean };

    // If state_dirty::dirty is returned the manifest should be uploaded
    // to object store at the next opportunity.
    state_dirty get_dirty(
      std::optional<model::offset> projected_clean = std::nullopt) const;

    // Users of the stm need to know insync offset in order to pass
    // the proper value to mark_clean
    model::offset get_insync_offset() const { return _insync_offset; }

    model::offset get_last_clean_at() const { return _last_clean_at; };

    model::offset max_collectible_offset() override;

private:
    ss::future<std::error_code> do_add_segments(
      std::vector<cloud_storage::segment_meta>,
      std::optional<model::offset> clean_offset,
      ss::lowres_clock::time_point deadline,
      ss::abort_source&);

    ss::future<std::error_code>
    do_replicate_commands(model::record_batch, ss::abort_source&);

    ss::future<> apply(model::record_batch batch) override;
    ss::future<> handle_raft_snapshot() override;

    ss::future<> apply_snapshot(stm_snapshot_header, iobuf&&) override;
    ss::future<stm_snapshot> take_snapshot() override;

    struct segment;
    struct start_offset;
    struct start_offset_with_delta;
    struct add_segment_cmd;
    struct truncate_cmd;
    struct update_start_offset_cmd;
    struct update_start_kafka_offset_cmd;
    struct cleanup_metadata_cmd;
    struct mark_clean_cmd;
    struct truncate_archive_init_cmd;
    struct truncate_archive_commit_cmd;
    struct reset_metadata_cmd;
    struct spillover_cmd;
    struct replace_manifest_cmd;
    struct snapshot;

    friend segment segment_from_meta(const cloud_storage::segment_meta& meta);

    static fragmented_vector<segment>
    segments_from_manifest(const cloud_storage::partition_manifest& manifest);

    static fragmented_vector<segment> replaced_segments_from_manifest(
      const cloud_storage::partition_manifest& manifest);

    static fragmented_vector<segment>
    spillover_from_manifest(const cloud_storage::partition_manifest& manifest);

    void apply_add_segment(const segment& segment);
    void apply_truncate(const start_offset& so);
    void apply_cleanup_metadata();
    void apply_mark_clean(model::offset);
    void apply_update_start_offset(const start_offset& so);
    void apply_truncate_archive_init(const start_offset_with_delta& so);
    void
    apply_truncate_archive_commit(model::offset co, uint64_t bytes_removed);
    void apply_update_start_kafka_offset(kafka::offset so);
    void apply_reset_metadata();
    void apply_spillover(const spillover_cmd& so);
    void apply_replace_manifest(iobuf);

private:
    prefix_logger _logger;

    mutex _lock;

    ss::shared_ptr<cloud_storage::partition_manifest> _manifest;

    // The offset of the last mark_clean_cmd applied: if the manifest is
    // clean, this will equal _insync_offset.
    model::offset _last_clean_at;

    // The offset of the last record that modified this stm
    model::offset _last_dirty_at;

    cloud_storage::remote& _cloud_storage_api;
    features::feature_table& _feature_table;
    ss::abort_source _download_as;
};

} // namespace cluster
