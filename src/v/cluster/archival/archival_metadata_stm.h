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
#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage/types.h"
#include "cluster/errc.h"
#include "cluster/state_machine_registry.h"
#include "cluster/topic_table.h"
#include "features/fwd.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "raft/persisted_stm.h"
#include "storage/record_batch_builder.h"
#include "utils/mutex.h"
#include "utils/prefix_logger.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>

#include <exception>
#include <functional>
#include <system_error>

namespace cluster {

namespace details {
/// This class is supposed to be implemented in unit tests.
class archival_metadata_stm_accessor;
class command_batch_builder_accessor;
} // namespace details

class archival_metadata_stm;

using segment_validated = ss::bool_class<struct segment_validated_tag>;

/// Batch builder allows to combine different archival_metadata_stm commands
/// together in a single record batch
class command_batch_builder {
    friend class command_batch_builder_accessor;

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
    command_batch_builder& add_segments(
      std::vector<cloud_storage::segment_meta>, segment_validated is_validated);
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
    command_batch_builder& process_anomalies(
      model::timestamp scrub_timestamp,
      std::optional<model::offset> last_scrubbed_offset,
      cloud_storage::scrub_status status,
      cloud_storage::anomalies detected);
    command_batch_builder& reset_scrubbing_metadata();
    /// Add read-write fence
    ///
    /// The fence prevents all subsequent commands in the batch from being
    /// applied to the in-memory state of the STM if the 'applied_offset' is
    /// greater than the 'offset'. This mechanism is supposed to be used as a
    /// concurrency-control mechanism.
    command_batch_builder& read_write_fence(model::offset offset);

    /// Add update_highest_producer_id_cmd command
    command_batch_builder&
    update_highest_producer_id(model::producer_id highest_pid);

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
class archival_metadata_stm final : public raft::persisted_stm<> {
    friend class details::archival_metadata_stm_accessor;

public:
    static constexpr std::string_view name = "archival_metadata_stm";
    friend class command_batch_builder;

    explicit archival_metadata_stm(
      raft::consensus*,
      cloud_storage::remote& remote,
      features::feature_table&,
      ss::logger& logger,
      std::optional<cloud_storage::remote_label>,
      std::optional<model::topic_namespace>);

    /// Add segments to the raft log, replicate them and
    /// wait until it is applied to the STM.
    ss::future<std::error_code> add_segments(
      std::vector<cloud_storage::segment_meta>,
      std::optional<model::offset> clean_offset,
      model::producer_id highest_pid,
      ss::lowres_clock::time_point deadline,
      ss::abort_source&,
      segment_validated is_validated);

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

    ss::future<std::error_code> process_anomalies(
      model::timestamp scrub_timestamp,
      std::optional<model::offset> last_scrubbed_offset,
      cloud_storage::scrub_status status,
      cloud_storage::anomalies detected,
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

    /// This method guarantees that the STM applied all changes
    /// in the log to the in-memory state.
    ss::future<std::optional<model::offset>>
    sync(model::timeout_clock::duration timeout);
    ss::future<std::optional<model::offset>>
    sync(model::timeout_clock::duration timeout, ss::abort_source* as);

    model::offset get_start_offset() const;
    model::offset get_last_offset() const;
    model::offset get_archive_start_offset() const;
    model::offset get_archive_clean_offset() const;
    kafka::offset get_start_kafka_offset() const;

    // Return list of all segments that has to be
    // removed from S3.
    fragmented_vector<cloud_storage::partition_manifest::lw_segment_meta>
    get_segments_to_cleanup() const;

    /// Create batch builder that can be used to combine and replicate multiple
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
    model::offset get_insync_offset() const { return last_applied_offset(); }

    model::offset get_last_clean_at() const { return _last_clean_at; };

    model::offset max_collectible_offset() override;

    ss::future<iobuf> take_snapshot(model::offset) final { co_return iobuf{}; }

    size_t get_compacted_replaced_bytes() const {
        return _compacted_replaced_bytes;
    }

    const cloud_storage::remote_path_provider& path_provider() const {
        return _remote_path_provider;
    }

private:
    ss::future<bool>
    do_sync(model::timeout_clock::duration timeout, ss::abort_source* as);

    ss::future<std::error_code> do_add_segments(
      std::vector<cloud_storage::segment_meta>,
      std::optional<model::offset> clean_offset,
      model::producer_id highest_pid,
      ss::lowres_clock::time_point deadline,
      ss::abort_source&,
      segment_validated is_validated);

    // Replicate commands in a batch and wait for their application.
    // Should be called under _lock to ensure linearisability
    ss::future<std::error_code>
    do_replicate_commands(model::record_batch, ss::abort_source&);

    ss::future<> do_apply(const model::record_batch& batch) override;
    ss::future<> apply_raft_snapshot(const iobuf&) override;

    ss::future<>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&&) override;
    ss::future<raft::stm_snapshot>
    take_local_snapshot(ssx::semaphore_units apply_units) override;

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
    struct process_anomalies_cmd;
    struct reset_scrubbing_metadata;
    struct update_highest_producer_id_cmd;
    struct read_write_fence_cmd;
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
    void apply_process_anomalies(iobuf);
    void apply_reset_scrubbing_metadata();
    void apply_update_highest_producer_id(model::producer_id pid);
    // apply fence command and return true if the 'apply' call should
    // be interrupted
    bool apply_read_write_fence(const read_write_fence_cmd&) noexcept;

    // Notify current waiter in the 'do_replicate'
    void maybe_notify_waiter(cluster::errc) noexcept;
    void maybe_notify_waiter(std::exception_ptr) noexcept;

private:
    prefix_logger _logger;

    mutex _lock{"archival_metadata_stm"};

    ss::shared_ptr<util::mem_tracker> _mem_tracker;
    ss::shared_ptr<cloud_storage::partition_manifest> _manifest;

    // The offset of the last mark_clean_cmd applied: if the manifest is
    // clean, this will equal last_applied_offset.
    model::offset _last_clean_at;

    // The offset of the last record that modified this stm
    model::offset _last_dirty_at;

    std::optional<ss::promise<errc>> _active_operation_res;

    cloud_storage::remote& _cloud_storage_api;
    features::feature_table& _feature_table;
    const cloud_storage::remote_path_provider _remote_path_provider;
    ss::abort_source _download_as;

    // for observability: keep track of the number of cloud bytes "removed" by
    // compaction. as in the size difference between the original segment(s) and
    // compacted segment reuploaded note that this value does not correlate to
    // the change in size in cloud storage until the original segment(s) are
    // garbage collected.
    size_t _compacted_replaced_bytes{0};
};

class archival_metadata_stm_factory : public state_machine_factory {
public:
    archival_metadata_stm_factory(
      bool cloud_storage_enabled,
      ss::sharded<cloud_storage::remote>&,
      ss::sharded<features::feature_table>&,
      ss::sharded<cluster::topic_table>&);

    bool is_applicable_for(const storage::ntp_config&) const final;
    void create(raft::state_machine_manager_builder&, raft::consensus*) final;

private:
    bool _cloud_storage_enabled;
    ss::sharded<cloud_storage::remote>& _cloud_storage_api;
    ss::sharded<features::feature_table>& _feature_table;
    ss::sharded<topic_table>& _topics;
};

} // namespace cluster
