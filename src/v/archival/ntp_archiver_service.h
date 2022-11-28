/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once
#include "archival/archival_policy.h"
#include "archival/probe.h"
#include "archival/types.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/client.h"
#include "cluster/fwd.h"
#include "cluster/partition.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "storage/fwd.h"
#include "storage/segment.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>

#include <functional>
#include <map>

namespace archival {

using namespace std::chrono_literals;

enum class segment_upload_kind { compacted, non_compacted };

std::ostream& operator<<(std::ostream& os, segment_upload_kind upload_kind);

/// This class performs per-ntp arhcival workload. Every ntp can be
/// processed independently, without the knowledge about others. All
/// 'ntp_archiver' instances that the shard posesses are supposed to be
/// aggregated on a higher level in the 'archiver_service'.
///
/// The 'ntp_archiver' is responsible for manifest manitpulations and
/// generation of per-ntp candidate set. The actual file uploads are
/// handled by 'archiver_service'.
///
/// Note that archiver uses initial revision of the partition, not the
/// current one. The revision of the partition can change when the partition
/// is moved between the nodes. To make all object names stable inside
/// the S3 bucket we're using initial revision. The revision that the
/// topic was assigned when it was just created.
class ntp_archiver {
public:
    /// Iterator type used to retrieve candidates for upload
    using back_insert_iterator
      = std::back_insert_iterator<std::vector<segment_name>>;

    enum class loop_state {
        initial,
        started,
        stopped,
    };

    /// Create new instance
    ///
    /// \param ntp is an ntp that archiver is responsible for
    /// \param conf is an S3 client configuration
    /// \param remote is an object used to send/recv data
    /// \param svc_probe is a service level probe (optional)
    ntp_archiver(
      const storage::ntp_config& ntp,
      const configuration& conf,
      cloud_storage::remote& remote,
      cluster::partition& parent);

    /// Start the fiber that will upload the partition data to the cloud
    /// storage. Can be started only once.
    void run_upload_loop();

    void run_sync_manifest_loop();

    /// Spawn background fibers, which depending on the mode (read replica or
    /// not) will either do uploads, or periodically read back the manifest.
    ss::future<> start();

    /// Stop archiver.
    ///
    /// \return future that will become ready when all async operation will be
    /// completed
    ss::future<> stop();

    bool upload_loop_stopped() const {
        return _upload_loop_state == loop_state::stopped;
    }

    bool sync_manifest_loop_stopped() const {
        return _sync_manifest_loop_state == loop_state::stopped;
    }

    /// Query if either of the manifest sync loop or upload loop has stopped
    /// These are mutually exclusive loops, and if any one has transitioned to a
    /// stopped state then the archiver is stopped.
    bool is_loop_stopped() const {
        return _sync_manifest_loop_state == loop_state::stopped
               || _upload_loop_state == loop_state::stopped;
    }

    /// Get NTP
    const model::ntp& get_ntp() const;

    /// Get revision id
    model::initial_revision_id get_revision_id() const;

    /// Get timestamp
    const ss::lowres_clock::time_point get_last_upload_time() const;

    /// Download manifest from pre-defined S3 locatnewion
    ///
    /// \return future that returns true if the manifest was found in S3
    ss::future<std::pair<
      cloud_storage::partition_manifest,
      cloud_storage::download_result>>
    download_manifest();

    struct upload_group_result {
        size_t num_succeeded;
        size_t num_failed;
        size_t num_cancelled;

        auto operator<=>(const upload_group_result&) const = default;
    };

    struct batch_result {
        upload_group_result non_compacted_upload_result;
        upload_group_result compacted_upload_result;

        auto operator<=>(const batch_result&) const = default;
    };

    /// \brief Upload next set of segments to S3 (if any)
    /// The semaphore is used to track number of parallel uploads. The method
    /// will pick not more than '_concurrency' candidates and start
    /// uploading them.
    ///
    /// \param lso_override last stable offset override
    /// \return future that returns number of uploaded/failed segments
    virtual ss::future<batch_result> upload_next_candidates(
      std::optional<model::offset> last_stable_offset_override = std::nullopt);

    ss::future<cloud_storage::download_result> sync_manifest();

    uint64_t estimate_backlog_size();

    /// \brief Probe remote storage and truncate the manifest if needed
    ss::future<std::optional<cloud_storage::partition_manifest>>
    maybe_truncate_manifest(retry_chain_node& rtc);

    /// \brief Perform housekeeping operations.
    ss::future<> housekeeping();

    /// \brief Advance the start offest for the remote partition
    /// according to the retention policy specified by the partition
    /// configuration. This function does *not* delete any data.
    ss::future<> apply_retention();

    /// \brief Remove segments that are no longer queriable by:
    /// segments that are below the current start offset and segments
    /// that have been replaced with their compacted equivalent.
    ss::future<> garbage_collect();

    virtual ~ntp_archiver() = default;

private:
    /// Information about started upload
    struct scheduled_upload {
        /// The future that will be ready when the segment will be fully
        /// uploaded
        std::optional<ss::future<cloud_storage::upload_result>> result;
        /// Last offset of the uploaded segment or part
        model::offset inclusive_last_offset;
        /// Segment metadata
        std::optional<cloud_storage::partition_manifest::segment_meta> meta;
        /// Name of the uploaded segment
        std::optional<ss::sstring> name;
        /// Offset range convered by the upload
        std::optional<model::offset> delta;
        /// Contains 'no' if the method can be called another time or 'yes'
        /// if it shouldn't be called (if there is no data to upload).
        /// If the 'stop' is 'no' the 'result' might be 'nullopt'. In this
        /// case the upload is not started but the method might be called
        /// again anyway.
        ss::stop_iteration stop;
        /// Protects the underlying segment(s) from being deleted while the
        /// upload is in flight.
        std::vector<ss::rwlock::holder> segment_read_locks;
        segment_upload_kind upload_kind;
    };

    using allow_reuploads_t = ss::bool_class<struct allow_reupload_tag>;
    /// An upload context represents a range of offsets to be uploaded. It will
    /// search for segments within this range and upload them, it also carries
    /// some context information like whether re-uploads are allowed, what is
    /// the maximum number of in-flight uploads that can be processed etc.
    struct upload_context {
        /// The kind of segment being uploaded
        segment_upload_kind upload_kind;
        /// The next scheduled upload will start from this offset
        model::offset start_offset;
        /// Uploads will stop at this offset
        model::offset last_offset;
        /// Controls checks for reuploads, compacted segments have this check
        /// disabled
        allow_reuploads_t allow_reuploads;
        /// Collection of uploads scheduled so far
        std::vector<scheduled_upload> uploads{};

        /// Schedules a single upload, adds it to upload collection and
        /// progresses the start offset
        ss::future<ss::stop_iteration>
        schedule_single_upload(ntp_archiver& archiver);

        upload_context(
          segment_upload_kind upload_kind,
          model::offset start_offset,
          model::offset last_offset,
          allow_reuploads_t allow_reuploads);
    };

    /// Start upload without waiting for it to complete
    ss::future<scheduled_upload>
    schedule_single_upload(const upload_context& upload_ctx);

    /// Start all uploads
    ss::future<std::vector<scheduled_upload>>
    schedule_uploads(model::offset last_stable_offset);

    ss::future<std::vector<scheduled_upload>>
    schedule_uploads(std::vector<upload_context> loop_contexts);

    /// Wait until all scheduled uploads will be completed
    ///
    /// Update the probe and manifest
    ss::future<ntp_archiver::batch_result> wait_all_scheduled_uploads(
      std::vector<ntp_archiver::scheduled_upload> scheduled);

    /// Waits for scheduled segment uploads. The uploaded segments could be
    /// compacted or non-compacted, the actions taken are similar in both cases
    /// with the major difference being the probe updates done after the upload.
    ss::future<ntp_archiver::upload_group_result> wait_uploads(
      std::vector<scheduled_upload> scheduled,
      segment_upload_kind segment_kind);

    /// Upload individual segment to S3.
    ///
    /// \return error code
    ss::future<cloud_storage::upload_result>
    upload_segment(upload_candidate candidate);

    /// Upload segment's transactions metadata to S3.
    ///
    /// \return error code
    ss::future<cloud_storage::upload_result>
    upload_tx(upload_candidate candidate);

    /// Upload manifest to the pre-defined S3 location
    ss::future<cloud_storage::upload_result> upload_manifest();

    /// Within a particular term, while leader, keep trying to upload data.
    ss::future<> upload_loop();

    /// Run upload loop until term change, then start it again.  Stop when
    /// abort source fires.
    ss::future<> outer_upload_loop();

    /// Launch the sync manifest loop fiber.
    ss::future<> sync_manifest_loop();

    /// Delete a segment and its transaction metadata from S3.
    /// The transaction metadata is only deleted if the segment
    /// deletion was successful.
    ///
    /// Throws if an abort was requested.
    ss::future<cloud_storage::upload_result>
    delete_segment(const remote_segment_path& path);

    void update_probe();

    bool upload_loop_can_continue() const;
    bool sync_manifest_loop_can_continue() const;
    bool housekeeping_can_continue() const;
    const cloud_storage::partition_manifest& manifest() const;

    /// Helper to generate a segment path from candidate
    remote_segment_path
    segment_path_for_candidate(const upload_candidate& candidate);

    /// Method to use with lazy_abort_source
    bool archiver_lost_leadership(cloud_storage::lazy_abort_source& las);

    model::ntp _ntp;
    model::initial_revision_id _rev;
    cloud_storage::remote& _remote;
    cluster::partition& _parent;
    model::term_id _start_term;
    archival_policy _policy;
    cloud_storage_clients::bucket_name _bucket;
    ss::gate _gate;
    ss::abort_source _as;
    retry_chain_node _rtcnode;
    retry_chain_logger _rtclog;
    ss::lowres_clock::duration _cloud_storage_initial_backoff;
    ss::lowres_clock::duration _segment_upload_timeout;
    ss::lowres_clock::duration _metadata_sync_timeout;
    ssx::semaphore _mutex{1, "archive/ntp"};
    ss::lowres_clock::duration _upload_loop_initial_backoff;
    ss::lowres_clock::duration _upload_loop_max_backoff;
    config::binding<std::chrono::milliseconds> _sync_manifest_timeout;
    config::binding<size_t> _max_segments_pending_deletion;
    simple_time_jitter<ss::lowres_clock> _backoff_jitter{100ms};
    size_t _concurrency{4};
    ss::lowres_clock::time_point _last_upload_time;
    ss::scheduling_group _upload_sg;
    ss::io_priority_class _io_priority;

    config::binding<std::chrono::milliseconds> _housekeeping_interval;
    simple_time_jitter<ss::lowres_clock> _housekeeping_jitter;
    ss::lowres_clock::time_point _next_housekeeping;

    per_ntp_metrics_disabled _ntp_metrics_disabled;
    std::optional<ntp_level_probe> _probe{std::nullopt};

    loop_state _upload_loop_state{loop_state::initial};
    loop_state _sync_manifest_loop_state{loop_state::initial};

    const cloud_storage_clients::object_tag_formatter _segment_tags;
    const cloud_storage_clients::object_tag_formatter _manifest_tags;
    const cloud_storage_clients::object_tag_formatter _tx_tags;
};

} // namespace archival
