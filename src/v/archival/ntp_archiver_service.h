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
#include "cloud_storage/remote.h"
#include "cloud_storage/types.h"
#include "cluster/fwd.h"
#include "cluster/partition.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "s3/client.h"
#include "storage/fwd.h"
#include "storage/segment.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>

#include <functional>
#include <map>

namespace archival {

using namespace std::chrono_literals;

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

    /// Create new instance
    ///
    /// \param ntp is an ntp that archiver is responsible for
    /// \param conf is an S3 client configuration
    /// \param remote is an object used to send/recv data
    /// \param svc_probe is a service level probe (optional)
    ntp_archiver(
      const storage::ntp_config& ntp,
      cluster::partition_manager&,
      const configuration& conf,
      cloud_storage::remote& remote,
      ss::lw_shared_ptr<cluster::partition> part);

    /// Start the fiber that will upload the partition data to the cloud
    /// storage. Can be started only once.
    void run_upload_loop();

    void run_sync_manifest_loop();

    /// Stop archiver.
    ///
    /// \return future that will become ready when all async operation will be
    /// completed
    ss::future<> stop();

    bool upload_loop_stopped() const { return _upload_loop_stopped; }
    bool sync_manifest_loop_stopped() const {
        return _sync_manifest_loop_stopped;
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
    ss::future<cloud_storage::download_result> download_manifest();

    const cloud_storage::partition_manifest& get_remote_manifest() const;

    struct batch_result {
        size_t num_succeded;
        size_t num_failed;
    };

    /// \brief Upload next set of segments to S3 (if any)
    /// The semaphore is used to track number of parallel uploads. The method
    /// will pick not more than '_concurrency' candidates and start
    /// uploading them.
    ///
    /// \param lso_override last stable offset override
    /// \return future that returns number of uploaded/failed segments
    ss::future<batch_result> upload_next_candidates(
      std::optional<model::offset> last_stable_offset_override = std::nullopt);

    ss::future<cloud_storage::download_result> sync_manifest();

    uint64_t estimate_backlog_size();

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
    };

    /// Start upload without waiting for it to complete
    ss::future<scheduled_upload> schedule_single_upload(
      model::offset last_uploaded_offset, model::offset last_stable_offset);

    /// Start all uploads
    ss::future<std::vector<scheduled_upload>>
    schedule_uploads(model::offset last_stable_offset);

    /// Wait until all scheduled uploads will be completed
    ///
    /// Update the probe and manifest
    ss::future<ntp_archiver::batch_result> wait_all_scheduled_uploads(
      std::vector<ntp_archiver::scheduled_upload> scheduled);

    /// Upload individual segment to S3.
    ///
    /// \return true on success and false otherwise
    ss::future<cloud_storage::upload_result>
    upload_segment(upload_candidate candidate);

    /// Upload manifest to the pre-defined S3 location
    ss::future<cloud_storage::upload_result> upload_manifest();

    /// Launch the upload loop fiber.
    ss::future<> upload_loop();

    /// Launch the sync manifest loop fiber.
    ss::future<> sync_manifest_loop();

    bool upload_loop_can_continue() const;
    bool sync_manifest_loop_can_continue() const;

    ntp_level_probe _probe;
    model::ntp _ntp;
    model::initial_revision_id _rev;
    cluster::partition_manager& _partition_manager;
    cloud_storage::remote& _remote;
    ss::lw_shared_ptr<cluster::partition> _partition;
    model::term_id _start_term;
    archival_policy _policy;
    s3::bucket_name _bucket;
    /// Remote manifest contains representation of the data stored in S3 (it
    /// gets uploaded to the remote location)
    cloud_storage::partition_manifest _manifest;
    ss::gate _gate;
    ss::abort_source _as;
    retry_chain_node _rtcnode;
    retry_chain_logger _rtclog;
    ss::lowres_clock::duration _cloud_storage_initial_backoff;
    ss::lowres_clock::duration _segment_upload_timeout;
    ss::lowres_clock::duration _manifest_upload_timeout;
    ss::semaphore _mutex{1};
    ss::lowres_clock::duration _upload_loop_initial_backoff;
    ss::lowres_clock::duration _upload_loop_max_backoff;
    config::binding<std::chrono::milliseconds> _sync_manifest_timeout;
    simple_time_jitter<ss::lowres_clock> _backoff_jitter{100ms};
    size_t _concurrency{4};
    ss::lowres_clock::time_point _last_upload_time;
    ss::scheduling_group _upload_sg;
    ss::io_priority_class _io_priority;
    bool _upload_loop_started = false;
    bool _upload_loop_stopped = false;

    bool _sync_manifest_loop_started = false;
    bool _sync_manifest_loop_stopped = false;
};

} // namespace archival
