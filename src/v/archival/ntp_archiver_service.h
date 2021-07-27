/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once
#include "archival/archival_policy.h"
#include "archival/probe.h"
#include "archival/types.h"
#include "cloud_storage/manifest.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/types.h"
#include "cluster/partition.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "s3/client.h"
#include "storage/fwd.h"
#include "storage/segment.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>

#include <functional>
#include <map>

namespace archival {

using namespace std::chrono_literals;

/// Archiver service configuration
struct configuration {
    /// S3 configuration
    s3::configuration client_config;
    /// Bucket used to store all archived data
    s3::bucket_name bucket_name;
    /// Time interval to run uploads & deletes
    ss::lowres_clock::duration interval;
    /// Number of simultaneous S3 uploads
    s3_connection_limit connection_limit;
    /// Initial backoff for uploads
    ss::lowres_clock::duration initial_backoff;
    /// Long upload timeout
    ss::lowres_clock::duration segment_upload_timeout;
    /// Shor upload timeout
    ss::lowres_clock::duration manifest_upload_timeout;
    /// Flag that indicates that service level metrics are disabled
    service_metrics_disabled svc_metrics_disabled;
    /// Flag that indicates that ntp-archiver level metrics are disabled
    per_ntp_metrics_disabled ntp_metrics_disabled;
    /// Upload time limit (if segment is not uploaded this amount of time the
    /// upload is triggered)
    std::optional<segment_time_limit> time_limit;
};

std::ostream& operator<<(std::ostream& o, const configuration& cfg);

/// This class performs per-ntp arhcival workload. Every ntp can be
/// processed independently, without the knowledge about others. All
/// 'ntp_archiver' instances that the shard posesses are supposed to be
/// aggregated on a higher level in the 'archiver_service'.
///
/// The 'ntp_archiver' is responsible for manifest manitpulations and
/// generation of per-ntp candidate set. The actual file uploads are
/// handled by 'archiver_service'.
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
      const configuration& conf,
      cloud_storage::remote& remote,
      ss::lw_shared_ptr<cluster::partition> part,
      service_probe& svc_probe);

    /// Stop archiver.
    ///
    /// \return future that will become ready when all async operation will be
    /// completed
    ss::future<> stop();

    /// Get NTP
    const model::ntp& get_ntp() const;

    /// Get revision id
    model::revision_id get_revision_id() const;

    /// Get timestamp
    const ss::lowres_clock::time_point get_last_upload_time() const;

    /// Download manifest from pre-defined S3 locatnewion
    ///
    /// \return future that returns true if the manifest was found in S3
    ss::future<cloud_storage::download_result>
    download_manifest(retry_chain_node& parent);

    /// Upload manifest to the pre-defined S3 location
    ss::future<cloud_storage::upload_result>
    upload_manifest(retry_chain_node& parent);

    const cloud_storage::manifest& get_remote_manifest() const;

    struct batch_result {
        size_t num_succeded;
        size_t num_failed;
    };

    /// \brief Upload next set of segments to S3 (if any)
    /// The semaphore is used to track number of parallel uploads. The method
    /// will pick not more than '_concurrency' candidates and start
    /// uploading them.
    ///
    /// \param lm is a log manager instance
    /// \param parent is a retry chain node of the caller
    /// \param lso_override last stable offset override
    /// \return future that returns number of uploaded/failed segments
    ss::future<batch_result> upload_next_candidates(
      storage::log_manager& lm,
      retry_chain_node& parent,
      std::optional<model::offset> last_stable_offset_override = std::nullopt);

private:
    /// Information about started upload
    struct scheduled_upload {
        /// The future that will be ready when the segment will be fully
        /// uploaded
        std::optional<ss::future<cloud_storage::upload_result>> result;
        /// Last offset of the uploaded segment or part
        model::offset inclusive_last_offset;
        /// Segment metadata
        std::optional<cloud_storage::manifest::segment_meta> meta;
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
      storage::log_manager& lm,
      model::offset last_uploaded_offset,
      model::offset last_stable_offset,
      retry_chain_node& parent);

    /// Start all uploads
    ss::future<std::vector<scheduled_upload>> schedule_uploads(
      storage::log_manager& lm,
      model::offset last_stable_offset,
      retry_chain_node& parent);

    /// Wait until all scheduled uploads will be completed
    ///
    /// Update the probe and manifest
    ss::future<ntp_archiver::batch_result> wait_all_scheduled_uploads(
      std::vector<ntp_archiver::scheduled_upload> scheduled,
      retry_chain_node& parent);

    /// Upload individual segment to S3.
    ///
    /// \return true on success and false otherwise
    ss::future<cloud_storage::upload_result>
    upload_segment(upload_candidate candidate, retry_chain_node& fib);

    service_probe& _svc_probe;
    ntp_level_probe _probe;
    model::ntp _ntp;
    model::revision_id _rev;
    cloud_storage::remote& _remote;
    ss::lw_shared_ptr<cluster::partition> _partition;
    archival_policy _policy;
    s3::bucket_name _bucket;
    /// Remote manifest contains representation of the data stored in S3 (it
    /// gets uploaded to the remote location)
    cloud_storage::manifest _manifest;
    ss::gate _gate;
    ss::abort_source _as;
    ss::semaphore _mutex{1};
    simple_time_jitter<ss::lowres_clock> _backoff{100ms};
    size_t _concurrency{4};
    ss::lowres_clock::time_point _last_upload_time;
    ss::lowres_clock::duration _initial_backoff;
    ss::lowres_clock::duration _segment_upload_timeout;
    ss::lowres_clock::duration _manifest_upload_timeout;
};

} // namespace archival
