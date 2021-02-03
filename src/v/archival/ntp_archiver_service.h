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
#include "archival/manifest.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "s3/client.h"
#include "storage/api.h"
#include "storage/log_manager.h"
#include "storage/ntp_config.h"
#include "storage/segment.h"
#include "storage/segment_set.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/lowres_clock.hh>

namespace archival {

/// Number of simultaneous connections to S3
using s3_connection_limit
  = named_type<size_t, struct archival_s3_connection_limit_t>;

/// Archiver service configuration
struct configuration {
    /// S3 configuration
    s3::configuration client_config;
    /// Bucket used to store all archived data
    s3::bucket_name bucket_name;
    /// Policy for choosing candidates for archiving
    upload_policy_selector upload_policy;
    /// Policy for choosing candidates for removing from S3
    delete_policy_selector delete_policy;
    /// Time interval to run uploads & deletes
    ss::lowres_clock::duration interval;
    /// Time interval to run GC
    ss::lowres_clock::duration gc_interval;
    /// Number of simultaneous S3 uploads
    s3_connection_limit connection_limit;
};

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
    /// \param bucket is an S3 bucket that should be used to store the data
    ntp_archiver(const storage::ntp_config& ntp, const configuration& conf);

    /// Stop archiver.
    ///
    /// \return future that will become ready when all async operation will be
    /// completed
    ss::future<> stop();

    /// Get NTP
    const model::ntp& get_ntp() const;

    /// Get timestamp
    const ss::lowres_clock::time_point get_last_upload_time() const;
    const ss::lowres_clock::time_point get_last_delete_time() const;

    /// Download manifest from pre-defined S3 locatnewion
    ///
    /// \return future that returns true if the manifest was found in S3
    ss::future<bool> download_manifest();

    /// Upload manifest to the pre-defined S3 location
    ss::future<> upload_manifest();

    const manifest& get_remote_manifest() const;

    struct batch_result {
        size_t succeded;
        size_t failed;
    };

    /// \brief Upload next segment to S3 (if any)
    /// The semaphore is used to track number of parallel uploads. The method
    /// will pick not more than 'req_limit.available_units()' candidates for
    /// upload and consme one unit for every candidate (non-blocking).
    ///
    /// \param req_limit is used to limit number of parallel uploads
    /// \param lm is a log manager instance
    /// \return future that returns number of uploaded/failed segments
    ss::future<batch_result>
    upload_next_candidate(ss::semaphore& req_limit, storage::log_manager& lm);

    /// \brief Delete next segment from S3
    ///
    /// \param req_limit is used to limit number of parallel operations
    /// \param lm is a log manager instance
    /// \return future that returns number of deleted/failed segments
    ss::future<batch_result>
    delete_next_candidate(ss::semaphore& req_limit, storage::log_manager& lm);

private:
    /// Get segment from log_manager instance by path
    ///
    /// \param path is a segment path (from the manifest)
    /// \param lm is a log manager instance
    /// \return pointer to segment instance or null
    ss::lw_shared_ptr<storage::segment>
    get_segment(segment_name path, storage::log_manager& lm);

    /// Upload individual segment to S3. Locate the segment in 'lm' and adjust
    /// 'req_limit' accordingly.
    ///
    /// \return true on success and false otherwise
    ss::future<bool> upload_segment(
      manifest::segment_map::value_type target, storage::log_manager& lm);

    /// Delete segment from S3. Locate the segment in 'lm' and adjust
    /// 'req_limit' accordingly.
    ///
    /// \return true on success and false otherwise
    ss::future<bool> delete_segment(
      manifest::segment_map::value_type target, storage::log_manager& lm);

    model::ntp _ntp;
    model::revision_id _rev;
    s3::configuration _client_conf;

    std::unique_ptr<archival_policy_base> _policy;
    s3::bucket_name _bucket;
    /// Remote manifest contains representation of the data stored in S3 (it
    /// gets uploaded to the remote location)
    manifest _remote;
    ss::gate _gate;
    ss::lowres_clock::time_point _last_upload_time;
    ss::lowres_clock::time_point _last_delete_time;
};

} // namespace archival
