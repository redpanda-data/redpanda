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
#include "archival/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "s3/client.h"
#include "storage/fwd.h"
#include "storage/segment.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/semaphore.hh>

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
    /// Time interval to run GC
    ss::lowres_clock::duration gc_interval;
    /// Number of simultaneous S3 uploads
    s3_connection_limit connection_limit;
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
    /// \param bucket is an S3 bucket that should be used to store the data
    ntp_archiver(const storage::ntp_config& ntp, const configuration& conf);

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
    ss::future<download_manifest_result> download_manifest();

    /// Upload manifest to the pre-defined S3 location
    ss::future<> upload_manifest();

    const manifest& get_remote_manifest() const;

    struct batch_result {
        size_t num_succeded;
        size_t num_failed;
    };

    /// \brief Upload next set of segments to S3 (if any)
    /// The semaphore is used to track number of parallel uploads. The method
    /// will pick not more than '_concurrency' candidates and start
    /// uploading them.
    ///
    /// \param req_limit is used to limit number of parallel uploads
    /// \param lm is a log manager instance
    /// \return future that returns number of uploaded/failed segments
    ss::future<batch_result>
    upload_next_candidates(ss::semaphore& req_limit, storage::log_manager& lm);

private:
    /// Upload individual segment to S3.
    ///
    /// \return true on success and false otherwise
    ss::future<bool>
    upload_segment(ss::semaphore& req_limit, upload_candidate candidate);

    model::ntp _ntp;
    model::revision_id _rev;
    s3::configuration _client_conf;
    archival_policy _policy;
    s3::bucket_name _bucket;
    /// Remote manifest contains representation of the data stored in S3 (it
    /// gets uploaded to the remote location)
    manifest _remote;
    ss::gate _gate;
    ss::abort_source _as;
    ss::semaphore _mutex{1};
    simple_time_jitter<ss::lowres_clock> _backoff{100ms};
    size_t _concurrency{4};
    ss::lowres_clock::time_point _last_upload_time;
};

} // namespace archival
