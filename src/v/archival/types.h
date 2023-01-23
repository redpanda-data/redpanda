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

#include "cloud_storage/types.h"
#include "seastar/core/lowres_clock.hh"
#include "seastar/core/sstring.hh"
#include "seastar/util/bool_class.hh"
#include "seastarx.h"
#include "utils/named_type.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/io_priority_class.hh>
#include <seastar/core/scheduling.hh>

#include <filesystem>

namespace archival {

using cloud_storage::connection_limit;
using cloud_storage::local_segment_path;
using cloud_storage::remote_manifest_path;
using cloud_storage::remote_segment_path;
using cloud_storage::segment_name;

using service_metrics_disabled
  = ss::bool_class<struct service_metrics_disabled_tag>;
using per_ntp_metrics_disabled
  = ss::bool_class<struct per_ntp_metrics_disabled_tag>;

using segment_time_limit
  = named_type<ss::lowres_clock::duration, struct segment_time_limit_tag>;

/// Archiver service configuration
struct configuration {
    /// Bucket used to store all archived data
    cloud_storage_clients::bucket_name bucket_name;
    /// Initial backoff for requests to cloud storage
    ss::lowres_clock::duration cloud_storage_initial_backoff;
    /// Long upload timeout
    ss::lowres_clock::duration segment_upload_timeout;
    /// Shor upload timeout
    ss::lowres_clock::duration manifest_upload_timeout;
    /// Initial backoff for upload loop in case there is nothing to upload
    ss::lowres_clock::duration upload_loop_initial_backoff;
    /// Max backoff for upload loop in case there is nothing to upload
    ss::lowres_clock::duration upload_loop_max_backoff;
    /// Flag that indicates that service level metrics are disabled
    service_metrics_disabled svc_metrics_disabled;
    /// Flag that indicates that ntp-archiver level metrics are disabled
    per_ntp_metrics_disabled ntp_metrics_disabled;
    /// Upload time limit (if segment is not uploaded this amount of time the
    /// upload is triggered)
    std::optional<segment_time_limit> time_limit;
    /// Scheduling group that throttles archival upload
    ss::scheduling_group upload_scheduling_group{
      ss::default_scheduling_group()};
    /// I/o priority used to throttle file reads
    ss::io_priority_class upload_io_priority{ss::default_priority_class()};

    friend std::ostream& operator<<(std::ostream& o, const configuration& cfg);
};

/// \brief create scheduler service config
/// This mehtod will use shard-local redpanda configuration
/// to generate the configuration.
/// \param sg is a scheduling group used to run all uploads
/// \param p is an io priority class used to throttle upload file reads
archival::configuration get_archival_service_config(
  ss::scheduling_group sg = ss::default_scheduling_group(),
  ss::io_priority_class p = ss::default_priority_class());

/// The housekeeping job that performs the long
/// task incrementally. It can be paused and resumed.
/// When the underlying partition is stopped or the
/// entire shard is stopped the job is 'completed'
/// by calling 'complete' method.
class housekeeping_job {
public:
    housekeeping_job() = default;
    virtual ~housekeeping_job() = default;
    housekeeping_job(const housekeeping_job&) = delete;
    housekeeping_job(housekeeping_job&&) = delete;
    housekeeping_job& operator=(const housekeeping_job&) = delete;
    housekeeping_job& operator=(housekeeping_job&&) = delete;

    /// Start the job. The job can be paused (not immediately).
    virtual ss::future<> run(retry_chain_node&) = 0;
    /// Stop the job. The job object can't be reused after that.
    /// Subsequent calls to 'run' method should fail.
    virtual void interrupt() = 0;

    /// Return true if the jb was interrupted.
    /// The job can't be executed if 'interrupted() == true'
    virtual bool interrupted() const = 0;

private:
    friend class housekeeping_workflow;
    intrusive_list_hook _hook{};
};

/// Represents a series of adjacent segments
/// The object is used to compute a possible reupload
/// candidate. The series of segment is supposed to be
/// merged and reuploaded. The object produces metadata
/// for the reuploaded segment.
struct adjacent_segment_run {
    explicit adjacent_segment_run(model::ntp ntp)
      : ntp(std::move(ntp)) {}

    model::ntp ntp;
    cloud_storage::segment_meta meta{};
    size_t num_segments{0};
    std::vector<cloud_storage::remote_segment_path> segments;

    /// Try to add segment to the run
    ///
    /// The subsequent calls are successful until the total size
    /// of the run is below the threshold. The object keeps track
    /// of all segment names.
    ///
    /// \return true if the segment is added, false otherwise
    bool
    maybe_add_segment(const cloud_storage::segment_meta& s, size_t max_size);
};

std::ostream& operator<<(std::ostream& o, const adjacent_segment_run& run);

} // namespace archival
