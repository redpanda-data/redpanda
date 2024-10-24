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

#include "base/seastarx.h"
#include "cloud_storage/types.h"
#include "cluster/archival/adjacent_segment_run.h"
#include "cluster/archival/run_quota.h"
#include "seastar/core/lowres_clock.hh"
#include "seastar/core/sstring.hh"
#include "seastar/util/bool_class.hh"
#include "utils/named_type.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/io_priority_class.hh>
#include <seastar/core/scheduling.hh>

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
    config::binding<std::chrono::milliseconds> cloud_storage_initial_backoff;
    /// Long upload timeout
    config::binding<std::chrono::milliseconds> segment_upload_timeout;
    /// Shor upload timeout
    config::binding<std::chrono::milliseconds> manifest_upload_timeout;
    /// Timeout for running delete operations during the GC phase
    config::binding<std::chrono::milliseconds> garbage_collect_timeout;
    /// Initial backoff for upload loop in case there is nothing to upload
    config::binding<std::chrono::milliseconds> upload_loop_initial_backoff;
    /// Max backoff for upload loop in case there is nothing to upload
    config::binding<std::chrono::milliseconds> upload_loop_max_backoff;
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

    enum class run_status {
        ok,
        skipped,
        failed,
    };

    struct run_result {
        run_status status{run_status::ok};
        run_quota_t consumed{run_quota_t::min()};
        run_quota_t remaining{run_quota_t::min()};
        uint32_t local_reuploads{0};
        uint32_t cloud_reuploads{0};
        uint32_t deletions{0};
        uint32_t manifest_uploads{0};
        uint32_t metadata_syncs{0};
    };

    /// Stop the job. The job object can't be reused after that.
    /// Subsequent calls to 'run' method should fail.
    virtual void interrupt() = 0;

    /// Wait until the job finishes. After the future is ready
    /// the job is no longer usable. This is supposed to be called
    /// by the owner of the job.
    virtual ss::future<> stop() = 0;

    /// Enable or disable the job. The job is responsible for implementing
    /// this. The 'run' method of the disabled job is supposed to return
    /// 'run_status::skipped'.
    virtual void set_enabled(bool) = 0;

    //
    // Method which are used by the housekeeping service
    //

    /// The housekeeping service invokes this method when the job
    /// is first added. The 'stop' method is supposed to be blocked
    /// until the job is acquired and not released yet. It can only be called
    /// once.
    virtual void acquire() = 0;

    /// Release the housekeeping job. After calling this method
    /// the housekeeping is guaranteed to no longer touch the job.
    /// The 'stop' method of the job will progress only after 'release' is
    /// called. The method can only be called once.
    virtual void release() = 0;

    /// Return true if the jb was interrupted.
    /// The job can't be executed if 'interrupted() == true'
    virtual bool interrupted() const = 0;

    /// Start the job. The job can be paused (not immediately).
    ///
    /// \param quota is number of actions job can execute during current run
    ///        the job is not forced to use its entire quota. It's also possible
    ///        to use more resuorces than the job was given.
    /// \return a future that will become available when the job is completed.
    ///         The result of the future contains stats for the current run (
    ///         number of uploaded segments/manifests, etc).
    virtual ss::future<run_result> run(run_quota_t quota) = 0;

    /// Returns the the root retry chain node of the job.
    virtual retry_chain_node* get_root_retry_chain_node() = 0;

    virtual ss::sstring name() const = 0;

private:
    friend class housekeeping_workflow;
    intrusive_list_hook _hook{};
};

/// Number of segment reuploads the job can do per housekeeping run
inline constexpr int max_reuploads_per_run = 4;

enum class error_outcome {
    unexpected_failure = 1,
    timed_out,
    out_of_range,
    offset_not_found,
    scan_failed,
    shutting_down,
    not_enough_data,
};

struct error_outcome_category final : public std::error_category {
    const char* name() const noexcept final {
        return "archival::error_outcome";
    }

    std::string message(int c) const final {
        switch (static_cast<error_outcome>(c)) {
        case error_outcome::unexpected_failure:
            return "archival_unexpected_failure";
        case error_outcome::timed_out:
            return "archival_timed_out";
        case error_outcome::out_of_range:
            return "archival_out_of_range";
        case error_outcome::offset_not_found:
            return "archival_offset_not_found";
        case error_outcome::scan_failed:
            return "archival_scan_failed";
        case error_outcome::shutting_down:
            return "archival_shutting_down";
        case error_outcome::not_enough_data:
            return "archival_not_enough_data";
        default:
            return "archival_unknown_error";
        }
    }
};

inline const std::error_category& error_category() noexcept {
    static error_outcome_category e;
    return e;
}

inline std::error_code make_error_code(error_outcome e) noexcept {
    return {static_cast<int>(e), error_category()};
}

} // namespace archival

namespace std {
template<>
struct is_error_code_enum<archival::error_outcome> : true_type {};
} // namespace std
