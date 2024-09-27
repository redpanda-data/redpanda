/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/outcome.h"
#include "model/fundamental.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>

#include <functional>

namespace ss = seastar;

namespace archival {

template<class Clock>
struct upload_resource_usage {
    model::ntp ntp;
    // Number of PUT requests used
    size_t put_requests_used{0};
    // Number of bytes uploaded
    size_t uploaded_bytes{0};
    // Set if error occurred (other fields are ignored in this case)
    std::optional<std::error_code> errc;
    // Used for logging and cancellation
    std::reference_wrapper<basic_retry_chain_node<Clock>> archiver_rtc;
    // Time of the next housekeeping
    std::optional<typename Clock::time_point> next_housekeeping;
    // Time of the next manifest upload
    std::optional<typename Clock::time_point> next_manifest_upload;

    bool operator==(const upload_resource_usage& o) const noexcept {
        return ntp == o.ntp && put_requests_used == o.put_requests_used
               && uploaded_bytes == o.uploaded_bytes && errc == o.errc
               && archiver_rtc.get().same_root(o.archiver_rtc.get());
    }
};

/// Scheduler can pass the amount of resources
/// the ntp_archiver should use to avoid being throttled.
struct upload_resource_quota {
    /// Number of requests that archiver can use
    std::optional<size_t> requests_quota;
    /// Number of bytes that archiver can upload
    std::optional<size_t> upload_size_quota;

    bool operator==(const upload_resource_quota&) const noexcept = default;
};

/// The object controls backoff in situation when there is no
/// new data to upload or the error have occurred. It can also apply
/// throttling so one ntp_archiver wouldn't be able to use too many
/// requests.
/// It also acts as a scheduler that organizes concurrent execution
/// of different ntp_archiver instances
template<class Clock = ss::lowres_clock>
class archiver_scheduler_api {
public:
    archiver_scheduler_api() = default;
    archiver_scheduler_api(const archiver_scheduler_api&) = delete;
    archiver_scheduler_api(archiver_scheduler_api&&) noexcept = delete;
    archiver_scheduler_api& operator=(const archiver_scheduler_api&) = delete;
    archiver_scheduler_api& operator=(archiver_scheduler_api&&) noexcept
      = delete;
    virtual ~archiver_scheduler_api() {};

    /// Applies throttling or backoff to uploads
    ///
    /// \param arg describes resources used by the partition
    /// \returns error or the hint for the workflow
    virtual ss::future<result<upload_resource_quota>>
    maybe_suspend_upload(upload_resource_usage<Clock> arg) noexcept = 0;

    /// Start managing state for particular NTP
    virtual ss::future<> create_ntp_state(model::ntp) = 0;

    /// Stop managing state for particular NTP
    virtual ss::future<> dispose_ntp_state(model::ntp) = 0;

    /// Start scheduler
    virtual ss::future<> start() = 0;

    /// Stop scheduler
    virtual ss::future<> stop() = 0;
};

template<class Clock>
std::ostream&
operator<<(std::ostream& o, const upload_resource_usage<Clock>& s) {
    fmt::print(
      o,
      "upload_resource_usage(ntp={}, put_requests_used={}, uploaded_bytes={}, "
      "errc={})",
      s.ntp,
      s.put_requests_used,
      s.uploaded_bytes,
      s.errc);
    return o;
}

std::ostream& operator<<(std::ostream& o, const upload_resource_quota& t);

} // namespace archival
