/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "http/probe.h"
#include "model/fundamental.h"
#include "net/types.h"
#include "s3/error.h"

#include <seastar/core/metrics_registration.hh>

#include <cstdint>

namespace s3 {

/// \brief S3 client probe
///
/// \note The goal of this is to measure billable traffic
///       and performance related metrics. Since S3 client
///       reuses the net::base_transport it can potentially
///       use its probe. But this doesn't make much sence
///       since S3 requires different approach in measuring.
///       For instance, all connectoins are made to the same
///       endpoint and we don't want to look at every individual
///       http connection here because they can be transient.
///       Approach that RPC package uses will lead to cardinality
///       inflation in prometheous creating a lot of small
///       time-series.
class client_probe : public http::client_probe {
public:
    /// \brief Probe c-tor
    ///
    /// \param disable is used to switch the monitoring off
    /// \param region is a cloud provider region
    /// \param endpoint is a cloud provider endpoint
    client_probe(
      net::metrics_disabled disable, ss::sstring region, ss::sstring endpoint);

    /// Register S3 rpc error
    void register_failure(s3_error_code err);

private:
    /// Total number of rpc errors
    uint64_t _total_rpc_errors;
    /// Total number of SlowDown responses
    uint64_t _total_slowdowns;
    /// Total number of NoSuchKey responses
    uint64_t _total_nosuchkeys;
    ss::metrics::metric_groups _metrics;
};

} // namespace s3
