/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_roles/types.h"
#include "cloud_storage_clients/abs_error.h"
#include "cloud_storage_clients/s3_error.h"
#include "cloud_storage_clients/types.h"
#include "http/probe.h"
#include "metrics/metrics.h"
#include "model/fundamental.h"
#include "net/types.h"
#include "utils/log_hist.h"

#include <seastar/core/metrics_registration.hh>
#include <seastar/core/metrics_types.hh>

#include <chrono>
#include <cstdint>
#include <span>

namespace cloud_storage_clients {

enum class op_type_tag { upload, download };

/// \brief Cloud storage client probe
///
/// \note The goal of this is to measure billable traffic
///       (not yet implemented on the cloud side)
///       and performance related metrics. Since clients
///       reuse net::base_transport, they could potentially
///       use its probe. But this doesn't make much sence
///       since cloud storage clients require different approaches
///       to measuring. For instance, all connections are made to the same
///       endpoint and we don't want to look at every individual
///       http connection here because they can be transient.
///       Approach that RPC package uses will lead to cardinality
///       inflation in prometheous creating a lot of small
///       time-series.
class client_probe : public http::client_probe {
public:
    using hist_t = log_hist_internal;

    /// \brief Probe c-tor for S3 client
    ///
    /// \param disable is used to switch the internal monitoring off
    /// \param public_disable is used to switch the public monitoring off
    /// \param region is the AWS region
    /// \param endpoint is the AWS S3 endpoint
    client_probe(
      net::metrics_disabled disable,
      net::public_metrics_disabled public_disable,
      cloud_roles::aws_region_name region,
      endpoint_url endpoint);

    /// \brief Probe c-tor for ABS client
    ///
    /// \param disable is used to switch the internal monitoring off
    /// \param public_disable is used to switch the public monitoring off
    /// \param storage_account_name
    /// \param endpoint is the ABS endpoint
    client_probe(
      net::metrics_disabled disable,
      net::public_metrics_disabled public_disable,
      cloud_roles::storage_account storage_account_name,
      endpoint_url endpoint);

    /// Register S3 rpc error
    void register_failure(
      s3_error_code err, std::optional<op_type_tag> op_type = std::nullopt);
    /// Register ABS rpc error
    void register_failure(abs_error_code err);
    void register_retryable_failure(std::optional<op_type_tag> op_type);

    /// Call on a shard which needs to borrow a client
    void register_borrow();
    /// Register total lease duration
    std::unique_ptr<hist_t::measurement> register_lease_duration();
    /// Utilization metric which is used to decide if borrowing is possible
    void register_utilization(unsigned clients_in_use);

private:
    struct raw_label {
        ss::sstring key;
        ss::sstring value;
    };

    void setup_internal_metrics(
      net::metrics_disabled disable, std::span<raw_label> raw_labels);
    void setup_public_metrics(
      net::public_metrics_disabled disable, std::span<raw_label> raw_labels);

    /// Total number of rpc errors
    uint64_t _total_rpc_errors{0};
    /// Total number of SlowDown responses
    uint64_t _total_slowdowns{0};
    /// Total number of NoSuchKey responses
    uint64_t _total_nosuchkeys{0};
    /// Total number of NoSuchKey responses
    uint64_t _total_upload_slowdowns{0};
    /// Total number of NoSuchKey responses
    uint64_t _total_download_slowdowns{0};
    /// Number of times this shard borrowed resources from other shards
    uint64_t _total_borrows{0};
    /// Total time the lease is held by the ntp_archiver (or another user)
    hist_t _lease_duration;
    /// Current utilization of the client pool
    uint64_t _pool_utilization;
    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;
};

} // namespace cloud_storage_clients
