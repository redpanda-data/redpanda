/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "model/fundamental.h"
#include "ssx/metrics.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_registration.hh>

#include <cstdint>
namespace raft {
class probe {
public:
    void vote_request() { ++_vote_requests; }
    void append_request() { ++_append_requests; }

    void vote_request_sent() { ++_vote_requests_sent; }

    void replicate_requests_ack_all() { ++_replicate_requests_ack_all; }
    void replicate_requests_ack_leader() { ++_replicate_requests_ack_leader; }
    void replicate_requests_ack_none() { ++_replicate_requests_ack_none; }
    void replicate_done() { ++_replicate_requests_done; }

    void log_truncated() { ++_log_truncations; }
    void log_flushed() { ++_log_flushes; }

    void replicate_batch_flushed() { ++_replicate_batch_flushed; }
    void recovery_append_request() { ++_recovery_requests; }
    void configuration_update() { ++_configuration_updates; }

    void leadership_changed() { ++_leadership_changes; }

    static std::vector<ss::metrics::label_instance>
    create_metric_labels(const model::ntp& ntp);

    void setup_metrics(const model::ntp& ntp);
    void setup_public_metrics(const model::ntp& ntp);

    void heartbeat_request_error() { ++_heartbeat_request_error; };
    void replicate_request_error() { ++_replicate_request_error; };
    void recovery_request_error() { ++_recovery_request_error; };

private:
    uint64_t _vote_requests = 0;
    uint64_t _append_requests = 0;
    uint64_t _vote_requests_sent = 0;
    uint64_t _replicate_requests_ack_all = 0;
    uint64_t _replicate_requests_ack_leader = 0;
    uint64_t _replicate_requests_ack_none = 0;
    uint64_t _replicate_requests_done = 0;
    uint64_t _log_flushes = 0;
    uint64_t _replicate_batch_flushed = 0;
    uint32_t _log_truncations = 0;
    uint32_t _configuration_updates = 0;
    uint64_t _recovery_requests = 0;
    uint64_t _leadership_changes = 0;
    uint64_t _heartbeat_request_error = 0;
    uint64_t _replicate_request_error = 0;
    uint64_t _recovery_request_error = 0;

    ss::metrics::metric_groups _metrics;
    ss::metrics::metric_groups _public_metrics{
      ssx::metrics::public_metrics_handle};
};
} // namespace raft
