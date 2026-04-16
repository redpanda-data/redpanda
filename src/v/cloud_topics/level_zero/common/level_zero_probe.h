/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cloud_topics/level_zero/common/micro_probe.h"
#include "metrics/metrics.h"
#include "utils/hdr_hist.h"
#include "utils/log_hist.h"

#include <seastar/core/metrics_registration.hh>
#include <seastar/core/metrics_types.hh>
#include <seastar/util/defer.hh>

#include <chrono>
#include <cstddef>
#include <cstdint>

namespace cloud_topics::l0 {

/// \brief Pipeline probe
///
/// \details This class is used to collect metrics for the level_zero pipeline.
///          It should work for both read and write pipeline. It tracks basic
///          metrics like number of requests, number of errors and processing
///          time. Also, it tracks total memory usage of the pipeline and
///          memory pressure events.
class pipeline_probe {
public:
    /// \brief Probe c-tor
    ///
    /// \param disable is used to switch the internal monitoring off
    /// \param public_disable is used to switch the public monitoring off
    pipeline_probe(std::string_view name, bool disable, bool public_disable);

    void register_request() { ++_requests_in; }
    void register_request_completed() { ++_requests_completed; }
    void register_request_error() { ++_requests_error; }
    void register_request_timeout() { ++_requests_timeout; }
    auto register_request_processing_time() {
        return _request_processing_time.auto_measure();
    }
    void set_memory_usage_gauge(uint64_t mem) { _current_memory_usage = mem; }
    auto register_memory_pressure_blocked(uint64_t mem) {
        _memory_pressure_waits += 1;
        _memory_pressure_blocked += mem;
        return ss::defer([this, mem] { _memory_pressure_blocked -= mem; });
    }
    void register_request_limit_blocked() { ++_request_limit_waits; }
    void register_bytes_in(uint64_t bytes) {
        _total_bytes_in += bytes;
        _request_memory_histogram.record(bytes);
    }
    void register_bytes_out(uint64_t bytes) { _total_bytes_out += bytes; }

    void register_micro_probe(const micro_probe& p) { _request_probe += p; }

private:
    void setup_internal_metrics(bool disable, ss::sstring name);
    void setup_public_metrics(bool disable, ss::sstring name);

    // requests in
    uint64_t _requests_in{0};
    // requests completed successfully
    uint64_t _requests_completed{0};
    // requests completed with error
    uint64_t _requests_error{0};
    // requests timeout errors (requests that spent too long in the pipeline)
    uint64_t _requests_timeout{0};
    // request processing time histogram
    log_hist_client_quota _request_processing_time;
    // current memory usage (gauge)
    uint64_t _current_memory_usage{0};
    // memory pressure (req. waits for semaphore)
    uint64_t _memory_pressure_waits{0};
    // memory pressure (memory blocked by waiting for semaphore)
    uint64_t _memory_pressure_blocked{0};
    // request count limit pressure (req. waits for inflight slot)
    uint64_t _request_limit_waits{0};
    // total bytes in (for write pipeline)
    uint64_t _total_bytes_in{0};
    // total bytes out (for read pipeline)
    uint64_t _total_bytes_out{0};
    // request memory histogram (distribution of memory sizes used by requests)
    batch_size_hist _request_memory_histogram;

    micro_probe _request_probe{};

    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;
};

class write_request_scheduler_probe {
public:
    explicit write_request_scheduler_probe(bool disable);

    void register_request(size_t bytes) {
        _scheduler_requests += 1;
        _scheduler_bytes += bytes;
    }

    void register_send_xshard(size_t bytes) {
        _tx_requests_xshard += 1;
        _tx_bytes_xshard += bytes;
    }

    void register_receive_xshard(size_t bytes) {
        _rx_requests_xshard += 1;
        _rx_bytes_xshard += bytes;
    }

    void set_active_groups(uint64_t count) { _active_groups = count; }

    void set_next_stage_bytes(uint64_t bytes) { _next_stage_bytes = bytes; }

private:
    void setup_internal_metrics(bool disable);

    /// Number of write requests and total bytes scheduled
    uint64_t _scheduler_requests{0};
    uint64_t _scheduler_bytes{0};
    /// Number of requests and total bytes proxied to another shard
    uint64_t _tx_requests_xshard{0};
    uint64_t _tx_bytes_xshard{0};
    /// Number of requests and total bytes received from another shard
    uint64_t _rx_requests_xshard{0};
    uint64_t _rx_bytes_xshard{0};
    /// Number of active upload groups
    uint64_t _active_groups{0};
    /// Bytes buffered in the next pipeline stage
    uint64_t _next_stage_bytes{0};

    metrics::internal_metric_groups _metrics;
};

class read_merge_probe {
public:
    explicit read_merge_probe(bool disable);

    void register_request_in(uint64_t bytes) {
        _requests_in += 1;
        _bytes_in += bytes;
    }

    void register_request_out(uint64_t bytes) {
        _requests_out += 1;
        _bytes_out += bytes;
    }

private:
    void setup_internal_metrics(bool disable);

    /// Number of requests handled by the component (all requests)
    uint64_t _requests_in{0};
    /// Total bytes of all requests handled
    uint64_t _bytes_in{0};
    /// Number of proxy requests forwarded to the next stage
    uint64_t _requests_out{0};
    /// Total bytes of proxy requests forwarded
    uint64_t _bytes_out{0};

    metrics::internal_metric_groups _metrics;
};

class batcher_probe {
public:
    explicit batcher_probe(bool disable);

    void register_upload(uint64_t size_bytes) {
        _objects_uploaded += 1;
        _bytes_uploaded += size_bytes;
        _upload_size_hist.record(size_bytes);
    }

    void register_error() { ++_upload_errors; }

    void register_epoch_error() { ++_epoch_errors; }

private:
    void setup_internal_metrics(bool disable);

    uint64_t _objects_uploaded{0};
    uint64_t _bytes_uploaded{0};
    uint64_t _upload_errors{0};
    uint64_t _epoch_errors{0};
    hdr_hist _upload_size_hist;

    metrics::internal_metric_groups _metrics;
};

} // namespace cloud_topics::l0
