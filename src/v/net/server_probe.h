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

#include "base/seastarx.h"
#include "metrics/metrics.h"

#include <seastar/core/metrics_registration.hh>

#include <iosfwd>

namespace net {

class server_probe {
public:
    server_probe() = default;
    server_probe(const server_probe&) = delete;
    server_probe& operator=(const server_probe&) = delete;
    server_probe(server_probe&&) = delete;
    server_probe& operator=(server_probe&&) = delete;
    ~server_probe() = default;

    void connection_established() {
        ++_connects;
        ++_connections;
    }

    void connection_closed() { --_connections; }

    void connection_close_error() { ++_connection_close_error; }

    void connection_rejected_open_limit() {
        ++_connections_rejected_open_limit;
    }

    void connection_rejected_rate_limit() {
        ++_connections_rejected_rate_limit;
    }

    void add_bytes_sent(size_t sent) { _out_bytes += sent; }

    void add_bytes_received(size_t recv) { _in_bytes += recv; }

    void request_received() { ++_requests_received; }

    void request_completed() { ++_requests_completed; }

    void header_corrupted() { ++_corrupted_headers; }

    void method_not_found() { ++_method_not_found_errors; }

    void service_error() { ++_service_errors; }

    void waiting_for_available_memory() { ++_requests_blocked_memory; }

    void waiting_for_conection_rate() { ++_connections_wait_rate; }

    // metric used to signal a produce request with a timestamp too far into the
    // future or too far in the past see configuration
    // log_message_timestamp_alert_after_ms and
    // log_message_timestamp_alert_before_ms
    void produce_bad_create_time() { _produce_bad_create_time++; }
    // for testing
    auto get_produce_bad_create_time() const {
        return _produce_bad_create_time;
    }

    void
    setup_metrics(metrics::internal_metric_groups& mgs, std::string_view proto);

    void setup_public_metrics(
      metrics::public_metric_groups& mgs, std::string_view proto);

private:
    uint64_t _requests_completed = 0;
    uint64_t _in_bytes = 0;
    uint64_t _out_bytes = 0;
    uint64_t _connects = 0;
    uint64_t _requests_received = 0;
    uint64_t _service_errors = 0;
    uint32_t _connections = 0;
    uint32_t _connection_close_error = 0;
    // connections rejected as we hit our "open connections" limit
    uint64_t _connections_rejected_open_limit = 0;
    // connections rejected as we hit our connection rate limit and
    // delaying the connection was not sufficient to stay under the limit
    uint32_t _connections_rejected_rate_limit = 0;
    uint32_t _corrupted_headers = 0;
    uint32_t _method_not_found_errors = 0;
    uint32_t _requests_blocked_memory = 0;
    uint32_t _connections_wait_rate = 0;
    uint32_t _produce_bad_create_time = 0;
    friend std::ostream& operator<<(std::ostream& o, const server_probe& p);
};

}; // namespace net
