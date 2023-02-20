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
#include "model/metadata.h"
#include "net/unresolved_address.h"
#include "rpc/logger.h"
#include "rpc/types.h"

#include <seastar/core/metrics_registration.hh>

#include <iosfwd>

namespace net {
class client_probe {
public:
    void request() {
        ++_requests;
        ++_requests_pending;
    }

    void request_completed() {
        ++_requests_completed;
        --_requests_pending;
    }

    void request_timeout() {
        ++_request_timeouts;
        --_requests_pending;
    }

    void request_error() {
        ++_request_errors;
        --_requests_pending;
    }

    void add_bytes_sent(size_t sent) { _out_bytes += sent; }

    void add_bytes_received(size_t recv) { _in_bytes += recv; }

    void connection_established() {
        ++_connects;
        ++_connections;
    }

    void connection_closed() { --_connections; }

    void connection_error(const std::exception_ptr& e) {
        rpc::rpclog.trace("Connection error: {}", e);
        ++_connection_errors;
    }

    void read_dispatch_error() { ++_read_dispatch_errors; }

    void header_corrupted() { ++_corrupted_headers; }

    void client_correlation_error() { ++_client_correlation_errors; }

    void server_correlation_error() { ++_server_correlation_errors; }

    void waiting_for_available_memory() { ++_requests_blocked_memory; }

    void setup_metrics(
      ss::metrics::metric_groups& mgs,
      const std::optional<rpc::connection_cache_label>& label,
      const std::optional<model::node_id>& node_id,
      const net::unresolved_address& target_addr);

private:
    uint64_t _requests = 0;
    uint32_t _requests_pending = 0;
    uint32_t _request_errors = 0;
    uint64_t _request_timeouts = 0;
    uint64_t _requests_completed = 0;
    uint64_t _in_bytes = 0;
    uint64_t _out_bytes = 0;
    uint64_t _connects = 0;
    uint32_t _connections = 0;
    uint32_t _connection_errors = 0;
    uint32_t _read_dispatch_errors = 0;
    uint32_t _corrupted_headers = 0;
    uint32_t _server_correlation_errors = 0;
    uint32_t _client_correlation_errors = 0;
    uint32_t _requests_blocked_memory = 0;
    ss::metrics::metric_groups _metrics;

    friend std::ostream& operator<<(std::ostream& o, const client_probe& p);
};
}; // namespace net
