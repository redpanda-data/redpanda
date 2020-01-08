#pragma once
#include "rpc/logger.h"

#include <seastar/core/metrics_registration.hh>
#include <seastar/net/socket_defs.hh>

#include <iostream>

namespace rpc {
class client_probe {
public:
    void request_sent() {
        ++_requests_sent;
        ++_requests_pending;
    }

    void request_completed() {
        ++_requests_completed;
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
        rpclog.trace("Connection error: {}", e);
        ++_connection_errors;
    }

    void read_dispatch_error(const std::exception_ptr& e) {
        rpclog.error("Error dispatching client reads: {}", e);
        ++_read_dispatch_errors;
    }

    void header_corrupted() {
        ++_corrupted_headers;
        request_error();
    }

    void client_correlation_error() { ++_client_correlation_errors; }

    void server_correlation_error() { ++_server_correlation_errors; }

    void waiting_for_available_memory() { ++_requests_blocked_memory; }

    void setup_metrics(
      metrics::metric_groups& mgs,
      const std::optional<sstring>& service_name,
      const socket_address& target_addr);

private:
    uint64_t _requests_sent = 0;
    uint32_t _requests_pending = 0;
    uint32_t _request_errors = 0;
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
    metrics::metric_groups _metrics;

    friend std::ostream&
    operator<<(std::ostream& o, const rpc::client_probe& p);
};
std::ostream& operator<<(std::ostream& o, const rpc::client_probe& p);
}; // namespace rpc
