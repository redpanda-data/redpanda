#pragma once
#include "seastarx.h"

#include <seastar/core/metrics_registration.hh>

#include <iostream>

namespace rpc {

class server_probe {
public:
    void connection_established() {
        ++_connects;
        ++_connections;
    }

    void connection_closed() { --_connections; }

    void connection_close_error() { ++_connection_close_error; }

    void add_bytes_sent(size_t sent) { _out_bytes += sent; }

    void add_bytes_received(size_t recv) { _in_bytes += recv; }

    void request_completed() { ++_requests_completed; }

    void header_corrupted() {
        ++_bad_requests;
        ++_corrupted_headers;
    }

    void method_not_found() {
        ++_bad_requests;
        ++_method_not_found_errors;
    }

    void waiting_for_available_memory() { ++_requests_blocked_memory; }

    void setup_metrics(metrics::metric_groups& mgs);

private:
    uint64_t _requests_completed = 0;
    uint64_t _in_bytes = 0;
    uint64_t _out_bytes = 0;
    uint64_t _connects = 0;
    uint32_t _connections = 0;
    uint32_t _connection_close_error = 0;
    uint32_t _bad_requests = 0;
    uint32_t _corrupted_headers = 0;
    uint32_t _method_not_found_errors = 0;
    uint32_t _requests_blocked_memory = 0;
    friend std::ostream& operator<<(std::ostream& o, const server_probe& p);
};

std::ostream& operator<<(std::ostream& o, const server_probe& p);
}; // namespace rpc
