#pragma once
#include "rpc/logger.h"

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

    void add_bytes_sent(size_t sent) {
        _out_bytes += sent;
    }

    void add_bytes_received(size_t recv) {
        _in_bytes += recv;
    }

    void connection_established() {
        ++_connects;
        ++_connections;
    }

    void connection_closed() {
        --_connections;
    }

    void connection_error(std::exception_ptr e) {
        rpclog().error("Connection error: {}", e);
        ++_connection_errors;
    }

    void read_dispatch_error(std::exception_ptr e) {
        rpclog().error("Error dispatching client reads: {}", e);
        ++_read_dispatch_errors;
    }

    void header_corrupted() {
        ++_corrupted_headers;
        request_error();
    }

    void client_correlation_error() {
        ++_client_correlation_errors;
    }

    void server_correlation_error() {
        ++_server_correlation_errors;
    }

    void waiting_for_available_memory() {
        ++_requests_blocked_memory;
    }

    uint64_t get_requests_sent() const {
        return _requests_sent;
    }

    uint32_t get_requests_pending() const {
        return _requests_pending;
    }

    uint64_t get_requests_completed() const {
        return _requests_completed;
    }

    uint32_t get_request_errors() const {
        return _request_errors;
    }

    uint64_t get_in_bytes() const {
        return _in_bytes;
    }

    uint64_t get_out_bytes() const {
        return _out_bytes;
    }

    uint64_t get_connects() const {
        return _connects;
    }

    uint32_t get_connections() const {
        return _connections;
    }

    uint32_t get_connection_errors() const {
        return _connection_errors;
    }

    uint32_t get_read_dispatch_errors() const {
        return _read_dispatch_errors;
    }

    uint32_t get_server_correlation_errors() const {
        return _server_correlation_errors;
    }

    uint32_t get_client_correlation_errors() const {
        return _client_correlation_errors;
    }

    uint32_t get_corrupted_headers() const {
        return _corrupted_headers;
    }

    uint32_t get_requests_blocked_memory() const {
        return _requests_blocked_memory;
    }

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
};
}; // namespace rpc

namespace std {
inline ostream& operator<<(ostream& o, const rpc::client_probe& p) {
    o << "{"
      << " requests_sent: " << p.get_requests_sent()
      << ", requests_pending: " << p.get_requests_pending()
      << ", requests_completed: " << p.get_requests_completed()
      << ", request_errors: " << p.get_request_errors()
      << ", in_bytes: " << p.get_in_bytes()
      << ", out_bytes: " << p.get_out_bytes()
      << ", connects: " << p.get_connects()
      << ", connections: " << p.get_connections()
      << ", connection_errors: " << p.get_connection_errors()
      << ", read_dispatch_errors: " << p.get_read_dispatch_errors()
      << ", corrupted_headers: " << p.get_corrupted_headers()
      << ", server_correlation_errors: " << p.get_server_correlation_errors()
      << ", client_correlation_errors: " << p.get_client_correlation_errors()
      << ", requests_blocked_memory: " << p.get_requests_blocked_memory()
      << " }";
    return o;
}
}; // namespace std
