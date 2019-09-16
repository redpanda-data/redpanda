#pragma once
#include <iostream>

namespace rpc {

class server_probe {
public:
    void connection_established() {
        ++_connects;
        ++_connections;
    }

    void connection_closed() {
        --_connections;
    }

    void connection_close_error() {
        ++_connection_close_error;
    }

    void add_bytes_sent(size_t sent) {
        _out_bytes += sent;
    }

    void add_bytes_recieved(size_t recv) {
        _in_bytes += recv;
    }

    void request_completed() {
        ++_requests_completed;
    }

    void header_corrupted() {
        ++_bad_requests;
        ++_corrupted_headers;
    }

    void method_not_found() {
        ++_bad_requests;
        ++_method_not_found_errors;
    }

    void waiting_for_available_memory() {
        ++_requests_blocked_memory;
    }

    uint64_t get_connects() const {
        return _connects;
    }

    uint64_t get_connections() const {
        return _connections;
    }

    uint64_t get_connection_close_errors() const {
        return _connection_close_error;
    }

    uint64_t get_requests_completed() const {
        return _requests_completed;
    }

    uint64_t get_in_bytes() const {
        return _in_bytes;
    }

    uint64_t get_out_bytes() const {
        return _out_bytes;
    }

    uint64_t get_bad_requests() const {
        return _bad_requests;
    }

    uint64_t get_requests_blocked_memory() const {
        return _requests_blocked_memory;
    }

    uint64_t get_corrupted_headers() const {
        return _corrupted_headers;
    }

    uint64_t get_method_not_found_errors() const {
        return _method_not_found_errors;
    }

private:
    uint64_t _connects = 0;
    uint64_t _connections = 0;
    uint64_t _connection_close_error = 0;
    uint64_t _requests_completed = 0;
    uint64_t _in_bytes = 0;
    uint64_t _out_bytes = 0;
    uint64_t _bad_requests = 0;
    uint64_t _corrupted_headers = 0;
    uint64_t _method_not_found_errors = 0;
    uint64_t _requests_blocked_memory = 0;
};

std::ostream& operator<<(std::ostream& o, const server_probe& p);
}; // namespace rpc
