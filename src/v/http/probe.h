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

#include <cstdint>

namespace http {

/// Simple differential counting for requests
struct diff_counter {
    diff_counter() = default;
    /// Count request start
    void start() { _started++; }
    /// Count request termination
    void complete() { _completed++; }
    /// Total number of completed requests
    uint64_t total() const { return _completed; }
    /// Number of active requests
    uint64_t active() const {
        if (_completed <= _started) {
            return _started - _completed;
        }
        return 0;
    }

private:
    uint64_t _started;
    uint64_t _completed;
};

class client_probe {
public:
    enum class verb {
        get,
        put,
        other,
    };

    client_probe() = default;

    /// Register incomming traffic
    void add_inbound_bytes(uint64_t bytes) { _in += bytes; }

    /// Register outgoing traffic
    void add_outbound_bytes(uint64_t bytes) { _out += bytes; }

    /// RAII wrapper for per-request probe
    struct subprobe {
        verb v;
        client_probe& probe;
        subprobe(verb v, client_probe& p)
          : v(v)
          , probe(p) {
            switch (v) {
            case verb::get:
                probe._get_requests.start();
                break;
            case verb::put:
                probe._put_requests.start();
                break;
            default:
                break;
            }
            probe._all_requests.start();
        }
        subprobe& operator=(const subprobe&) = delete;
        subprobe& operator=(subprobe&&) = delete;
        subprobe(const subprobe&) = delete;
        subprobe(subprobe&&) = delete;
        ~subprobe() {
            switch (v) {
            case verb::get:
                probe._get_requests.complete();
                break;
            case verb::put:
                probe._put_requests.complete();
                break;
            default:
                break;
            }
            probe._all_requests.complete();
        }
    };

    auto create_request_subprobe(verb v) { return subprobe(v, *this); }

    void register_transport_error() { _transport_errors += 1; }

    /// Return total incomming traffic
    uint64_t get_inbound_bytes() const { return _in; }

    /// Return total outgoing traffic
    uint64_t get_outbound_bytes() const { return _out; }

    /// Return total number of transport errors
    uint64_t get_transport_errors() const { return _transport_errors; }

    /// Get total number of GET requests
    uint64_t get_total_get_requests() const { return _get_requests.total(); }

    /// Get total number of PUT requests
    uint64_t get_total_put_requests() const { return _put_requests.total(); }

    /// Get total number of requests
    uint64_t get_total_requests() const { return _all_requests.total(); }

    /// Get number of in-flight GET requests
    uint64_t get_active_get_requests() const { return _get_requests.active(); }

    /// Get number of in-flight PUT requests
    uint64_t get_active_put_requests() const { return _put_requests.active(); }

    /// Get number of in-flight requests
    uint64_t get_active_requests() const { return _all_requests.active(); }

private:
    /// Total inbound traffic size
    uint64_t _in;
    /// Total outbound traffic size
    uint64_t _out;
    /// Number of downloads
    diff_counter _get_requests;
    /// Number of uploads
    diff_counter _put_requests;
    /// All requests
    diff_counter _all_requests;
    /// Number of connection errors
    uint64_t _transport_errors;
};

} // namespace http
