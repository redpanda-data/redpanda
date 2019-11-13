#pragma once

#include "prometheus/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>

#include <cstdint>

namespace kafka {

class probe {
public:
    void connection_established() {
        ++_connects;
        ++_connections;
    }

    void connection_closed() {
        --_connections;
    }

    void waiting_for_available_memory() {
        ++_requests_blocked_memory;
    }

    void serving_request() {
        ++_requests_serving;
    }

    void request_served() {
        ++_requests_served;
        --_requests_serving;
    }

    void request_processing_error() {
        ++_request_processing_errors;
        --_requests_serving;
    }

    void add_bytes_sent(size_t sent) {
        _bytes_sent += sent;
    }

    void add_bytes_received(size_t recv) {
        _bytes_received += recv;
    }

    void setup_metrics(metrics::metric_groups& mgs) {
        namespace sm = metrics;
        mgs.add_group(
          prometheus_sanitize::metrics_name("kafka:api:server"),
          {
            sm::make_derive(
              "connects",
              [this] { return _connects; },
              sm::description("Number of connection attempts")),
            sm::make_derive(
              "connections",
              [this] { return _connections; },
              sm::description("Number of connections")),
            sm::make_derive(
              "requests_blocked_memory",
              [this] { return _requests_blocked_memory; },
              sm::description(
                "Number of requests that are currently blocked by "
                "insufficient memory")),
            sm::make_total_operations(
              "requests_served",
              [this] { return _requests_served; },
              sm::description("Total number of requests served")),
            sm::make_derive(
              "requests_serving",
              [this] { return _requests_serving; },
              sm::description("Requests that are being served now")),
            sm::make_derive(
              "bytes_received",
              [this] { return _bytes_received; },
              sm::description("Bytes received by Kafka API server")),
            sm::make_derive(
              "bytes_sent",
              [this] { return _bytes_sent; },
              sm::description("Bytes sent by Kafka API server")),
            sm::make_derive(
              "request_processing_errors",
              [this] { return _request_processing_errors; },
              sm::description("Total number of requests processing errors")),
          });
    }

private:
    uint64_t _requests_served = 0;
    uint64_t _bytes_received = 0;
    uint64_t _bytes_sent = 0;
    uint32_t _requests_serving = 0;
    uint32_t _request_processing_errors = 0;
    uint32_t _requests_blocked_memory = 0;
    uint32_t _connections = 0;
    uint64_t _connects = 0;
};

} // namespace kafka
