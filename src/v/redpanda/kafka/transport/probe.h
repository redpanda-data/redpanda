#pragma once

#include "prometheus/prometheus_sanitize.h"
#include "redpanda/kafka/requests/request_context.h"

#include <seastar/core/metrics.hh>

#include <cstdint>

namespace kafka::transport {

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

    void serving_request(requests::request_context&) {
        ++_requests_served;
        ++_requests_serving;
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
          });
    }

private:
    uint64_t _requests_served = 0;
    uint32_t _requests_serving = 0;
    uint32_t _requests_blocked_memory = 0;
    uint64_t _connects = 0;
    uint32_t _connections = 0;
};

} // namespace kafka::transport
