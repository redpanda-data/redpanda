#include "rpc/server_probe.h"

#include "prometheus/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>

namespace rpc {
void server_probe::setup_metrics(metrics::metric_groups& mgs) {
    namespace sm = metrics;
    mgs.add_group(
      prometheus_sanitize::metrics_name("rpc"),
      {
        sm::make_gauge(
          "active_connections",
          [this] { return _connections; },
          sm::description("Currently active connections")),
        sm::make_derive(
          "connects",
          [this] { return _connects; },
          sm::description("Number of accepted connections")),
        sm::make_derive(
          "connection_close_errors",
          [this] { return _connection_close_error; },
          sm::description(
            "Number of errors when shutting down the connection")),
        sm::make_derive(
          "requests_completed",
          [this] { return _requests_completed; },
          sm::description("Number of successfully served requests")),
        sm::make_derive(
          "received_bytes",
          [this] { return _in_bytes; },
          sm::description(
            "Number of bytes received from the clients in valid requests")),
        sm::make_derive(
          "sent_bytes",
          [this] { return _out_bytes; },
          sm::description("Number of bytes sent to clients")),
        sm::make_derive(
          "method_not_found_errors",
          [this] { return _method_not_found_errors; },
          sm::description("Number of requests with not available RPC method")),
        sm::make_derive(
          "corrupted_headers",
          [this] { return _corrupted_headers; },
          sm::description("Number of requests with corrupted headers")),
        sm::make_derive(
          "bad_requests",
          [this] { return _bad_requests; },
          sm::description("Total number of all bad requests")),
        sm::make_derive(
          "requests_blocked_memory",
          [this] { return _requests_blocked_memory; },
          sm::description(
            "Number of requests that have to"
            "wait for processing beacause of insufficient memory")),
      });
}

std::ostream& operator<<(std::ostream& o, const server_probe& p) {
    o << "{"
      << "connects: " << p._connects << ", "
      << "current connections: " << p._connections << ", "
      << "connection close errors: " << p._connection_close_error << ", "
      << "requests completed: " << p._requests_completed << ", "
      << "received bytes: " << p._in_bytes << ", "
      << "sent bytes: " << p._out_bytes << ", "
      << "bad requests: " << p._bad_requests << ", "
      << "corrupted headers: " << p._corrupted_headers << ", "
      << "method not found errors: " << p._method_not_found_errors << ", "
      << "requests blocked by memory: " << p._requests_blocked_memory << "}";
    return o;
}
} // namespace rpc
