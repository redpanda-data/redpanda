#include "prometheus/prometheus_sanitize.h"
#include "rpc/client_probe.h"
#include "rpc/server_probe.h"

#include <seastar/core/metrics.hh>

namespace rpc {
void server_probe::setup_metrics(ss::metrics::metric_groups& mgs) {
    namespace sm = ss::metrics;
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

void client_probe::setup_metrics(
  ss::metrics::metric_groups& mgs,
  const std::optional<ss::sstring>& service_name,
  const ss::socket_address& target_addr) {
    namespace sm = ss::metrics;
    auto target = sm::label("target");
    std::vector<sm::label_instance> labels = {target(target_addr)};
    if (service_name) {
        labels.push_back(sm::label("service_name")(*service_name));
    }
    mgs.add_group(
      prometheus_sanitize::metrics_name("rpc_client"),
      {
        sm::make_gauge(
          "active_connections",
          [this] { return _connections; },
          sm::description("Currently active connections"),
          labels),
        sm::make_derive(
          "connects",
          [this] { return _connects; },
          sm::description("Connection attempts"),
          labels),
        sm::make_derive(
          "requests_sent",
          [this] { return _requests_sent; },
          sm::description("Number of requests sent"),
          labels),
        sm::make_derive(
          "requests_pending",
          [this] { return _requests_pending; },
          sm::description("Number of requests pending"),
          labels),
        sm::make_derive(
          "request_errors",
          [this] { return _request_errors; },
          sm::description("Number or requests errors"),
          labels),
        sm::make_total_bytes(
          "in_bytes",
          [this] { return _out_bytes; },
          sm::description("Total number of bytes sent (including headers)"),
          labels),
        sm::make_total_bytes(
          "out_bytes",
          [this] { return _in_bytes; },
          sm::description("Total number of bytes received"),
          labels),
        sm::make_derive(
          "connection_errors",
          [this] { return _connection_errors; },
          sm::description("Number of connection errors"),
          labels),
        sm::make_derive(
          "read_dispatch_errors",
          [this] { return _read_dispatch_errors; },
          sm::description("Number of errors while dispatching responses"),
          labels),
        sm::make_derive(
          "corrupted_headers",
          [this] { return _corrupted_headers; },
          sm::description("Number of responses with corrupted headers"),
          labels),
        sm::make_derive(
          "server_correlation_errors",
          [this] { return _server_correlation_errors; },
          sm::description("Number of responses with wrong correlation id"),
          labels),
        sm::make_derive(
          "client_correlation_errors",
          [this] { return _client_correlation_errors; },
          sm::description("Number of errors in client correlation id"),
          labels),
        sm::make_derive(
          "requests_blocked_memory",
          [this] { return _requests_blocked_memory; },
          sm::description("Number of requests that are blocked beacause"
                          " of insufficient memory"),
          labels),
      });
}

std::ostream& operator<<(std::ostream& o, const rpc::client_probe& p) {
    o << "{"
      << " requests_sent: " << p._requests_sent
      << ", requests_pending: " << p._requests_pending
      << ", requests_completed: " << p._requests_completed
      << ", request_errors: " << p._request_errors
      << ", in_bytes: " << p._in_bytes << ", out_bytes: " << p._out_bytes
      << ", connects: " << p._connects << ", connections: " << p._connections
      << ", connection_errors: " << p._connection_errors
      << ", read_dispatch_errors: " << p._read_dispatch_errors
      << ", corrupted_headers: " << p._corrupted_headers
      << ", server_correlation_errors: " << p._server_correlation_errors
      << ", client_correlation_errors: " << p._client_correlation_errors
      << ", requests_blocked_memory: " << p._requests_blocked_memory << " }";
    return o;
}
} // namespace rpc
