// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/configuration.h"
#include "hashing/crc32c.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"
#include "net/client_probe.h"
#include "net/server_probe.h"
#include "net/tls_certificate_probe.h"
#include "net/types.h"
#include "ssx/sformat.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/smp.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/tls.hh>
#include <seastar/util/defer.hh>

#include <boost/lexical_cast.hpp>
#include <fmt/chrono.h>
#include <fmt/ranges.h>

#include <chrono>
#include <ostream>
#include <span>
#include <string>
#include <vector>

namespace net {
void server_probe::setup_metrics(
  metrics::internal_metric_groups& mgs, std::string_view proto) {
    namespace sm = ss::metrics;
    mgs.add_group(
      prometheus_sanitize::metrics_name(ss::sstring{proto}),
      {
        sm::make_gauge(
          "active_connections",
          [this] { return _connections; },
          sm::description(
            ssx::sformat("{}: Currently active connections", proto))),
        sm::make_counter(
          "connects",
          [this] { return _connects; },
          sm::description(
            ssx::sformat("{}: Number of accepted connections", proto))),
        sm::make_counter(
          "connection_close_errors",
          [this] { return _connection_close_error; },
          sm::description(ssx::sformat(
            "{}: Number of errors when shutting down the connection", proto))),
        sm::make_counter(
          "connections_rejected",
          [this] { return _connections_rejected_open_limit; },
          sm::description(ssx::sformat(
            "{}: Number of connection attempts rejected for hitting open "
            "connection count limits",
            proto))),
        sm::make_counter(
          "connections_rejected_rate_limit",
          [this] { return _connections_rejected_rate_limit; },
          sm::description(ssx::sformat(
            "{}: Number of connection attempts rejected for hitting "
            "connection rate limits",
            proto))),
        sm::make_counter(
          "requests_completed",
          [this] { return _requests_completed; },
          sm::description(
            ssx::sformat("{}: Number of successful requests", proto))),
        sm::make_total_bytes(
          "received_bytes",
          [this] { return _in_bytes; },
          sm::description(ssx::sformat(
            "{}: Number of bytes received from the clients in valid requests",
            proto))),
        sm::make_total_bytes(
          "sent_bytes",
          [this] { return _out_bytes; },
          sm::description(
            ssx::sformat("{}: Number of bytes sent to clients", proto))),
        sm::make_counter(
          "method_not_found_errors",
          [this] { return _method_not_found_errors; },
          sm::description(ssx::sformat(
            "{}: Number of requests with not available RPC method", proto))),
        sm::make_counter(
          "corrupted_headers",
          [this] { return _corrupted_headers; },
          sm::description(ssx::sformat(
            "{}: Number of requests with corrupted headers", proto))),
        sm::make_counter(
          "service_errors",
          [this] { return _service_errors; },
          sm::description(ssx::sformat("{}: Number of service errors", proto))),
        sm::make_counter(
          "requests_blocked_memory",
          [this] { return _requests_blocked_memory; },
          sm::description(ssx::sformat(
            "{}: Number of requests blocked in memory backpressure", proto))),
        sm::make_gauge(
          "requests_pending",
          [this] { return _requests_received - _requests_completed; },
          sm::description(ssx::sformat(
            "{}: Number of requests pending in the queue", proto))),
        sm::make_counter(
          "connections_wait_rate",
          [this] { return _connections_wait_rate; },
          sm::description(ssx::sformat(
            "{}: Number of connections are blocked by connection rate",
            proto))),
        sm::make_counter(
          "produce_bad_create_time",
          [this] { return _produce_bad_create_time; },
          sm::description("number of produce requests with timestamps too far "
                          "in the future or in the past")),
      },
      {},
      {sm::shard_label});
}

void server_probe::setup_public_metrics(
  metrics::public_metric_groups& mgs, std::string_view proto) {
    namespace sm = ss::metrics;

    if (proto.ends_with("_rpc")) {
        proto.remove_suffix(4);
    }

    auto server_label = metrics::make_namespaced_label("server");

    mgs.add_group(
      "rpc",
      {
        sm::make_counter(
          "request_errors_total",
          [this] { return _service_errors; },
          sm::description("Number of rpc errors"),
          {server_label(proto)})
          .aggregate({sm::shard_label}),
        sm::make_gauge(
          "active_connections",
          [this] { return _connections; },
          sm::description("Count of currently active connections"),
          {server_label(proto)})
          .aggregate({sm::shard_label}),
        sm::make_total_bytes(
          "received_bytes",
          [this] { return _in_bytes; },
          sm::description(ssx::sformat(
            "{}: Number of bytes received from the clients in valid requests",
            proto)),
          {server_label(proto)})
          .aggregate({sm::shard_label}),
        sm::make_total_bytes(
          "sent_bytes",
          [this] { return _out_bytes; },
          sm::description(
            ssx::sformat("{}: Number of bytes sent to clients", proto)),
          {server_label(proto)})
          .aggregate({sm::shard_label}),
      });
}

std::ostream& operator<<(std::ostream& o, const server_probe& p) {
    o << "{"
      << "connects: " << p._connects << ", "
      << "current connections: " << p._connections << ", "
      << "connection close errors: " << p._connection_close_error << ", "
      << "connections rejected (open limit): "
      << p._connections_rejected_open_limit << ", "
      << "connections rejected (rate limit): "
      << p._connections_rejected_rate_limit << ", "
      << "requests received: " << p._requests_received << ", "
      << "requests completed: " << p._requests_completed << ", "
      << "service errors: " << p._service_errors << ", "
      << "received bytes: " << p._in_bytes << ", "
      << "sent bytes: " << p._out_bytes << ", "
      << "corrupted headers: " << p._corrupted_headers << ", "
      << "method not found errors: " << p._method_not_found_errors << ", "
      << "requests blocked by memory: " << p._requests_blocked_memory << ", "
      << "connections wait rate: " << p._connections_wait_rate << ", "
      << "produce bad create time: " << p._produce_bad_create_time << ", "
      << "}";
    return o;
}

void client_probe::setup_metrics(
  std::string_view name,
  const std::vector<ss::metrics::label_instance>& labels,
  const std::vector<ss::metrics::label>& aggregate_labels,
  std::vector<ss::metrics::metric_definition> defs) {
    namespace sm = ss::metrics;

    defs.emplace_back(sm::make_gauge(
                        "active_connections",
                        [this] { return _connections; },
                        sm::description("Currently active connections"),
                        labels)
                        .aggregate(aggregate_labels));

    defs.emplace_back(sm::make_counter(
                        "connects",
                        [this] { return _connects; },
                        sm::description("Connection attempts"),
                        labels)
                        .aggregate(aggregate_labels));

    defs.emplace_back(sm::make_counter(
                        "connection_errors",
                        [this] { return _connection_errors; },
                        sm::description("Number of connection errors"),
                        labels)
                        .aggregate(aggregate_labels));

    _metrics.add_group(
      prometheus_sanitize::metrics_name(ss::sstring(name)), defs);
}

ss::future<ss::shared_ptr<ss::tls::server_credentials>>
build_reloadable_server_credentials_with_probe(
  config::tls_config config,
  ss::sstring service,
  ss::sstring listener_name,
  ss::tls::reload_callback cb) {
    auto builder = co_await config.get_credentials_builder();
    if (!builder) {
        co_return nullptr;
    }
    co_return co_await build_reloadable_credentials_with_probe<
      ss::tls::server_credentials>(
      std::move(*builder),
      std::move(service),
      std::move(listener_name),
      std::move(cb));
}

template<TLSCreds T>
ss::future<ss::shared_ptr<T>> build_reloadable_credentials_with_probe(
  ss::tls::credentials_builder builder,
  ss::sstring area,
  ss::sstring detail,
  ss::tls::reload_callback cb) {
    auto probe = ss::make_lw_shared<net::tls_certificate_probe>();
    auto wrap_cb =
      [probe, cb = std::move(cb)](
        const std::unordered_set<ss::sstring>& updated,
        const ss::tls::certificate_credentials& creds,
        const std::exception_ptr& eptr,
        std::optional<ss::tls::blob> trust_file_contents = std::nullopt) {
          if (cb) {
              cb(updated, eptr);
          }
          probe->loaded(creds, eptr, trust_file_contents);
      };

    ss::shared_ptr<T> cred;
    if constexpr (std::is_same<T, ss::tls::server_credentials>::value) {
        cred = co_await builder.build_reloadable_server_credentials(wrap_cb);
    } else {
        cred = co_await builder.build_reloadable_certificate_credentials(
          wrap_cb);
    }

    probe->setup_metrics(std::move(area), std::move(detail));
    probe->loaded(*cred, nullptr, builder.get_trust_file_blob());
    co_return cred;
}

template ss::future<ss::shared_ptr<ss::tls::server_credentials>>
build_reloadable_credentials_with_probe(
  ss::tls::credentials_builder builder,
  ss::sstring area,
  ss::sstring detail,
  ss::tls::reload_callback cb);

template ss::future<ss::shared_ptr<ss::tls::certificate_credentials>>
build_reloadable_credentials_with_probe(
  ss::tls::credentials_builder builder,
  ss::sstring area,
  ss::sstring detail,
  ss::tls::reload_callback cb);

void tls_certificate_probe::loaded(
  const ss::tls::certificate_credentials& creds,
  std::exception_ptr ex,
  std::optional<ss::tls::blob> trust_file_contents) {
    _load_time = clock_type::now();
    reset();

    if (ex) {
        return;
    }

    auto to_tls_serial = [](const std::vector<std::byte>& b) {
        using T = tls_serial_number::type;
        T result = 0;
        const auto end = std::min(b.size(), sizeof(T));
        std::memcpy(&result, b.data(), end);
        return tls_serial_number{result};
    };

    auto certs_info = creds.get_cert_info();
    auto ts_info = creds.get_trust_list_info();

    if (!certs_info.has_value() || !ts_info.has_value()) {
        return;
    }

    _cert_loaded = true;

    for (auto& info : certs_info.value()) {
        auto exp = clock_type::from_time_t(info.expiry);
        auto srl = to_tls_serial(info.serial);
        if (!_cert || exp < _cert->expiry) {
            _cert.emplace(cert{.expiry = exp, .serial = srl});
        }
    }

    for (auto& info : ts_info.value()) {
        auto exp = clock_type::from_time_t(info.expiry);
        auto srl = to_tls_serial(info.serial);
        if (!_ca || exp < _ca->expiry) {
            _ca.emplace(cert{.expiry = exp, .serial = srl});
        }
    }

    _trust_file_crc32c = [&trust_file_contents]() -> uint32_t {
        if (!trust_file_contents.has_value()) {
            return 0u;
        }
        crc::crc32c hash;
        hash.extend(
          trust_file_contents.value().data(),
          trust_file_contents.value().size());
        return hash.value();
    }();
}

void tls_certificate_probe::setup_metrics(
  std::string_view area, std::string_view detail) {
    if (ss::this_shard_id() != 0) {
        return;
    }

    namespace sm = ss::metrics;
    const auto area_label = sm::label("area");
    const auto detail_label = sm::label("detail");

    const std::vector<sm::label_instance> labels = {
      area_label(area), detail_label(detail)};

    auto setup = [this,
                  &labels](const std::vector<sm::label>& aggregate_labels) {
        using namespace std::literals::chrono_literals;
        std::vector<sm::metric_definition> defs;
        defs.emplace_back(
          sm::make_gauge(
            "truststore_expires_at_timestamp_seconds",
            [this] {
                return _ca.value_or(cert{}).expiry.time_since_epoch() / 1s;
            },
            sm::description(
              "Expiry time of the shortest-lived CA in the truststore"
              "(seconds since epoch)"),
            labels)
            .aggregate(aggregate_labels));
        defs.emplace_back(
          sm::make_gauge(
            "certificate_expires_at_timestamp_seconds",
            [this] {
                return _cert.value_or(cert{}).expiry.time_since_epoch() / 1s;
            },
            sm::description(
              "Expiry time of the server certificate (seconds since epoch)"),
            labels)
            .aggregate(aggregate_labels));
        defs.emplace_back(
          sm::make_gauge(
            "certificate_serial",
            [this] { return _cert.value_or(cert{}).serial; },
            sm::description("Least significant four bytes of the server "
                            "certificate serial number"),
            labels)
            .aggregate(aggregate_labels));
        defs.emplace_back(
          sm::make_gauge(
            "loaded_at_timestamp_seconds",
            [this] { return _load_time.time_since_epoch() / 1s; },
            sm::description(
              "Load time of the server certificate (seconds since epoch)."),
            labels)
            .aggregate(aggregate_labels));
        defs.emplace_back(
          sm::make_gauge(
            "certificate_valid",
            [this] { return cert_valid() ? 1 : 0; },
            sm::description("The value is one if the certificate is valid with "
                            "the given truststore, otherwise zero."),
            labels)
            .aggregate(aggregate_labels));
        defs.emplace_back(
          sm::make_gauge(
            "trust_file_crc32c",
            [this] { return cert_valid() ? _trust_file_crc32c : 0; },
            sm::description(
              "crc32c calculated from the contents of the trust "
              "file if loaded certificate is valid and trust store "
              "is present, otherwise zero."),
            labels)
            .aggregate(aggregate_labels));
        return defs;
    };

    if (!config::shard_local_cfg().disable_metrics()) {
        const auto aggregate_labels
          = config::shard_local_cfg().aggregate_metrics()
              ? std::vector<sm::label>{sm::shard_label}
              : std::vector<sm::label>{};
        _metrics.add_group(
          prometheus_sanitize::metrics_name("tls"), setup(aggregate_labels));
    }

    if (!config::shard_local_cfg().disable_public_metrics()) {
        _public_metrics.add_group(
          prometheus_sanitize::metrics_name("tls"),
          setup(std::vector<sm::label>{sm::shard_label}));
    }
}

} // namespace net
