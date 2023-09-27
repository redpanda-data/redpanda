/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "redpanda/management_endpoint.h"

#include "config/configuration.h"
#include "net/dns.h"
#include "redpanda/logger.h"
#include "rpc/rpc_utils.h"
#include "ssx/metrics.h"

#include <seastar/core/prometheus.hh>

ss::future<> management_endpoint::start() {
    configure_metrics_route();
    co_await configure_listeners();

    vlog(
      alogger.info,
      "Started HTTP admin service listening at {}",
      _cfg.endpoints);
}

ss::future<> management_endpoint::stop() { return _server.stop(); }

ss::future<> management_endpoint::configure_listeners() {
    // We will remember any endpoint that is listening
    // on an external address and does not have mTLS,
    // for emitting a warning later if user/pass auth is disabled.
    std::optional<model::broker_endpoint> insecure_ep;

    for (auto& ep : _cfg.endpoints) {
        // look for credentials matching current endpoint
        auto tls_it = std::find_if(
          _cfg.endpoints_tls.begin(),
          _cfg.endpoints_tls.end(),
          [&ep](const config::endpoint_tls_config& c) {
              return c.name == ep.name;
          });

        const bool localhost = ep.address.host() == "127.0.0.1"
                               || ep.address.host() == "localhost"
                               || ep.address.host() == "localhost.localdomain"
                               || ep.address.host() == "::1";

        ss::shared_ptr<ss::tls::server_credentials> cred;
        if (tls_it != _cfg.endpoints_tls.end()) {
            auto builder = co_await tls_it->config.get_credentials_builder();
            if (builder) {
                cred = co_await builder->build_reloadable_server_credentials(
                  [](
                    const std::unordered_set<ss::sstring>& updated,
                    const std::exception_ptr& eptr) {
                      rpc::log_certificate_reload_event(
                        alogger, "API TLS", updated, eptr);
                  });
            }

            if (!localhost && !tls_it->config.get_require_client_auth()) {
                insecure_ep = ep;
            }
        } else {
            if (!localhost) {
                insecure_ep = ep;
            }
        }

        auto resolved = co_await net::resolve_dns(ep.address);
        co_await ss::with_scheduling_group(_cfg.sg, [this, cred, resolved] {
            return _server.listen(resolved, cred);
        });
    }

    if (
      insecure_ep.has_value()
      && !config::shard_local_cfg().admin_api_require_auth()) {
        auto& ep = insecure_ep.value();
        vlog(
          alogger.warn,
          "Insecure Admin API listener on {}:{}, consider enabling "
          "`admin_api_require_auth`",
          ep.address.host(),
          ep.address.port());
    }
}
void management_endpoint::configure_metrics_route() {
    ss::prometheus::add_prometheus_routes(
      _server,
      {.metric_help = "redpanda metrics",
       .prefix = "vectorized",
       .handle = ss::metrics::default_handle(),
       .route = "/metrics"})
      .get();
    ss::prometheus::add_prometheus_routes(
      _server,
      {.metric_help = "redpanda metrics",
       .prefix = "redpanda",
       .handle = ssx::metrics::public_metrics_handle,
       .route = "/public_metrics"})
      .get();
}
