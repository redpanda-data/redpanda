/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "worker.h"

#include "metrics/metrics.h"
#include "rpc/rpc_server.h"

#include <seastar/core/prometheus.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/smp.hh>

#include <memory>

namespace transform::worker {

ss::future<> worker_service::start(config cfg) {
    _wasm_runtime = std::make_unique<wasm::caching_runtime>(
      wasm::runtime::create_default(/*sr=*/nullptr));
    // TODO: Support injecting these from cluster config?
    constexpr wasm::runtime::config wasm_config = {
        .heap_memory = {
            .per_core_pool_size_bytes = 20_MiB,
            .per_engine_memory_limit = 2_MiB,
        },
        .stack_memory = {
            .debug_host_stack_usage = false,
        },
        .cpu = {
            .per_invocation_timeout = 3s,
        },
    };
    co_await _wasm_runtime->start(wasm_config);
    co_await _service.start(_wasm_runtime.get());
    co_await _rpc_server.start(cfg.server);
    co_await _rpc_server.invoke_on_all([this](::rpc::rpc_server& s) {
        s.register_service<network_service>(
          ss::default_scheduling_group(),
          ss::default_smp_service_group(),
          &_service);
        s.set_all_services_added();
    });
    co_await _rpc_server.invoke_on_all(&::rpc::rpc_server::start);
    co_await _http_server.start(ss::sstring("admin"));
    co_await _http_server.invoke_on_all(
      &ss::httpd::http_server::set_content_streaming, true);
    constexpr uint16_t http_server_port = 9644;
    co_await _http_server.invoke_on_all<
      ss::future<> (ss::httpd::http_server::*)(ss::socket_address),
      uint16_t>(&ss::httpd::http_server::listen, http_server_port);
    co_await ss::prometheus::add_prometheus_routes(
      _http_server,
      {
        .metric_help = "redpanda wasm worker metrics",
        .prefix = "vectorized",
        .handle = ss::metrics::default_handle(),
        .route = "/metrics",
      });
    co_await ss::prometheus::add_prometheus_routes(
      _http_server,
      {
        .metric_help = "redpanda wasm worker metrics",
        .prefix = "redpanda",
        .handle = metrics::public_metrics_handle,
        .route = "/public_metrics",
      });
}
ss::future<> worker_service::stop() {
    co_await _http_server.stop();
    co_await _rpc_server.stop();
    co_await _service.stop();
    co_await _wasm_runtime->stop();
}

} // namespace transform::worker
