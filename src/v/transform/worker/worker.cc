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

#include "rpc/rpc_server.h"

namespace transform::worker {

worker_service::worker_service(config cfg)
  : _rpc_server(std::move(cfg.server)) {}

ss::future<> worker_service::start() {
    _wasm_runtime = std::make_unique<wasm::caching_runtime>(
      wasm::runtime::create_default(/*sr=*/nullptr));
    constexpr wasm::runtime::config wasm_config = {
        .heap_memory = {
            .per_core_pool_size_bytes = 10_MiB,
            .per_engine_memory_limit = 1_MiB,
        },
        .stack_memory = {
            .debug_host_stack_usage = false,
        },
        .cpu = {
            .per_invocation_timeout = 3s,
        },
    };
    co_await _wasm_runtime->start(wasm_config);
    _rpc_server.start();
    _rpc_server.set_all_services_added();
}
ss::future<> worker_service::stop() {
    co_await _rpc_server.shutdown_input();
    co_await _rpc_server.wait_for_shutdown();
    co_await _wasm_runtime->stop();
}

} // namespace transform::worker
