/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "rpc/rpc_server.h"
#include "service.h"
#include "wasm/cache.h"

#include <seastar/http/httpd.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/noncopyable_function.hh>

namespace transform::worker {

/**
 * The entry point to the worker for Data Transforms
 */
class worker_service {
public:
    struct config {
        net::server_configuration server;
    };
    ss::future<> start(config cfg);
    ss::future<> stop();

private:
    std::unique_ptr<wasm::caching_runtime> _wasm_runtime;
    ss::sharded<local_service> _service;
    ss::sharded<::rpc::rpc_server> _rpc_server;
    ss::sharded<ss::httpd::http_server> _http_server;
};

} // namespace transform::worker
