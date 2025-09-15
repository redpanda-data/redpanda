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

#include "base/seastarx.h"
#include "cluster/fwd.h"
#include "pandaproxy/fwd.h"
#include "pandaproxy/rest/configuration.h"
#include "pandaproxy/server.h"
#include "pandaproxy/util.h"
#include "utils/adjustable_semaphore.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/net/socket_defs.hh>

namespace pandaproxy::rest {

class proxy : public ss::peering_sharded_service<proxy> {
public:
    using server = auth_ctx_server<proxy>;
    proxy(
      const YAML::Node& config,
      const YAML::Node& client_cfg,
      ss::smp_service_group smp_sg,
      size_t max_memory,
      ss::sharded<kafka::client::client>& client,
      ss::sharded<kafka_client_cache>& client_cache,
      cluster::controller* controller);

    ss::future<> start();
    ss::future<> stop();

    configuration& config();
    const configuration& config() const;
    ss::sharded<kafka::client::client>& client() { return _client; }
    ss::sharded<kafka_client_cache>& client_cache() { return _client_cache; }
    ss::future<> mitigate_error(std::exception_ptr);

private:
    ss::future<> do_start();
    ss::future<> configure();

    configuration _config;
    kafka::client::configuration _client_cfg;
    ssx::semaphore _mem_sem;
    adjustable_semaphore _inflight_sem;
    config::binding<size_t> _inflight_config_binding;
    ss::gate _gate;
    ss::sharded<kafka::client::client>& _client;
    ss::sharded<kafka_client_cache>& _client_cache;
    server::context_t _ctx;
    server _server;
    one_shot _ensure_started;
    bool _is_started{false};
};

} // namespace pandaproxy::rest
