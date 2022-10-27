/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "pandaproxy/sharded_client_cache.h"

#include "pandaproxy/rest/configuration.h"

namespace pandaproxy {

ss::future<> sharded_client_cache::start(
  ss::smp_service_group sg,
  YAML::Node const& proxy_client_cfg,
  size_t client_cache_max_size,
  std::chrono::milliseconds client_keep_alive) {
    _smp_opts = ss::smp_submit_to_options{sg};

    return _cache.start(
      proxy_client_cfg, client_cache_max_size, client_keep_alive);
}

ss::future<> sharded_client_cache::stop() {
    co_await _gate.close();
    co_await _cache.stop();
}

} // namespace pandaproxy
