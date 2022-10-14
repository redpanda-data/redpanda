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
#include "ssx/future-util.h"

using namespace std::chrono_literals;

namespace pandaproxy {

static constexpr auto clean_timer_period = 10s;

ss::future<> sharded_client_cache::start(
  ss::smp_service_group sg,
  YAML::Node const& proxy_client_cfg,
  size_t client_cache_max_size,
  std::chrono::milliseconds client_keep_alive) {
    _smp_opts = ss::smp_submit_to_options{sg};

    _clean_timer.set_callback([this] {
        ssx::spawn_with_gate(_gate, [this] {
            return _cache
              .invoke_on_all(
                _smp_opts,
                [](kafka_client_cache& cache) {
                    return cache.clean_stale_clients();
                })
              .finally([this] {
                  if (!_gate.is_closed()) {
                      _clean_timer.arm(clean_timer_period);
                  }
              });
        });
    });

    return _cache
      .start(
        proxy_client_cfg,
        client_cache_max_size,
        client_keep_alive,
        std::reference_wrapper(_clean_timer))
      .then([this] { _clean_timer.arm(clean_timer_period); });
}

ss::future<> sharded_client_cache::stop() {
    _clean_timer.cancel();
    co_await _gate.close();
    co_await _cache.stop();
}

} // namespace pandaproxy
