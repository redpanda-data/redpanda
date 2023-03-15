// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/rest/api.h"

#include "kafka/client/client.h"
#include "kafka/client/configuration.h"
#include "model/metadata.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/rest/configuration.h"
#include "pandaproxy/rest/fwd.h"
#include "pandaproxy/rest/proxy.h"

#include <seastar/core/coroutine.hh>

#include <functional>
#include <memory>

namespace pandaproxy::rest {
api::api(
  ss::smp_service_group sg,
  size_t max_memory,
  kafka::client::configuration& client_cfg,
  configuration& cfg,
  cluster::controller* c) noexcept
  : _sg{sg}
  , _max_memory{max_memory}
  , _client_cfg{client_cfg}
  , _cfg{cfg}
  , _controller(c) {}

api::~api() noexcept = default;

ss::future<> api::start() {
    const auto mitigate_error = [this](std::exception_ptr ex) {
        return _proxy.local().mitigate_error(ex);
    };

    co_await _client.start(
      config::to_yaml(_client_cfg, config::redact_secrets::no), mitigate_error);

    co_await _client_cache.start(
      config::to_yaml(_client_cfg, config::redact_secrets::no),
      _cfg.client_cache_max_size.value(),
      _cfg.client_keep_alive.value());

    co_await _proxy.start(
      config::to_yaml(_cfg, config::redact_secrets::no),
      _sg,
      _max_memory,
      std::ref(_client),
      std::ref(_client_cache),
      _controller);

    co_await _proxy.invoke_on_all(&proxy::start);
}

ss::future<> api::stop() {
    co_await _proxy.stop();
    co_await _client_cache.stop();
    co_await _client.stop();
}

ss::future<> api::restart() {
    vlog(plog.info, "Restarting the http proxy");
    co_await stop();
    co_await start();
}

ss::future<> api::set_config(ss::sstring name, std::any val) {
    return _proxy.invoke_on_all(
      [name{std::move(name)}, val{std::move(val)}](pandaproxy::rest::proxy& p) {
          p.config().get(name).set_value(val);
      });
}

ss::future<> api::set_client_config(ss::sstring name, std::any val) {
    return _proxy.invoke_on_all(
      [name{std::move(name)}, val{std::move(val)}](pandaproxy::rest::proxy& p) {
          p.client_config().get(name).set_value(val);
      });
}
} // namespace pandaproxy::rest
