// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/api.h"

#include "config/configuration.h"
#include "kafka/client/configuration.h"
#include "model/metadata.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/configuration.h"
#include "pandaproxy/schema_registry/schema_id_cache.h"
#include "pandaproxy/schema_registry/service.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/validation_metrics.h"

#include <seastar/core/coroutine.hh>

#include <functional>
#include <memory>

namespace pandaproxy::schema_registry {
api::api(
  model::node_id node_id,
  ss::smp_service_group sg,
  size_t max_memory,
  kafka::client::configuration& client_cfg,
  configuration& cfg,
  std::unique_ptr<cluster::controller>& c,
  ss::sharded<security::audit::audit_log_manager>& audit_mgr) noexcept
  : _node_id{node_id}
  , _sg{sg}
  , _max_memory{max_memory}
  , _client_cfg{client_cfg}
  , _cfg{cfg}
  , _controller(c)
  , _audit_mgr(audit_mgr) {}

api::~api() noexcept = default;

ss::future<> api::start() {
    _store = std::make_unique<sharded_store>();
    co_await _store->start(is_mutable(_cfg.mode_mutability), _sg);
    co_await _schema_id_validation_probe.start();
    co_await _schema_id_validation_probe.invoke_on_all(
      &schema_id_validation_probe::setup_metrics);
    co_await _schema_id_cache.start(ss::sharded_parameter([] {
        return config::shard_local_cfg()
          .kafka_schema_id_validation_cache_capacity.bind();
    }));
    co_await _client.start(
      config::to_yaml(_client_cfg, config::redact_secrets::no),
      [this](std::exception_ptr ex) {
          return _service.local().mitigate_error(ex);
      });
    co_await _sequencer.start(
      _node_id, _sg, std::ref(_client), std::ref(*_store));
    co_await _service.start(
      config::to_yaml(_cfg, config::redact_secrets::no),
      _sg,
      _max_memory,
      std::ref(_client),
      std::ref(*_store),
      std::ref(_sequencer),
      std::ref(_controller),
      std::ref(_audit_mgr));

    co_await _service.invoke_on_all(&service::start);
}

ss::future<> api::stop() {
    co_await _service.stop();
    co_await _sequencer.stop();
    co_await _client.stop();
    co_await _schema_id_cache.stop();
    co_await _schema_id_validation_probe.stop();
    if (_store) {
        co_await _store->stop();
    }
}

ss::future<> api::restart() {
    vlog(plog.info, "Restarting the schema registry");
    co_await stop();
    co_await start();
}

} // namespace pandaproxy::schema_registry
