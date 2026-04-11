// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/api.h"

#include "cluster/cluster_link/frontend.h"
#include "cluster/controller.h"
#include "config/configuration.h"
#include "kafka/client/configuration.h"
#include "kafka/data/rpc/deps.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/configuration.h"
#include "pandaproxy/schema_registry/schema_id_cache.h"
#include "pandaproxy/schema_registry/service.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/types.h"
#include "pandaproxy/schema_registry/validation_metrics.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <functional>
#include <memory>

namespace pandaproxy::schema_registry {

class sequence_state_checker_impl : public sequence_state_checker {
public:
    explicit sequence_state_checker_impl(
      std::unique_ptr<cluster::controller>& c)
      : _controller(c) {}

    writes_disabled_t writes_disabled() const final {
        return writes_disabled_t{_controller->get_cluster_link_frontend()
                                   .local()
                                   .schema_registry_shadowing_active()};
    }

private:
    std::unique_ptr<cluster::controller>& _controller;
};

api::api(
  model::node_id node_id,
  ss::smp_service_group sg,
  size_t max_memory,
  kafka::client::configuration& client_cfg,
  configuration& cfg,
  ss::sharded<cluster::metadata_cache>* metadata_cache,
  std::unique_ptr<cluster::controller>& c,
  ss::sharded<security::audit::audit_log_manager>& audit_mgr) noexcept
  : _node_id{node_id}
  , _sg{sg}
  , _max_memory{max_memory}
  , _client_cfg{client_cfg}
  , _cfg{cfg}
  , _metadata_cache(metadata_cache)
  , _controller(c)
  , _audit_mgr(audit_mgr) {}

api::~api() noexcept = default;

ss::future<> api::start() {
    co_await enable_qualified_subjects::initialize(
      config::shard_local_cfg().schema_registry_enable_qualified_subjects());
    vlog(
      srlog.info,
      "Qualified subject parsing enabled: {}",
      enable_qualified_subjects::get());

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
      _node_id,
      _sg,
      std::ref(_client),
      std::ref(*_store),
      ss::sharded_parameter([this] {
          return std::make_unique<sequence_state_checker_impl>(_controller);
      }));
    co_await _service.start(
      config::to_yaml(_cfg, config::redact_secrets::no),
      config::to_yaml(_client_cfg, config::redact_secrets::no),
      _sg,
      _max_memory,
      std::ref(_client),
      std::ref(*_store),
      std::ref(_sequencer),
      ss::sharded_parameter([this]() {
          return kafka::data::rpc::topic_metadata_cache::make_default(
            _metadata_cache);
      }),
      ss::sharded_parameter([this]() {
          return kafka::data::rpc::topic_creator::make_default(
            _controller.get());
      }),
      std::ref(_controller),
      std::ref(_audit_mgr));

    co_await _service.invoke_on_all(&service::start);

    if (ss::this_shard_id() == 0) {
        vassert(
          !_metrics_contributor_id.has_value(),
          "Metrics contributor ID should not be set when starting the API");
        _metrics_contributor_id = _controller->register_metrics_contributor(
          [this](cluster::metrics_reporter::metrics_snapshot& snap) {
              if (_metrics_gate.is_closed()) {
                  vlog(srlog.debug, "Gate already closed, skipping metrics");
                  return ss::now();
              }
              return ss::with_gate(_metrics_gate, [this, &snap] {
                  return contribute_metrics(snap);
              });
          });
    }
}

ss::future<> api::stop() {
    vlog(srlog.debug, "Stopping schema registry API...");
    if (ss::this_shard_id() == 0 && _metrics_contributor_id.has_value()) {
        _controller->unregister_metrics_contributor(*_metrics_contributor_id);
        _metrics_contributor_id.reset();
        co_await _metrics_gate.close();
        // Reset gate to support api restart
        _metrics_gate = ss::gate{};
    }
    co_await _client.invoke_on_all(&kafka::client::client::stop);
    co_await _service.stop();
    co_await _sequencer.stop();
    co_await _client.stop();
    co_await _schema_id_cache.stop();
    co_await _schema_id_validation_probe.stop();
    if (_store) {
        co_await _store->stop();
    }
    if (enable_qualified_subjects::is_initialized()) {
        co_await enable_qualified_subjects::reset();
    }
    vlog(srlog.debug, "Stopped schema registry API...");
}

ss::future<> api::restart() {
    vlog(srlog.info, "Restarting the schema registry");
    co_await stop();
    co_await start();
}

const configuration& api::get_config() const { return _cfg; }

const kafka::client::configuration& api::get_client_config() const {
    return _client_cfg;
}

bool api::has_ephemeral_credentials() const {
    return _service.local().has_ephemeral_credentials();
}

ss::future<> api::contribute_metrics(
  cluster::metrics_reporter::metrics_snapshot& snap) const {
    if (!_store) {
        vlog(
          srlog.debug,
          "Schema registry store not initialized, skipping metrics");
        co_return;
    }
    auto ctxs = co_await _store->get_materialized_contexts();
    auto count = std::ranges::count_if(
      ctxs, [](const context& c) { return c != default_context; });
    snap.schema_registry = cluster::metrics_reporter::schema_registry_metrics{
      .context_count = static_cast<uint32_t>(count),
    };
}
} // namespace pandaproxy::schema_registry
