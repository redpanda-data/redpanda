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
#include "features/feature_table.h"
#include "kafka/client/configuration.h"
#include "kafka/data/rpc/client.h"
#include "kafka/data/rpc/deps.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/configuration.h"
#include "pandaproxy/schema_registry/kafka_client_transport.h"
#include "pandaproxy/schema_registry/rpc_transport.h"
#include "pandaproxy/schema_registry/schema_id_cache.h"
#include "pandaproxy/schema_registry/seq_writer.h"
#include "pandaproxy/schema_registry/service.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/types.h"
#include "pandaproxy/schema_registry/validation_metrics.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <functional>
#include <memory>
#include <variant>

namespace pandaproxy::schema_registry {

struct api::transport_impl {
    transport_impl(
      ss::sharded<kafka::data::rpc::client>* rpc_client,
      kafka::client::configuration& client_cfg,
      cluster::controller& controller)
      : _rpc_client(rpc_client)
      , _client_cfg(client_cfg)
      , _controller(controller) {
        if (!should_use_rpc(rpc_client, controller)) {
            _v.emplace<ss::sharded<kafka_client_transport>>();
        }
        // else: _v default-constructs to sharded<rpc_transport>.
        vlog(
          srlog.info,
          "Schema registry in {} mode",
          std::holds_alternative<ss::sharded<rpc_transport>>(_v)
            ? "RPC"
            : "Kafka client");
    }

    /// Decide whether to use the RPC transport at startup. Falls back to
    /// the kafka::client transport (with a warning) if the config asks for
    /// RPC but preconditions aren't met.
    static bool should_use_rpc(
      const ss::sharded<kafka::data::rpc::client>* rpc_client,
      cluster::controller& controller) {
        if (!config::shard_local_cfg().schema_registry_use_rpc()) {
            return false;
        }
        if (!rpc_client) {
            vlog(
              srlog.info,
              "schema_registry_use_rpc enabled but RPC client not available. "
              "Falling back to Kafka client.");
            return false;
        }
        const bool rpc_available
          = controller.get_feature_table().local().get_active_version()
            >= features::to_cluster_version(features::release_version::v26_2_1);
        if (!rpc_available) {
            vlog(
              srlog.info,
              "schema_registry_use_rpc enabled but cluster version too old. "
              "Falling back to Kafka client. RPC mode will be available "
              "on the next restart after all brokers are upgraded.");
            return false;
        }
        return true;
    }

    ss::future<> start() {
        return ss::visit(
          _v,
          [this](ss::sharded<rpc_transport>& t) {
              return t.start(ss::sharded_parameter([this] {
                  return std::ref(_rpc_client->local());
              }));
          },
          [this](ss::sharded<kafka_client_transport>& t) {
              return t.start(
                std::ref(_client_cfg),
                std::ref(_controller),
                ss::sharded_parameter([this] {
                    return kafka::data::rpc::topic_creator::make_default(
                      &_controller);
                }));
          });
    }

    /// Only kafka_client_transport has per-shard credentials to load; the
    /// RPC path needs no configuration.
    ss::future<> configure() {
        return ss::visit(
          _v,
          [](ss::sharded<rpc_transport>&) { return ss::now(); },
          [](ss::sharded<kafka_client_transport>& t) {
              return t.invoke_on_all(&kafka_client_transport::configure);
          });
    }

    ss::future<> invoke_stop_on_all() {
        return ss::visit(_v, [](auto& t) {
            return t.invoke_on_all([](auto& local) { return local.stop(); });
        });
    }

    ss::future<> stop() {
        return ss::visit(_v, [](auto& t) { return t.stop(); });
    }

    transport& local() {
        return ss::visit(_v, [](auto& t) -> transport& { return t.local(); });
    }

    bool has_ephemeral_credentials() const {
        return ss::visit(
          _v,
          [](const ss::sharded<rpc_transport>&) { return false; },
          [](const ss::sharded<kafka_client_transport>& t) {
              return t.local().has_ephemeral_credentials();
          });
    }

private:
    std::
      variant<ss::sharded<rpc_transport>, ss::sharded<kafka_client_transport>>
        _v;
    ss::sharded<kafka::data::rpc::client>* _rpc_client;
    kafka::client::configuration& _client_cfg;
    cluster::controller& _controller;
};

class sequence_state_checker_impl : public sequence_state_checker {
public:
    explicit sequence_state_checker_impl(
      std::unique_ptr<cluster::controller>& c)
      : _controller(c) {}

    writes_disabled_t
    writes_disabled(write_source source, const context& ctx) const final {
        auto& frontend = _controller->get_cluster_link_frontend().local();
        switch (source) {
        case write_source::client:
            return writes_disabled_t{
              frontend.schema_registry_client_writes_disabled(ctx())};
        case write_source::schema_registry_sync:
            return writes_disabled_t{
              frontend.schema_registry_local_topic_writes_disabled()};
        }
        vunreachable("Unhandled write_source: {}", static_cast<int>(source));
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
  ss::sharded<security::audit::audit_log_manager>& audit_mgr,
  ss::sharded<kafka::data::rpc::client>* rpc_client) noexcept
  : _node_id{node_id}
  , _sg{sg}
  , _max_memory{max_memory}
  , _client_cfg{client_cfg}
  , _cfg{cfg}
  , _metadata_cache(metadata_cache)
  , _controller(c)
  , _audit_mgr(audit_mgr)
  , _rpc_client(rpc_client) {}

api::~api() noexcept = default;

ss::future<> api::start() {
    vlog(
      srlog.info,
      "Qualified subject parsing enabled: {}",
      config::shard_local_cfg().schema_registry_enable_qualified_subjects());

    _store = std::make_unique<sharded_store>();
    co_await _store->start(is_mutable(_cfg.mode_mutability), _sg);
    co_await _schema_id_validation_probe.start();
    co_await _schema_id_validation_probe.invoke_on_all(
      &schema_id_validation_probe::setup_metrics);
    co_await _schema_id_cache.start(ss::sharded_parameter([] {
        return config::shard_local_cfg()
          .kafka_schema_id_validation_cache_capacity.bind();
    }));
    // Build the transport locally and only install it into _transport
    // on successful start. If start() throws, the local impl unwinds and
    // the partially-initialized sharded<> goes with it.
    auto impl = std::make_unique<transport_impl>(
      _rpc_client, _client_cfg, *_controller);
    co_await impl->start();
    _transport = std::move(impl);

    co_await _sequencer.start(
      _node_id,
      _sg,
      ss::sharded_parameter([this] { return std::ref(_transport->local()); }),
      std::ref(*_store),
      ss::sharded_parameter([this] {
          return std::make_unique<sequence_state_checker_impl>(_controller);
      }));
    co_await _service.start(
      config::to_yaml(_cfg, config::redact_secrets::no),
      _sg,
      _max_memory,
      ss::sharded_parameter([this] { return std::ref(_transport->local()); }),
      std::ref(*_store),
      std::ref(_sequencer),
      ss::sharded_parameter([this]() {
          return kafka::data::rpc::topic_metadata_cache::make_default(
            _metadata_cache);
      }),
      std::ref(_controller),
      std::ref(_audit_mgr));

    co_await _transport->configure();
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

ss::future<> api::stop_clients() {
    co_await _client.invoke_on_all(&kafka::client::client::stop);
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
    if (_transport) {
        co_await _transport->invoke_stop_on_all();
    }
    co_await _service.stop();
    co_await _sequencer.stop();
    if (_transport) {
        co_await _transport->stop();
        _transport.reset();
    }

    co_await _schema_id_cache.stop();
    co_await _schema_id_validation_probe.stop();
    if (_store) {
        co_await _store->stop();
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
    return _transport && _transport->has_ephemeral_credentials();
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
