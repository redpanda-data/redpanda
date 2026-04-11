// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/rest/proxy.h"

#include "cluster/controller.h"
#include "config/configuration.h"
#include "kafka/client/config_utils.h"
#include "kafka/client/configuration.h"
#include "pandaproxy/api/api-doc/rest.json.hh"
#include "pandaproxy/logger.h"
#include "pandaproxy/rest/configuration.h"
#include "pandaproxy/rest/handlers.h"
#include "pandaproxy/rest/iceberg_handlers.h"
#include "security/authorizer.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/memory.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/http/api_docs.hh>

namespace pandaproxy::rest {

using server = proxy::server;

class wrap {
public:
    wrap(ss::gate& g, one_shot& os, server::function_handler h)
      : _g{g}
      , _os{os}
      , _h{std::move(h)} {}

    ss::future<server::reply_t>
    operator()(server::request_t rq, server::reply_t rp) const {
        co_await _os();
        auto guard = _g.hold();
        co_return co_await _h(std::move(rq), std::move(rp));
    }

private:
    ss::gate& _g;
    one_shot& _os;
    server::function_handler _h;
};

server::routes_t get_proxy_routes(ss::gate& gate, one_shot& es) {
    server::routes_t routes;
    routes.api = ss::httpd::rest_json::name;

    routes.routes.emplace_back(
      server::route_t{
        ss::httpd::rest_json::get_brokers, wrap(gate, es, get_brokers)});

    routes.routes.emplace_back(
      server::route_t{
        ss::httpd::rest_json::get_topics_names,
        wrap(gate, es, get_topics_names)});

    routes.routes.emplace_back(
      server::route_t{
        ss::httpd::rest_json::get_topics_records,
        wrap(gate, es, get_topics_records)});

    routes.routes.emplace_back(
      server::route_t{
        ss::httpd::rest_json::post_topics_name,
        wrap(gate, es, post_topics_name)});

    routes.routes.emplace_back(
      server::route_t{
        ss::httpd::rest_json::create_consumer,
        wrap(gate, es, create_consumer)});

    routes.routes.emplace_back(
      server::route_t{
        ss::httpd::rest_json::remove_consumer,
        wrap(gate, es, remove_consumer)});

    routes.routes.emplace_back(
      server::route_t{
        ss::httpd::rest_json::subscribe_consumer,
        wrap(gate, es, subscribe_consumer)});

    routes.routes.emplace_back(
      server::route_t{
        ss::httpd::rest_json::consumer_fetch, wrap(gate, es, consumer_fetch)});

    routes.routes.emplace_back(
      server::route_t{
        ss::httpd::rest_json::get_consumer_offsets,
        wrap(gate, es, get_consumer_offsets)});

    routes.routes.emplace_back(
      server::route_t{
        ss::httpd::rest_json::post_consumer_offsets,
        wrap(gate, es, post_consumer_offsets)});

    routes.routes.emplace_back(
      server::route_t{
        ss::httpd::rest_json::http_rest_status_ready,
        wrap(gate, es, status_ready)});

    routes.routes.emplace_back(
      server::route_t{
        ss::httpd::rest_json::get_translation_state,
        wrap(gate, es, get_translation_state)});

    return routes;
}

proxy::proxy(
  const YAML::Node& config,
  const YAML::Node& client_cfg,
  ss::smp_service_group smp_sg,
  size_t max_memory,
  ss::sharded<kafka::client::client>& client,
  ss::sharded<kafka_client_cache>& client_cache,
  cluster::controller* controller,
  ss::sharded<datalake::coordinator::frontend>& dl_frontend)
  : _config(config)
  , _client_cfg(client_cfg)
  , _mem_sem(max_memory, "pproxy/mem")
  , _inflight_sem(config::shard_local_cfg().max_in_flight_pandaproxy_requests_per_shard(), "pproxy/inflight")
  , _inflight_config_binding(config::shard_local_cfg().max_in_flight_pandaproxy_requests_per_shard.bind())
  , _client(client)
  , _client_cache(client_cache)
  , _controller(controller)
  , _dl_frontend(dl_frontend)
  , _ctx{{{{}, max_memory, _mem_sem, _inflight_config_binding(), _inflight_sem, {}, smp_sg}, *this},
  {config::always_true(), config::shard_local_cfg().superusers.bind(), controller},
  _config.pandaproxy_api.value()}
  , _topic_table(controller->get_topics_state())
  , _server(
      "pandaproxy",
      "rest_proxy",
      ss::httpd::api_registry_builder20(_config.api_doc_dir(), "/v1"),
      "header",
      "/definitions",
      _ctx,
      json::serialization_format::application_json,
      plog,
      preqs)
  , _ensure_started{[this]() { return do_start(); }} {
    _inflight_config_binding.watch([this]() {
        const size_t capacity = _inflight_config_binding();
        _inflight_sem.set_capacity(capacity);
        _ctx.max_inflight = capacity;
    });
}

ss::future<> proxy::start() {
    _server.routes(get_proxy_routes(_gate, _ensure_started));
    return _server.start(
      _config.pandaproxy_api(),
      _config.pandaproxy_api_tls(),
      _config.advertised_pandaproxy_api());
}

ss::future<> proxy::stop() {
    co_await _gate.close();
    co_await _server.stop();
}

configuration& proxy::config() { return _config; }
const configuration& proxy::config() const { return _config; }

security::authorizer& proxy::authorizer() {
    return _controller->get_authorizer().local();
}

ss::future<> proxy::do_start() {
    if (_is_started) {
        co_return;
    }
    auto guard = _gate.hold();
    try {
        co_await configure();
        vlog(plog.info, "Pandaproxy successfully initialized");
    } catch (...) {
        vlog(
          plog.error,
          "Pandaproxy failed to initialize: {}",
          std::current_exception());
        throw;
    }
    co_await container().invoke_on_all(
      _ctx.smp_sg, [](proxy& p) { p._is_started = true; });
}

ss::future<> proxy::configure() {
    std::optional<kafka::client::sasl_configuration> sasl_config;
    if (is_scram_configured(_client_cfg)) {
        sasl_config = kafka::client::sasl_configuration{
          .mechanism = _client_cfg.sasl_mechanism(),
          .username = _client_cfg.scram_username(),
          .password = _client_cfg.scram_password()};
    }
    co_await _client.invoke_on_all(
      [sasl_config = std::move(sasl_config)](kafka::client::client& c) {
          c.set_credentials(sasl_config);
      });
}

ss::future<> proxy::mitigate_error(std::exception_ptr eptr) {
    if (_gate.is_closed()) {
        // Return so that the client doesn't try to mitigate.
        return ss::now();
    }
    vlog(plog.debug, "mitigate_error: {}", eptr);
    return ss::make_exception_future<>(eptr);
}

} // namespace pandaproxy::rest
