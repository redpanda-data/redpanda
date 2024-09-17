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
#include "cluster/ephemeral_credential_frontend.h"
#include "cluster/members_table.h"
#include "cluster/security_frontend.h"
#include "config/configuration.h"
#include "kafka/client/config_utils.h"
#include "pandaproxy/api/api-doc/rest.json.hh"
#include "pandaproxy/logger.h"
#include "pandaproxy/rest/configuration.h"
#include "pandaproxy/rest/handlers.h"
#include "security/ephemeral_credential_store.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/memory.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/http/api_docs.hh>

namespace pandaproxy::rest {

using server = proxy::server;

const security::acl_principal principal{
  security::principal_type::ephemeral_user, "__pandaproxy"};

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

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::get_brokers, wrap(gate, es, get_brokers)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::get_topics_names,
      wrap(gate, es, get_topics_names)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::get_topics_records,
      wrap(gate, es, get_topics_records)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::post_topics_name,
      wrap(gate, es, post_topics_name)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::create_consumer, wrap(gate, es, create_consumer)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::remove_consumer, wrap(gate, es, remove_consumer)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::subscribe_consumer,
      wrap(gate, es, subscribe_consumer)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::consumer_fetch, wrap(gate, es, consumer_fetch)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::get_consumer_offsets,
      wrap(gate, es, get_consumer_offsets)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::post_consumer_offsets,
      wrap(gate, es, post_consumer_offsets)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::http_rest_status_ready,
      wrap(gate, es, status_ready)});

    return routes;
}

proxy::proxy(
  const YAML::Node& config,
  ss::smp_service_group smp_sg,
  size_t max_memory,
  ss::sharded<kafka::client::client>& client,
  ss::sharded<kafka_client_cache>& client_cache,
  cluster::controller* controller)
  : _config(config)
  , _mem_sem(max_memory, "pproxy/mem")
  , _inflight_sem(config::shard_local_cfg().max_in_flight_pandaproxy_requests_per_shard(), "pproxy/inflight")
  , _inflight_config_binding(config::shard_local_cfg().max_in_flight_pandaproxy_requests_per_shard.bind())
  , _client(client)
  , _client_cache(client_cache)
  , _ctx{{{{}, _mem_sem, _inflight_sem, {}, smp_sg}, *this},
        {config::always_true(), config::shard_local_cfg().superusers.bind(), controller},
        _config.pandaproxy_api.value()}
  , _server(
      "pandaproxy",
      "rest_proxy",
      ss::httpd::api_registry_builder20(_config.api_doc_dir(), "/v1"),
      "header",
      "/definitions",
      _ctx,
      json::serialization_format::application_json)
  , _ensure_started{[this]() { return do_start(); }}
  , _controller(controller) {
    _inflight_config_binding.watch(
      [this]() { _inflight_sem.set_capacity(_inflight_config_binding()); });
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

kafka::client::configuration& proxy::client_config() {
    return _client.local().config();
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
    auto config = co_await kafka::client::create_client_credentials(
      *_controller,
      config::shard_local_cfg(),
      _client.local().config(),
      principal);
    co_await kafka::client::set_client_credentials(*config, _client);

    const auto& store = _controller->get_ephemeral_credential_store().local();
    bool has_ephemeral_credentials = store.has(store.find(principal));
    co_await container().invoke_on_all(
      _ctx.smp_sg, [has_ephemeral_credentials](proxy& p) {
          p._has_ephemeral_credentials = has_ephemeral_credentials;
      });

    security::acl_entry acl_entry{
      principal,
      security::acl_host::wildcard_host(),
      security::acl_operation::all,
      security::acl_permission::allow};

    co_await _controller->get_security_frontend().local().create_acls(
      {security::acl_binding{
         security::resource_pattern{
           security::resource_type::topic,
           security::resource_pattern::wildcard,
           security::pattern_type::literal},
         acl_entry},
       security::acl_binding{
         security::resource_pattern{
           security::resource_type::group,
           security::resource_pattern::wildcard,
           security::pattern_type::literal},
         acl_entry}},
      5s);
}

ss::future<> proxy::mitigate_error(std::exception_ptr eptr) {
    if (_gate.is_closed()) {
        // Return so that the client doesn't try to mitigate.
        return ss::now();
    }
    vlog(plog.debug, "mitigate_error: {}", eptr);
    return ss::make_exception_future<>(eptr).handle_exception_type(
      [this, eptr](const kafka::client::broker_error& ex) {
          if (
            ex.error == kafka::error_code::sasl_authentication_failed
            && _has_ephemeral_credentials) {
              return inform(ex.node_id).then([this]() {
                  // This fully mitigates, don't rethrow.
                  return _client.local().connect();
              });
          }
          // Rethrow unhandled exceptions
          return ss::make_exception_future<>(eptr);
      });
}

ss::future<> proxy::inform(model::node_id id) {
    vlog(plog.trace, "inform: {}", id);

    // Inform a particular node
    if (id != kafka::client::unknown_node_id) {
        return do_inform(id);
    }

    // Inform all nodes
    return seastar::parallel_for_each(
      _controller->get_members_table().local().node_ids(),
      [this](model::node_id id) { return do_inform(id); });
}

ss::future<> proxy::do_inform(model::node_id id) {
    auto& fe = _controller->get_ephemeral_credential_frontend().local();
    auto ec = co_await fe.inform(id, principal);
    vlog(plog.info, "Informed: broker: {}, ec: {}", id, ec);
}

} // namespace pandaproxy::rest
