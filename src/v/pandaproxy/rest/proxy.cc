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
#include "net/unresolved_address.h"
#include "pandaproxy/api/api-doc/rest.json.h"
#include "pandaproxy/auth_utils.h"
#include "pandaproxy/config_utils.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/parsing/exceptions.h"
#include "pandaproxy/parsing/from_chars.h"
#include "pandaproxy/rest/configuration.h"
#include "pandaproxy/rest/handlers.h"
#include "pandaproxy/sharded_client_cache.h"
#include "security/ephemeral_credential_store.h"
#include "utils/gate_guard.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/memory.hh>
#include <seastar/http/api_docs.hh>

namespace pandaproxy::rest {

using server = proxy::server;

const security::acl_principal principal{
  security::principal_type::ephemeral_user, "__pandaproxy"};

template<typename Handler>
auto wrap(ss::gate& g, one_shot& os, Handler h) {
    return [&g, &os, _h{std::move(h)}](
             server::request_t rq,
             server::reply_t rp) -> ss::future<server::reply_t> {
        auto h{_h};

        auto units = co_await os();
        auto guard = gate_guard(g);
        co_return co_await h(std::move(rq), std::move(rp));
    };
}

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

    return routes;
}

proxy::proxy(
  const YAML::Node& config,
  ss::smp_service_group smp_sg,
  size_t max_memory,
  ss::sharded<kafka::client::client>& client,
  sharded_client_cache& client_cache,
  cluster::controller* controller)
  : _config(config)
  , _mem_sem(max_memory, "pproxy/mem")
  , _client(client)
  , _client_cache(client_cache)
  , _ctx{{{{}, _mem_sem, {}, smp_sg}, *this}, {config::always_true(), controller}, _config.pandaproxy_api.value()}
  , _server(
      "pandaproxy",
      "rest_proxy",
      ss::api_registry_builder20(_config.api_doc_dir(), "/v1"),
      "header",
      "/definitions",
      _ctx,
      json::serialization_format::application_json)
  , _ensure_started{[this]() { return do_start(); }}
  , _controller(controller) {}

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
    auto guard = gate_guard(_gate);
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
}

ss::future<> proxy::configure() {
    auto config = co_await pandaproxy::create_client_credentials(
      *_controller,
      config::shard_local_cfg(),
      _client.local().config(),
      principal);
    co_await set_client_credentials(*config, _client);

    auto const& store = _controller->get_ephemeral_credential_store().local();
    _has_ephemeral_credentials = store.has(store.find(principal));

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
    vlog(plog.debug, "mitigate_error: {}", eptr);
    return ss::make_exception_future<>(eptr).handle_exception_type(
      [this, eptr](kafka::client::broker_error const& ex) {
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
    const auto do_inform = [this](model::node_id n) -> ss::future<> {
        auto& fe = _controller->get_ephemeral_credential_frontend().local();
        auto ec = co_await fe.inform(n, principal);
        vlog(plog.info, "Informed: broker: {}, ec: {}", n, ec);
    };

    // Inform a particular node
    if (id != kafka::client::unknown_node_id) {
        co_return co_await do_inform(id);
    }

    // Inform all nodes
    co_await seastar::parallel_for_each(
      _controller->get_members_table().local().all_broker_ids(),
      [do_inform](model::node_id n) { return do_inform(n); });
}

} // namespace pandaproxy::rest
