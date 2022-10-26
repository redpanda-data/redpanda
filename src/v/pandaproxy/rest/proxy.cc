// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/rest/proxy.h"

#include "net/unresolved_address.h"
#include "pandaproxy/api/api-doc/rest.json.h"
#include "pandaproxy/auth_utils.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/parsing/exceptions.h"
#include "pandaproxy/parsing/from_chars.h"
#include "pandaproxy/rest/configuration.h"
#include "pandaproxy/rest/handlers.h"
#include "pandaproxy/sharded_client_cache.h"
#include "utils/gate_guard.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/memory.hh>
#include <seastar/http/api_docs.hh>

namespace pandaproxy::rest {

using server = proxy::server;

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
  , _ensure_started{[this]() { return do_start(); }} {}

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

ss::future<> proxy::do_start() { return ss::now(); }

} // namespace pandaproxy::rest
