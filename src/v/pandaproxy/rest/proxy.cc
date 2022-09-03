// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/rest/proxy.h"

#include "pandaproxy/api/api-doc/rest.json.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/rest/configuration.h"
#include "pandaproxy/rest/handlers.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/memory.hh>
#include <seastar/http/api_docs.hh>

namespace pandaproxy::rest {

using server = ctx_server<proxy>;

server::routes_t get_proxy_routes() {
    server::routes_t routes;
    routes.api = ss::httpd::rest_json::name;

    routes.routes.emplace_back(
      server::route_t{ss::httpd::rest_json::get_brokers, get_brokers});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::get_topics_names, get_topics_names});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::get_topics_records, get_topics_records});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::post_topics_name, post_topics_name});

    routes.routes.emplace_back(
      server::route_t{ss::httpd::rest_json::create_consumer, create_consumer});

    routes.routes.emplace_back(
      server::route_t{ss::httpd::rest_json::remove_consumer, remove_consumer});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::subscribe_consumer, subscribe_consumer});

    routes.routes.emplace_back(
      server::route_t{ss::httpd::rest_json::consumer_fetch, consumer_fetch});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::get_consumer_offsets, get_consumer_offsets});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::post_consumer_offsets, post_consumer_offsets});

    return routes;
}

proxy::proxy(
  const YAML::Node& config,
  ss::smp_service_group smp_sg,
  size_t max_memory,
  ss::sharded<kafka::client::client>& client)
  : _config(config)
  , _mem_sem(max_memory, "pproxy/mem")
  , _client(client)
  , _ctx{{{}, _mem_sem, {}, smp_sg}, *this}
  , _server(
      "pandaproxy",
      "rest_proxy",
      ss::api_registry_builder20(_config.api_doc_dir(), "/v1"),
      "header",
      "/definitions",
      _ctx,
      json::serialization_format::application_json) {}

ss::future<> proxy::start() {
    _server.routes(get_proxy_routes());
    return _server.start(
      _config.pandaproxy_api(),
      _config.pandaproxy_api_tls(),
      _config.advertised_pandaproxy_api());
}

ss::future<> proxy::stop() { return _server.stop(); }

configuration& proxy::config() { return _config; }

kafka::client::configuration& proxy::client_config() {
    return _client.local().config();
}

} // namespace pandaproxy::rest
