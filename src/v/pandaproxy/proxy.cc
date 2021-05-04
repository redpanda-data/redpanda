// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/proxy.h"

#include "pandaproxy/api/api-doc/v1.json.h"
#include "pandaproxy/configuration.h"
#include "pandaproxy/handlers.h"
#include "pandaproxy/logger.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/memory.hh>
#include <seastar/http/api_docs.hh>

namespace pandaproxy {

server::routes_t get_proxy_routes() {
    server::routes_t routes;
    routes.api = ss::httpd::v1_json::name;

    routes.routes.emplace_back(
      server::route_t{ss::httpd::v1_json::get_topics_names, get_topics_names});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::v1_json::get_topics_records, get_topics_records});

    routes.routes.emplace_back(
      server::route_t{ss::httpd::v1_json::post_topics_name, post_topics_name});

    routes.routes.emplace_back(
      server::route_t{ss::httpd::v1_json::create_consumer, create_consumer});

    routes.routes.emplace_back(
      server::route_t{ss::httpd::v1_json::remove_consumer, remove_consumer});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::v1_json::subscribe_consumer, subscribe_consumer});

    routes.routes.emplace_back(
      server::route_t{ss::httpd::v1_json::consumer_fetch, consumer_fetch});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::v1_json::get_consumer_offsets, get_consumer_offsets});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::v1_json::post_consumer_offsets, post_consumer_offsets});

    return routes;
}

static context_t make_context(
  const configuration& cfg,
  ss::smp_service_group smp_sg,
  ss::sharded<kafka::client::client>& client) {
    return context_t{
      .mem_sem{ss::memory::stats().free_memory()},
      .as{},
      .smp_sg = smp_sg,
      .client{client},
      .config{cfg}};
}

proxy::proxy(
  const YAML::Node& config,
  ss::smp_service_group smp_sg,
  ss::sharded<kafka::client::client>& client)
  : _config(config)
  , _smp_sg(smp_sg)
  , _client(client)
  , _ctx(make_context(_config, _smp_sg, _client))
  , _server(
      "pandaproxy",
      ss::api_registry_builder20(_config.api_doc_dir(), "/v1"),
      make_context(_config, _smp_sg, _client)) {}

ss::future<> proxy::start() {
    _server.routes(get_proxy_routes());
    return _server.start();
}

ss::future<> proxy::stop() { return _server.stop(); }

pandaproxy::configuration& proxy::config() { return _config; }

kafka::client::configuration& proxy::client_config() {
    return _client.local().config();
}

} // namespace pandaproxy
