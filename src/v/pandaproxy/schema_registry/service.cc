// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/service.h"

#include "pandaproxy/api/api-doc/schema_registry.json.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/configuration.h"
#include "pandaproxy/schema_registry/handlers.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/memory.hh>
#include <seastar/http/api_docs.hh>

namespace pandaproxy::schema_registry {

server::routes_t get_schema_registry_routes() {
    server::routes_t routes;
    routes.api = ss::httpd::schema_registry_json::name;

    routes.routes.emplace_back(server::route_t{
      ss::httpd::schema_registry_json::get_schemas_types, get_schemas_types});

    return routes;
}

static context_t make_context(
  const config::config_store& cfg,
  ss::smp_service_group smp_sg,
  ss::semaphore& mem_sem,
  ss::sharded<kafka::client::client>& client) {
    return context_t{
      .mem_sem{mem_sem},
      .as{},
      .smp_sg = smp_sg,
      .client{client},
      .config{cfg}};
}

service::service(
  const YAML::Node& config,
  ss::smp_service_group smp_sg,
  size_t max_memory,
  ss::sharded<kafka::client::client>& client)
  : _config(config)
  , _smp_sg(smp_sg)
  , _mem_sem(max_memory)
  , _client(client)
  , _ctx(make_context(_config, _smp_sg, _mem_sem, _client))
  , _server(
      "schema_registry",
      ss::api_registry_builder20(_config.api_doc_dir(), "/v1"),
      "schema_registry_header",
      "/definitions",
      make_context(_config, _smp_sg, _mem_sem, _client)) {}

ss::future<> service::start() {
    static std::vector<model::broker_endpoint> not_advertised{};
    _server.routes(get_schema_registry_routes());
    return _server.start(
      _config.schema_registry_api(),
      _config.schema_registry_api_tls(),
      not_advertised);
}

ss::future<> service::stop() { return _server.stop(); }

configuration& service::config() { return _config; }

kafka::client::configuration& service::client_config() {
    return _client.local().config();
}

} // namespace pandaproxy::schema_registry
