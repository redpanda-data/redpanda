// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/proxy.h"

#include "pandaproxy/api/api-doc/create_consumer.json.h"
#include "pandaproxy/api/api-doc/get_topics_names.json.h"
#include "pandaproxy/api/api-doc/get_topics_records.json.h"
#include "pandaproxy/api/api-doc/health.json.h"
#include "pandaproxy/api/api-doc/post_topics_name.json.h"
#include "pandaproxy/api/api-doc/subscribe_consumer.json.h"
#include "pandaproxy/configuration.h"
#include "pandaproxy/handlers.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/memory.hh>
#include <seastar/http/api_docs.hh>

namespace pandaproxy {

std::vector<server::route_t> get_proxy_routes() {
    std::vector<server::route_t> routes;

    routes.emplace_back(server::route_t{
      "health",
      ss::httpd::health_json::health_check,
      [](server::request_t, server::reply_t rp) {
          rp.rep->set_status(ss::httpd::reply::status_type::ok);
          return ss::make_ready_future<server::reply_t>(std::move(rp));
      }});

    routes.emplace_back(server::route_t{
      ss::httpd::get_topics_names_json::name,
      ss::httpd::get_topics_names_json::get_topics_names,
      get_topics_names});

    routes.emplace_back(server::route_t{
      ss::httpd::get_topics_records_json::name,
      ss::httpd::get_topics_records_json::get_topics_records,
      get_topics_records});

    routes.emplace_back(server::route_t{
      ss::httpd::post_topics_name_json::name,
      ss::httpd::post_topics_name_json::post_topics_name,
      post_topics_name});

    routes.emplace_back(server::route_t{
      ss::httpd::create_consumer_json::name,
      ss::httpd::create_consumer_json::create_consumer,
      create_consumer});

    routes.emplace_back(server::route_t{
      ss::httpd::subscribe_consumer_json::name,
      ss::httpd::subscribe_consumer_json::subscribe_consumer,
      subscribe_consumer});

    return routes;
}

static server::context_t make_context(kafka::client::client& client) {
    return server::context_t{
      .mem_sem{ss::memory::stats().free_memory()},
      .as{},
      .client{client},
    };
}

proxy::proxy(
  ss::socket_address listen_addr, std::vector<unresolved_address> broker_addrs)
  : _client(std::move(broker_addrs))
  , _ctx(make_context(_client))
  , _server(
      "pandaproxy",
      listen_addr,
      ss::api_registry_builder20(shard_local_cfg().api_doc_dir(), "/v1"),
      make_context(_client)) {}

ss::future<> proxy::start() {
    return seastar::when_all_succeed(
             [this]() {
                 _server.route(get_proxy_routes());
                 return _server.start();
             },
             [this]() { return _client.connect(); })
      .discard_result();
}

ss::future<> proxy::stop() {
    return _server.stop().finally([this]() { return _client.stop(); });
}

} // namespace pandaproxy
