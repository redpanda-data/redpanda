// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/rest/proxy.h"

#include "config/rest_authn_endpoint.h"
#include "net/unresolved_address.h"
#include "pandaproxy/api/api-doc/rest.json.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/parsing/exceptions.h"
#include "pandaproxy/parsing/from_chars.h"
#include "pandaproxy/rest/configuration.h"
#include "pandaproxy/rest/handlers.h"
#include "pandaproxy/sharded_client_cache.h"
#include "pandaproxy/types.h"
#include "utils/gate_guard.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/memory.hh>
#include <seastar/http/api_docs.hh>

namespace pandaproxy::rest {

using server = ctx_server<proxy>;

template<typename Handler>
auto wrap(Handler h) {
    return [h{std::move(h)}](
             server::request_t rq,
             server::reply_t rp) -> ss::future<server::reply_t> {
        rq.authn_method = rq.service().config().authn_method(
          rq.req->get_listener_idx());

        if (rq.authn_method == config::rest_authn_method::http_basic) {
            // Will throw 400 & 401 if auth fails
            auto auth_result = rq.service().authenticator().authenticate(
              *rq.req);
            // Will throw 403 if user enabled HTTP Basic Auth but
            // did not give the authorization header.
            auth_result.require_authenticated();
            rq.user = credential_t{
              auth_result.get_username(), auth_result.get_password()};
        }

        return h(std::move(rq), std::move(rp));
    };
}

server::routes_t get_proxy_routes() {
    server::routes_t routes;
    routes.api = ss::httpd::rest_json::name;

    routes.routes.emplace_back(
      server::route_t{ss::httpd::rest_json::get_brokers, wrap(get_brokers)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::get_topics_names, wrap(get_topics_names)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::get_topics_records, wrap(get_topics_records)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::post_topics_name, wrap(post_topics_name)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::create_consumer, wrap(create_consumer)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::remove_consumer, wrap(remove_consumer)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::subscribe_consumer, wrap(subscribe_consumer)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::consumer_fetch, wrap(consumer_fetch)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::get_consumer_offsets, wrap(get_consumer_offsets)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::post_consumer_offsets,
      wrap(post_consumer_offsets)});

    return routes;
}

struct always_true : public config::binding<bool> {
    always_true()
      : config::binding<bool>(true) {}
};

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
  , _ctx{{{}, _mem_sem, {}, smp_sg}, *this}
  , _server(
      "pandaproxy",
      "rest_proxy",
      ss::api_registry_builder20(_config.api_doc_dir(), "/v1"),
      "header",
      "/definitions",
      _ctx,
      json::serialization_format::application_json)
  , _auth{always_true(), controller} {}

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
