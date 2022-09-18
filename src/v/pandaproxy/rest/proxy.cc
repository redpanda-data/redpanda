// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/rest/proxy.h"

#include "config/property.h"
#include "config/rest_authn_endpoint.h"
#include "net/unresolved_address.h"
#include "pandaproxy/api/api-doc/rest.json.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/parsing/exceptions.h"
#include "pandaproxy/parsing/from_chars.h"
#include "pandaproxy/rest/configuration.h"
#include "pandaproxy/rest/handlers.h"
#include "pandaproxy/types.h"
#include "redpanda/request_auth.h"
#include "utils/gate_guard.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/http/api_docs.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/request_parser.hh>

#include <algorithm>
#include <string>

namespace pandaproxy::rest {

namespace {
net::unresolved_address address_from_host(ss::sstring host_and_port) {
    auto colon = host_and_port.find_last_of(':');
    ss::sstring host{host_and_port.substr(0, colon)};
    ss::sstring port_str{host_and_port.substr(colon+1)};

    auto res = parse::from_chars<uint16_t>{}(port_str);
    if (res.has_error()) {
        throw parse::error(
          parse::error_code::not_acceptable, "INT conversion fail");
    }

    return net::unresolved_address{host, res.value()};
}
} // namespace

using server = ctx_server<proxy>;

template<typename Handler>
auto wrap(configuration& cfg, Handler h) {
    return [&cfg, h{std::move(h)}](
             server::request_t rq,
             server::reply_t rp) -> ss::future<server::reply_t> {
        net::unresolved_address req_address{address_from_host(rq.req->get_header("Host"))};
        rq.ctx.authn_type = rq.service().config().authn_type(req_address);

        if (rq.ctx.authn_type == config::rest_authn_type::http_basic) {
            config::property<bool> require_auth{
              cfg,
              "",
              "",
              {.needs_restart = config::needs_restart::no,
               .visibility = config::visibility::user},
              true};
            request_authenticator auth{
              require_auth.bind(), rq.service().controller()};

            // Will throw if auth fails
            // The catch statements in reply.h will do error handling
            auto auth_result = auth.authenticate(*rq.req);
            auth_result.pass();
            rq.ctx.user = credential_t{
              auth_result.get_username(), auth_result.get_password()};
        }

        co_return co_await h(std::move(rq), std::move(rp));
    };
}

server::routes_t get_proxy_routes(configuration& cfg) {
    server::routes_t routes;
    routes.api = ss::httpd::rest_json::name;

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::get_brokers, wrap(cfg, get_brokers)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::get_topics_names, wrap(cfg, get_topics_names)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::get_topics_records, wrap(cfg, get_topics_records)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::post_topics_name, wrap(cfg, post_topics_name)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::create_consumer, wrap(cfg, create_consumer)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::remove_consumer, wrap(cfg, remove_consumer)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::subscribe_consumer, wrap(cfg, subscribe_consumer)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::consumer_fetch, wrap(cfg, consumer_fetch)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::get_consumer_offsets,
      wrap(cfg, get_consumer_offsets)});

    routes.routes.emplace_back(server::route_t{
      ss::httpd::rest_json::post_consumer_offsets,
      wrap(cfg, post_consumer_offsets)});

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
  , _controller(controller)
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
    _server.routes(get_proxy_routes(_config));
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
