#include "pandaproxy/proxy.h"

#include "pandaproxy/api/api-doc/get_topics_names.json.h"
#include "pandaproxy/api/api-doc/health.json.h"
#include "pandaproxy/api/api-doc/post_topics_name.json.h"
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
      "get_topics_names",
      ss::httpd::get_topics_names_json::get_topics_names,
      get_topics_names});

    routes.emplace_back(server::route_t{
      "post_topics_name",
      ss::httpd::post_topics_name_json::post_topics_name,
      post_topics_name});

    return routes;
}

static server::context_t make_context(client::client& client) {
    return server::context_t{
      .mem_sem{ss::memory::stats().free_memory()},
      .as{},
      .client{client},
    };
}

proxy::proxy(
  ss::socket_address listen_addr, std::vector<ss::socket_address> broker_addrs)
  : _client(std::move(broker_addrs))
  , _ctx(make_context(_client))
  , _server(
      "pandaproxy",
      listen_addr,
      ss::api_registry_builder20(shard_local_cfg().api_doc_dir(), "/v1"),
      make_context(_client)) {}

ss::future<> proxy::start() {
    return seastar::when_all(
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
