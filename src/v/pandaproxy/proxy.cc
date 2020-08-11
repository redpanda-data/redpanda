#include "pandaproxy/proxy.h"

#include "pandaproxy/api/api-doc/health.json.h"
#include "pandaproxy/api/api-doc/topics.json.h"
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
      "topics", ss::httpd::topics_json::get_topics_names, get_topics_names});

    return routes;
}

static server::context_t make_context(kafka::client& client) {
    return server::context_t{ss::memory::stats().free_memory(), client};
}

proxy::proxy(
  ss::socket_address listen_addr, std::vector<ss::socket_address> broker_addrs)
  : _client(rpc::base_transport::configuration{
    .server_addr = broker_addrs[0], // TODO: Support multiple brokers
  })
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
