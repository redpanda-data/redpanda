#pragma once

#include "kafka/client.h"
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/http/api_docs.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/json_path.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/noncopyable_function.hh>

#include <memory>

namespace pandaproxy {

/// \brief wrapper around ss::httpd.
///
/// Server is the basis of middleware component to allow
/// strategies to be composed around request/response,
/// e.g., logging, serialisation, metrics, rate-limiting.
class server {
public:
    struct context_t {
        kafka::client& client;
    };

    struct request_t {
        std::unique_ptr<ss::httpd::request> req;
        context_t& ctx;
        // will contain other extensions passed to user specific handler.
    };

    struct reply_t {
        std::unique_ptr<ss::httpd::reply> rep;
        // will contains other extensions passed to user specific handler.
    };

    using function_handler
      = ss::noncopyable_function<ss::future<reply_t>(request_t, reply_t)>;

    struct route_t {
        ss::sstring api;
        ss::path_description path_desc;
        function_handler handler;
    };

    server() = delete;
    ~server() = default;
    server(const server&) = delete;
    server(server&&) = default;
    server& operator=(const server&) = delete;
    server& operator=(server&&) = delete;

    server(
      const ss::sstring& server_name,
      ss::socket_address addr,
      ss::api_registry_builder20&& api20,
      context_t ctx);

    void route(route_t route);
    void route(std::vector<route_t>&& routes);

    ss::future<> start();
    ss::future<> stop();

private:
    ss::httpd::http_server _server;
    ss::gate _pending_reqs;

    ss::socket_address _addr;
    ss::api_registry_builder20 _api20;
    bool _has_routes;
    context_t _ctx;
};

} // namespace pandaproxy
