#include "pandaproxy/server.h"

#include "pandaproxy/logger.h"

#include <seastar/http/function_handlers.hh>

#include <exception>

namespace pandaproxy {
ss::httpd::reply& set_reply_unavailable(ss::httpd::reply& rep) {
    return rep.set_status(ss::httpd::reply::status_type::service_unavailable)
      .add_header("Retry-After", "0");
}

std::unique_ptr<ss::httpd::reply> reply_unavailable() {
    auto rep = std::make_unique<ss::httpd::reply>(ss::httpd::reply{});
    set_reply_unavailable(*rep);
    return rep;
}

std::unique_ptr<ss::httpd::reply> exception_reply(std::exception_ptr e) {
    try {
        std::rethrow_exception(e);
    } catch (const ss::gate_closed_exception& e) {
        return reply_unavailable();
    } catch (...) {
        vlog(plog.error, "{}", std::current_exception());
        throw;
    }
}

// server::function_handler to seastar::httpd::handler
struct handler_adaptor : ss::httpd::handler_base {
    handler_adaptor(
      ss::gate& pending_requests,
      server::context_t& ctx,
      server::function_handler&& handler)
      : _pending_requests(pending_requests)
      , _ctx(ctx)
      , _handler(std::move(handler)) {}

    ss::future<std::unique_ptr<ss::reply>> handle(
      const ss::sstring&,
      std::unique_ptr<ss::request> req,
      std::unique_ptr<ss::reply> rep) final {
        return ss::with_gate(
          _pending_requests,
          [this, req{std::move(req)}, rep{std::move(rep)}]() mutable {
              server::request_t rq{std::move(req), this->_ctx};
              server::reply_t rp{std::move(rep)};

              return _handler(std::move(rq), std::move(rp))
                .then([](server::reply_t rp) {
                    rp.rep->set_mime_type("application/vnd.kafka.json.v2+json");
                    return std::move(rp.rep);
                });
          });
    }

    ss::gate& _pending_requests;
    server::context_t& _ctx;
    server::function_handler _handler;
};

server::server(
  const ss::sstring& server_name,
  ss::socket_address addr,
  ss::api_registry_builder20&& api20,
  context_t ctx)
  : _server(server_name)
  , _pending_reqs()
  , _addr(addr)
  , _api20(std::move(api20))
  , _has_routes(false)
  , _ctx(ctx) {
    _api20.set_api_doc(_server._routes);
    _api20.register_api_file(_server._routes, "header");
}

/*
 *  the method route register a route handler for the specified endpoint.
 */
void server::route(server::route_t r) {
    // Insert a comma between routes to make the api docs valid JSON.
    if (_has_routes) {
        _api20.register_function(
          _server._routes,
          [](ss::output_stream<char>& os) { return os.write(",\n"); });
    } else {
        _has_routes = true;
    }
    _api20.register_api_file(_server._routes, r.api);

    // NOTE: this pointer will be owned by data member _routes of
    // ss::httpd:server. seastar didn't use any unique ptr to express that.
    auto* handler = new handler_adaptor(
      _pending_reqs, _ctx, std::move(r.handler));
    r.path_desc.set(_server._routes, handler);
}

void server::route(std::vector<server::route_t>&& rts) {
    for (auto& e : rts) {
        this->route(std::move(e));
    }
}

ss::future<> server::start() {
    _server._routes.register_exeption_handler(exception_reply);
    return _server.listen(_addr);
}

ss::future<> server::stop() {
    return _pending_reqs.close().finally(
      [this]() mutable { return _server.stop(); });
}

} // namespace pandaproxy
