// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/server.h"

#include "cluster/cluster_utils.h"
#include "pandaproxy/configuration.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/probe.h"
#include "pandaproxy/reply.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/std-coroutine.hh>
#include <seastar/http/function_handlers.hh>

#include <charconv>
#include <exception>

namespace pandaproxy {

/**
 * Search for the first header of a given name
 * @param name the header name
 * @return a string_view to the header value, if it exists or empty string_view
 */
std::string_view
get_header(const ss::httpd::request& req, const ss::sstring& name) {
    auto res = req._headers.find(name);
    if (res == req._headers.end()) {
        return std::string_view();
    }
    return res->second;
}

size_t get_request_size(const ss::httpd::request& req) {
    const size_t fixed_overhead{1024};

    auto content_length_hdr{get_header(req, "Content-Length")};
    size_t content_length{0};
    // Ignore failure, content_length is unchanged
    std::from_chars(
      content_length_hdr.begin(), content_length_hdr.end(), content_length);

    return fixed_overhead + content_length;
}

// server::function_handler to seastar::httpd::handler
struct handler_adaptor : ss::httpd::handler_base {
    handler_adaptor(
      ss::gate& pending_requests,
      server::context_t& ctx,
      server::function_handler&& handler,
      ss::httpd::path_description& path_desc)
      : _pending_requests(pending_requests)
      , _ctx(ctx)
      , _handler(std::move(handler))
      , _probe(path_desc) {}

    ss::future<std::unique_ptr<ss::reply>> handle(
      const ss::sstring&,
      std::unique_ptr<ss::request> req,
      std::unique_ptr<ss::reply> rep) final {
        return ss::with_gate(
          _pending_requests,
          [this,
           req{std::move(req)},
           rep{std::move(rep)},
           m = _probe.hist().auto_measure()]() mutable {
              server::request_t rq{std::move(req), this->_ctx};
              server::reply_t rp{std::move(rep)};
              auto req_size = get_request_size(*rq.req);

              return ss::with_semaphore(
                       _ctx.mem_sem,
                       req_size,
                       [this, rq{std::move(rq)}, rp{std::move(rp)}]() mutable {
                           if (_ctx.as.abort_requested()) {
                               set_reply_unavailable(*rp.rep);
                               return ss::make_ready_future<
                                 std::unique_ptr<ss::reply>>(std::move(rp.rep));
                           }
                           return _handler(std::move(rq), std::move(rp))
                             .then([](server::reply_t rp) {
                                 rp.rep->set_mime_type(
                                   "application/vnd.kafka.binary.v2+json");
                                 return std::move(rp.rep);
                             });
                       })
                .finally([m{std::move(m)}]() {});
          });
    }

    ss::gate& _pending_requests;
    server::context_t& _ctx;
    server::function_handler _handler;
    probe _probe;
};

server::server(
  const ss::sstring& server_name,
  ss::api_registry_builder20&& api20,
  context_t ctx)
  : _server(server_name)
  , _pending_reqs()
  , _api20(std::move(api20))
  , _has_routes(false)
  , _ctx(std::move(ctx)) {
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
      _pending_reqs, _ctx, std::move(r.handler), r.path_desc);
    r.path_desc.set(_server._routes, handler);
}

void server::route(std::vector<server::route_t>&& rts) {
    for (auto& e : rts) {
        this->route(std::move(e));
    }
}

ss::future<> server::start() {
    _server._routes.register_exeption_handler(exception_reply);

    auto builder
      = co_await _ctx.config.pandaproxy_api_tls().get_credentials_builder();
    if (builder) {
        auto cred = co_await builder->build_reloadable_server_credentials(
          [](
            const std::unordered_set<ss::sstring>& updated,
            const std::exception_ptr& eptr) {
              cluster::log_certificate_reload_event(
                plog, "API TLS", updated, eptr);
          });

        _server.set_tls_credentials(std::move(cred));
    }

    auto addr = co_await rpc::resolve_dns(_ctx.config.pandaproxy_api);
    co_await _server.listen(addr);
}

ss::future<> server::stop() {
    return _pending_reqs.close()
      .finally([this]() { return _ctx.as.request_abort(); })
      .finally([this]() mutable { return _server.stop(); });
}

} // namespace pandaproxy
