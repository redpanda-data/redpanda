// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/server.h"

#include "model/metadata.h"
#include "net/dns.h"
#include "net/tls_certificate_probe.h"
#include "pandaproxy/json/types.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/probe.h"
#include "pandaproxy/reply.h"
#include "rpc/rpc_utils.h"

#include <seastar/core/coroutine.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/reply.hh>
#include <seastar/net/tls.hh>

#include <fmt/chrono.h>
#include <fmt/ranges.h>

#include <charconv>
#include <exception>

namespace pandaproxy {

namespace {
void set_mime_type(ss::http::reply& rep, json::serialization_format fmt) {
    if (fmt != json::serialization_format::none) {
        rep.set_mime_type(ss::sstring(name(fmt)));
    } else { // TODO(Ben Pope): Remove this branch when endpoints are migrated
        rep.set_mime_type("application/vnd.kafka.binary.v2+json");
    }
}

} // namespace

/**
 * Search for the first header of a given name
 * @param name the header name
 * @return a string_view to the header value, if it exists or empty string_view
 */
std::string_view
get_header(const ss::http::request& req, const ss::sstring& name) {
    auto res = req._headers.find(name);
    if (res == req._headers.end()) {
        return std::string_view();
    }
    return res->second;
}

size_t get_request_size(const ss::http::request& req) {
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
      ss::httpd::path_description& path_desc,
      const ss::sstring& metrics_group_name,
      json::serialization_format exceptional_mime_type)
      : _pending_requests(pending_requests)
      , _ctx(ctx)
      , _handler(std::move(handler))
      , _probe(path_desc, metrics_group_name)
      , _exceptional_mime_type(exceptional_mime_type) {}

    ss::future<std::unique_ptr<ss::http::reply>> handle(
      const ss::sstring&,
      std::unique_ptr<ss::http::request> req,
      std::unique_ptr<ss::http::reply> rep) final {
        auto measure = _probe.auto_measure();
        auto guard = ss::gate::holder(_pending_requests);
        server::request_t rq{std::move(req), this->_ctx};
        server::reply_t rp{std::move(rep)};
        const auto set_and_measure_response =
          [&measure](const server::reply_t& rp) {
              set_mime_type(*rp.rep, rp.mime_type);
              measure.set_status(rp.rep->_status);
          };
        auto inflight_units = _ctx.inflight_sem.try_get_units(1);
        if (!inflight_units) {
            set_reply_too_many_requests(*rp.rep);
            rp.mime_type = _exceptional_mime_type;
            set_and_measure_response(rp);
            co_return std::move(rp.rep);
        }
        auto req_size = get_request_size(*rq.req);
        auto sem_units = co_await ss::get_units(_ctx.mem_sem, req_size);
        if (_ctx.as.abort_requested()) {
            set_reply_unavailable(*rp.rep);
            rp.mime_type = _exceptional_mime_type;
            set_and_measure_response(rp);
            co_return std::move(rp.rep);
        }
        auto method = rq.req->_method;
        auto url = rq.req->_url;
        try {
            rp = co_await _handler(std::move(rq), std::move(rp));
        } catch (...) {
            auto ex = std::current_exception();
            vlog(
              plog.warn,
              "Request: {} {} failed: {}",
              method,
              url,
              std::current_exception());
            rp = server::reply_t{exception_reply(ex), _exceptional_mime_type};
        }
        set_and_measure_response(rp);
        co_return std::move(rp.rep);
    }

    ss::gate& _pending_requests;
    server::context_t& _ctx;
    server::function_handler _handler;
    probe _probe;
    json::serialization_format _exceptional_mime_type;
};

server::server(
  const ss::sstring& server_name,
  const ss::sstring& public_metrics_group_name,
  ss::httpd::api_registry_builder20&& api20,
  const ss::sstring& header,
  const ss::sstring& definitions,
  context_t& ctx,
  json::serialization_format exceptional_mime_type)
  : _server(server_name)
  , _public_metrics_group_name(public_metrics_group_name)
  , _pending_reqs()
  , _api20(std::move(api20))
  , _has_routes(false)
  , _ctx(ctx)
  , _exceptional_mime_type(exceptional_mime_type) {
    _api20.set_api_doc(_server._routes);
    _api20.register_api_file(_server._routes, header);
    _api20.add_definitions_file(_server._routes, definitions);
    _server.set_content_streaming(true);
}

/*
 *  the method route register a route handler for the specified endpoint.
 */
void server::route(server::route_t r) {
    // NOTE: this pointer will be owned by data member _routes of
    // ss::httpd:server. seastar didn't use any unique ptr to express that.
    auto* handler = new handler_adaptor(
      _pending_reqs,
      _ctx,
      std::move(r.handler),
      r.path_desc,
      _public_metrics_group_name,
      _exceptional_mime_type);
    r.path_desc.set(_server._routes, handler);
}

void server::routes(server::routes_t&& rts) {
    // Insert a comma between routes to make the api docs valid JSON.
    if (_has_routes) {
        _api20.register_function(
          _server._routes,
          [](ss::output_stream<char>& os) { return os.write(",\n"); });
    } else {
        _has_routes = true;
    }
    _api20.register_api_file(_server._routes, rts.api);

    for (auto& e : rts.routes) {
        this->route(std::move(e));
    }
}

ss::future<> server::start(
  const std::vector<config::rest_authn_endpoint>& endpoints,
  const std::vector<config::endpoint_tls_config>& endpoints_tls,
  const std::vector<model::broker_endpoint>& advertised) {
    _server._routes.register_exeption_handler(
      exception_replier{ss::sstring{name(_exceptional_mime_type)}});
    _ctx.advertised_listeners.reserve(endpoints.size());
    for (auto& server_endpoint : endpoints) {
        auto addr = co_await net::resolve_dns(server_endpoint.address);
        auto it = find_if(
          endpoints_tls.begin(),
          endpoints_tls.end(),
          [&server_endpoint](const config::endpoint_tls_config& ep_tls) {
              return ep_tls.name == server_endpoint.name;
          });
        auto advertised_it = find_if(
          advertised.begin(),
          advertised.end(),
          [&server_endpoint](const model::broker_endpoint& e) {
              return e.name == server_endpoint.name;
          });

        // if we have advertised listener use it, otherwise use server
        // endpoint address
        if (advertised_it != advertised.end()) {
            _ctx.advertised_listeners.push_back(advertised_it->address);
        } else {
            _ctx.advertised_listeners.push_back(server_endpoint.address);
        }

        ss::shared_ptr<ss::tls::server_credentials> cred;
        if (it != endpoints_tls.end()) {
            cred = co_await net::build_reloadable_server_credentials_with_probe(
              it->config,
              _public_metrics_group_name,
              it->name,
              [](
                const std::unordered_set<ss::sstring>& updated,
                const std::exception_ptr& eptr) {
                  rpc::log_certificate_reload_event(
                    plog, "API TLS", updated, eptr);
              });
        }
        co_await _server.listen(addr, cred);
    }
    co_return;
}

ss::future<> server::stop() {
    return _pending_reqs.close()
      .finally([this]() { return _ctx.as.request_abort(); })
      .finally([this]() mutable { return _server.stop(); });
}

} // namespace pandaproxy
