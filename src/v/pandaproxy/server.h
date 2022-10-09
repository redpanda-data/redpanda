/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "config/config_store.h"
#include "config/rest_authn_endpoint.h"
#include "kafka/client/client.h"
#include "pandaproxy/json/types.h"
#include "pandaproxy/types.h"
#include "cluster/request_auth.h"
#include "seastarx.h"

#include <seastar/core/abort_source.hh>
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
        std::vector<net::unresolved_address> advertised_listeners;
        ssx::semaphore& mem_sem;
        ss::abort_source as;
        ss::smp_service_group smp_sg;
    };

    struct request_t {
        std::unique_ptr<ss::httpd::request> req;
        context_t& ctx;
        credential_t user;
        config::rest_authn_method authn_method;
        // will contain other extensions passed to user specific handler.
    };

    struct reply_t {
        std::unique_ptr<ss::httpd::reply> rep;
        json::serialization_format mime_type;
        // will contain other extensions passed to user specific handler.
    };

    using function_handler
      = ss::noncopyable_function<ss::future<reply_t>(request_t, reply_t)>;

    struct route_t {
        ss::path_description path_desc;
        function_handler handler;
    };

    struct routes_t {
        ss::sstring api;
        std::vector<route_t> routes;
    };

    server() = delete;
    ~server() = default;
    server(const server&) = delete;
    server(server&&) noexcept = default;
    server& operator=(const server&) = delete;
    server& operator=(server&&) = delete;

    server(
      const ss::sstring& server_name,
      const ss::sstring& public_metrics_group_name,
      ss::api_registry_builder20&& api20,
      const ss::sstring& header,
      const ss::sstring& definitions,
      context_t& ctx,
      json::serialization_format exceptional_mime_type);

    void route(route_t route);
    void routes(routes_t&& routes);

    ss::future<> start(
      const std::vector<config::rest_authn_endpoint>& endpoints,
      const std::vector<config::endpoint_tls_config>& endpoints_tls,
      const std::vector<model::broker_endpoint>& advertised);
    ss::future<> stop();

private:
    ss::httpd::http_server _server;
    ss::sstring _public_metrics_group_name;
    ss::gate _pending_reqs;
    ss::api_registry_builder20 _api20;
    bool _has_routes;
    context_t& _ctx;
    json::serialization_format _exceptional_mime_type;
};

template<typename service_t>
class ctx_server : public server {
public:
    using server::server;

    struct context_t : server::context_t {
        service_t& service;
    };

    // request_t restores the type of the context passed in.
    struct request_t final : server::request_t {
        // Implicit constructor from type-erased server::request_t.
        request_t(server::request_t&& impl) // NOLINT
          : server::request_t(std::move(impl)) {}
        // Type-restored context
        context_t& context() const {
            return static_cast<context_t&>(server::request_t::ctx);
        };
        // The service
        service_t& service() const { return context().service; };
    };
};

} // namespace pandaproxy
