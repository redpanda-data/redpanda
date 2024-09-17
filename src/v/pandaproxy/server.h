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

#include "base/seastarx.h"
#include "config/rest_authn_endpoint.h"
#include "hashing/jump_consistent_hash.h"
#include "hashing/xx.h"
#include "kafka/client/client.h"
#include "pandaproxy/json/types.h"
#include "pandaproxy/kafka_client_cache.h"
#include "pandaproxy/types.h"
#include "security/request_auth.h"
#include "utils/adjustable_semaphore.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/http/api_docs.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/json_path.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/noncopyable_function.hh>

#include <memory>
#include <type_traits>

namespace pandaproxy {

inline ss::shard_id user_shard(const ss::sstring& name) {
    auto hash = xxhash_64(name.data(), name.length());
    return jump_consistent_hash(hash, ss::smp::count);
}

namespace impl {

template<typename T>
concept KafkaRequestType = std::same_as<T, typename T::api_type::request_type>;

template<typename F>
concept KafkaRequestFactory = KafkaRequestType<std::invoke_result_t<F>>;

inline ss::shard_id consumer_shard(const kafka::group_id& g_id) {
    auto hash = xxhash_64(g_id().data(), g_id().length());
    return jump_consistent_hash(hash, ss::smp::count);
}

} // namespace impl

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
        adjustable_semaphore& inflight_sem;
        ss::abort_source as;
        ss::smp_service_group smp_sg;
    };

    struct request_t {
        std::unique_ptr<ss::http::request> req;
        context_t& ctx;
        credential_t user;
        config::rest_authn_method authn_method;
        // will contain other extensions passed to user specific handler.
    };

    struct reply_t {
        std::unique_ptr<ss::http::reply> rep;
        json::serialization_format mime_type;
        // will contain other extensions passed to user specific handler.
    };

    using function_handler
      = ss::noncopyable_function<ss::future<reply_t>(request_t, reply_t)>;

    struct route_t {
        ss::httpd::path_description path_desc;
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
      ss::httpd::api_registry_builder20&& api20,
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
    ss::httpd::api_registry_builder20 _api20;
    bool _has_routes;
    context_t& _ctx;
    json::serialization_format _exceptional_mime_type;
};

template<typename service_t>
class ctx_server : public server {
    using base = server;

public:
    using reply_t = base::reply_t;
    using function_handler
      = ss::noncopyable_function<ss::future<reply_t>(request_t, reply_t)>;

    using base::server;

    struct context_t : server::context_t {
        service_t& service;
    };

    // request_t restores the type of the context passed in.
    struct request_t : server::request_t {
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

template<typename service_t>
class auth_ctx_server : public ctx_server<service_t> {
    using base = ctx_server<service_t>;

public:
    struct context_t : base::context_t {
        request_authenticator authenticator;
        std::vector<config::rest_authn_endpoint> listeners;
    };

    using base::ctx_server;

    // request_t restores the type of the context passed in.
    struct request_t : base::request_t {
        // Implicit constructor from type-erased server::request_t.
        request_t(server::request_t&& impl) // NOLINT
          : base::request_t(std::move(impl)) {
            // This will throw if authentication fails
            authenticate();
        }
        // Type-restored context
        context_t& context() const {
            return static_cast<context_t&>(base::request_t::ctx);
        };
        // The service
        service_t& service() const { return context().service; };

        template<std::invocable<kafka_client_cache&> Func>
        auto dispatch(Func&& func) {
            // Access the cache on the appropriate shard.
            return service().client_cache().invoke_on(
              user_shard(user.name),
              ss::smp_submit_to_options{context().smp_sg},
              [func{std::forward<Func>(func)}](
                kafka_client_cache& cache) mutable {
                  return std::invoke(std::move(func), cache);
              });
        }

        template<std::invocable<kafka::client::client&> Func>
        auto dispatch(Func&& func) {
            switch (authn_method) {
            case config::rest_authn_method::none: {
                return std::invoke(
                  std::forward<Func>(func), service().client().local());
            }
            case config::rest_authn_method::http_basic: {
                return dispatch([this, func{std::forward<Func>(func)}](
                                  kafka_client_cache& cache) mutable {
                    return cache.with_client_for(
                      user, authn_method, std::forward<Func>(func));
                });
            }
            }
        }

        template<std::invocable<kafka::client::client&> Func>
        auto dispatch(const kafka::group_id& group_id, Func&& func) {
            switch (authn_method) {
            case config::rest_authn_method::none: {
                return service().client().invoke_on(
                  impl::consumer_shard(group_id), std::forward<Func>(func));
            }
            case config::rest_authn_method::http_basic: {
                return dispatch([this, func{std::forward<Func>(func)}](
                                  kafka_client_cache& cache) mutable {
                    return cache.with_client_for(
                      user, authn_method, std::forward<Func>(func));
                });
            }
            }
        }

        template<impl::KafkaRequestFactory Func>
        auto dispatch(Func&& func) {
            return dispatch([func{std::forward<Func>(func)}](
                              kafka::client::client& client) mutable {
                return client.dispatch(std::forward<Func>(func));
            });
        }

        void authenticate() {
            authn_method = config::get_authn_method(
              context().listeners, base::request_t::req->get_listener_idx());

            if (authn_method == config::rest_authn_method::http_basic) {
                // Will throw 400 & 401 if auth fails
                auto auth_result = context().authenticator.authenticate(
                  *base::request_t::req);
                // Will throw 403 if user enabled HTTP Basic Auth but
                // did not give the authorization header.
                auth_result.require_authenticated();
                user = credential_t{
                  auth_result.get_username(),
                  auth_result.get_password(),
                  auth_result.get_sasl_mechanism()};
            }
        }

        using reply_t = typename base::reply_t;
        using function_handler
          = ss::noncopyable_function<ss::future<reply_t>(request_t, reply_t)>;

        credential_t user;
        config::rest_authn_method authn_method;
    };
};

} // namespace pandaproxy
