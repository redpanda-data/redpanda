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
#include "archival/service.h"
#include "cluster/fwd.h"
#include "config/endpoint_tls_config.h"
#include "coproc/partition_manager.h"
#include "model/metadata.h"
#include "cluster/request_auth.h"
#include "rpc/connection_cache.h"
#include "seastarx.h"

#include <seastar/core/scheduling.hh>
#include <seastar/core/sstring.hh>
#include <seastar/http/exception.hh>
#include <seastar/http/file_handler.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/json_path.hh>
#include <seastar/util/log.hh>

#include <absl/container/flat_hash_map.h>

struct admin_server_cfg {
    std::vector<model::broker_endpoint> endpoints;
    std::vector<config::endpoint_tls_config> endpoints_tls;
    ss::sstring admin_api_docs_dir;
    ss::scheduling_group sg;
};

namespace detail {
// Helper for static_assert-ing false below.
template<auto V>
struct dependent_false : std::false_type {};
} // namespace detail

class admin_server {
public:
    explicit admin_server(
      admin_server_cfg,
      ss::sharded<cluster::partition_manager>&,
      ss::sharded<coproc::partition_manager>&,
      cluster::controller*,
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::metadata_cache>&,
      ss::sharded<archival::scheduler_service>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<cluster::node_status_table>&);

    ss::future<> start();
    ss::future<> stop();

    void set_ready() { _ready = true; }

private:
    enum class auth_level {
        // Unauthenticated endpoint (not a typo, 'public' is a keyword)
        publik = 0,
        // Requires authentication (if enabled) but not superuser status
        user = 1,
        // Requires authentication (if enabled) and superuser status
        superuser = 2
    };

    // Enable handlers to refer without auth_level:: prefix
    static constexpr auth_level publik = auth_level::publik;
    static constexpr auth_level user = auth_level::user;
    static constexpr auth_level superuser = auth_level::superuser;

    /**
     * Authenticate, and raise if `required_auth` is not met by
     * the credential (or pass if authentication is disabled).
     */
    template<auth_level required_auth>
    request_auth_result apply_auth(ss::const_req req) {
        auto auth_state = _auth.authenticate(req);
        if constexpr (required_auth == auth_level::superuser) {
            auth_state.require_superuser();
        } else if constexpr (required_auth == auth_level::user) {
            auth_state.require_authenticated();
        } else if constexpr (required_auth == auth_level::publik) {
            auth_state.pass();
        } else {
            static_assert(
              detail::dependent_false<required_auth>::value,
              "Invalid auth_level");
        }

        return auth_state;
    }

    void log_exception(
      const ss::sstring& url,
      const request_auth_result& auth_state,
      std::exception_ptr eptr) const;

    template<typename R>
    auto exception_intercepter(
      const ss::sstring& url, const request_auth_result& auth_state) {
        return [this, url, auth_state](std::exception_ptr eptr) mutable {
            log_exception(url, auth_state, eptr);
            return ss::make_exception_future<R>(eptr);
        };
    };

    /**
     * Helper for binding handlers to routes, which also adds in
     * authentication step and common request logging.  Expects
     * `handler` to be a ss::httpd::future_json_function, or variant
     * with an extra request_auth_state argument if peek_auth is true.
     */
    template<auth_level required_auth, bool peek_auth = false, typename F>
    void register_route(ss::httpd::path_description const& path, F handler) {
        path.set(
          _server._routes,
          [this, handler](std::unique_ptr<ss::httpd::request> req) {
              auto auth_state = apply_auth<required_auth>(*req);

              // Note: a request is only logged if it does not throw
              // from authenticate().
              log_request(*req, auth_state);

              // Intercept exceptions
              const auto url = req->get_url();
              if constexpr (peek_auth) {
                  return handler(std::move(req), auth_state)
                    .handle_exception(
                      exception_intercepter<
                        decltype(handler(std::move(req), auth_state).get0())>(
                        url, auth_state));

              } else {
                  return handler(std::move(req))
                    .handle_exception(exception_intercepter<
                                      decltype(handler(std::move(req)).get0())>(
                      url, auth_state));
              }
          });
    }

    /**
     * Variant of register_route for routes that use the raw `handle_function`
     * callback interface (for returning raw strings) rather than the usual
     * json handlers.
     *
     * This is 'raw' in the sense that less auto-serialization is going on,
     * and the handler has direct access to the `reply` object
     */
    template<auth_level required_auth>
    void register_route_raw(
      ss::httpd::path_description const& path,
      ss::httpd::handle_function handler) {
        auto handler_f = new ss::httpd::function_handler{
          [this, handler](ss::const_req req, ss::reply& reply) {
              auto auth_state = apply_auth<required_auth>(req);

              log_request(req, auth_state);

              /// Intercept and log exceptions
              try {
                  return handler(req, reply);
              } catch (...) {
                  log_exception(
                    req.get_url(), auth_state, std::current_exception());
                  throw;
              }
          },
          "json"};

        path.set(_server._routes, handler_f);
    }

    void log_request(
      const ss::httpd::request& req,
      const request_auth_result& auth_state) const;

    ss::future<> configure_listeners();
    void configure_metrics_route();
    void configure_admin_routes();
    void register_config_routes();
    void register_cluster_config_routes();
    void register_raft_routes();
    void register_kafka_routes();
    void register_security_routes();
    void register_status_routes();
    void register_features_routes();
    void register_broker_routes();
    void register_partition_routes();
    void register_hbadger_routes();
    void register_transaction_routes();
    void register_debug_routes();
    void register_cluster_routes();
    void register_shadow_indexing_routes();

    ss::future<> throw_on_error(
      ss::httpd::request& req,
      std::error_code ec,
      model::ntp const& ntp,
      model::node_id id = model::node_id{-1}) const;
    ss::future<ss::httpd::redirect_exception>
    redirect_to_leader(ss::httpd::request& req, model::ntp const& ntp) const;

    ss::future<ss::json::json_return_type> cancel_node_partition_moves(
      ss::httpd::request& req, cluster::partition_move_direction direction);

    struct level_reset {
        using time_point = ss::timer<>::clock::time_point;

        level_reset(ss::log_level level, time_point expires)
          : level(level)
          , expires(expires) {}

        ss::log_level level;
        time_point expires;
    };

    ss::timer<> _log_level_timer;
    absl::flat_hash_map<ss::sstring, level_reset> _log_level_resets;

    void rearm_log_level_timer();
    void log_level_timer_handler();

    ss::http_server _server;
    admin_server_cfg _cfg;
    ss::sharded<cluster::partition_manager>& _partition_manager;
    ss::sharded<coproc::partition_manager>& _cp_partition_manager;
    cluster::controller* _controller;
    ss::sharded<cluster::shard_table>& _shard_table;
    ss::sharded<cluster::metadata_cache>& _metadata_cache;
    ss::sharded<rpc::connection_cache>& _connection_cache;
    request_authenticator _auth;
    bool _ready{false};
    ss::sharded<archival::scheduler_service>& _archival_service;
    ss::sharded<cluster::node_status_table>& _node_status_table;
};
