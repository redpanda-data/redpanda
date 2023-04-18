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

#include "cluster/fwd.h"
#include "config/endpoint_tls_config.h"
#include "coproc/partition_manager.h"
#include "kafka/server/fwd.h"
#include "model/metadata.h"
#include "pandaproxy/rest/fwd.h"
#include "pandaproxy/schema_registry/fwd.h"
#include "rpc/connection_cache.h"
#include "seastarx.h"
#include "utils/request_auth.h"

#include <seastar/core/scheduling.hh>
#include <seastar/core/sstring.hh>
#include <seastar/http/exception.hh>
#include <seastar/http/file_handler.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/json_path.hh>
#include <seastar/json/json_elements.hh>
#include <seastar/util/log.hh>

#include <absl/container/flat_hash_map.h>

struct admin_server_cfg {
    std::vector<model::broker_endpoint> endpoints;
    std::vector<config::endpoint_tls_config> endpoints_tls;
    ss::sstring admin_api_docs_dir;
    ss::scheduling_group sg;
};

enum class service_kind {
    http_proxy,
    schema_registry,
};

namespace detail {
// Helper for static_assert-ing false below.
template<auto V>
struct dependent_false : std::false_type {};
} // namespace detail

namespace cloud_storage {
struct topic_recovery_service;
}

class admin_server {
public:
    explicit admin_server(
      admin_server_cfg,
      ss::sharded<cluster::partition_manager>&,
      ss::sharded<coproc::partition_manager>&,
      cluster::controller*,
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::metadata_cache>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<cluster::node_status_table>&,
      ss::sharded<cluster::self_test_frontend>&,
      ss::sharded<kafka::usage_manager>&,
      pandaproxy::rest::api*,
      pandaproxy::schema_registry::api*,
      ss::sharded<cloud_storage::topic_recovery_service>&,
      ss::sharded<cluster::topic_recovery_status_frontend>&);

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
    request_auth_result apply_auth(ss::httpd::const_req req) {
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
          [this, handler](std::unique_ptr<ss::http::request> req)
            -> ss::future<ss::json::json_return_type> {
              auto auth_state = apply_auth<required_auth>(*req);

              // Note: a request is only logged if it does not throw
              // from authenticate().
              log_request(*req, auth_state);

              // Intercept exceptions
              const auto url = req->get_url();
              if constexpr (peek_auth) {
                  return ss::futurize_invoke(
                           handler, std::move(req), auth_state)
                    .handle_exception(
                      exception_intercepter<
                        decltype(handler(std::move(req), auth_state).get0())>(
                        url, auth_state));

              } else {
                  return ss::futurize_invoke(handler, std::move(req))
                    .handle_exception(exception_intercepter<
                                      decltype(handler(std::move(req)).get0())>(
                      url, auth_state));
              }
          });
    }

    /**
     * Variant of register_route for synchronous handlers.
     * \tparam F An ss::httpd::json_request_function, or variant
     *    with an extra request_auth_state argument if peek_auth is true.
     */
    template<auth_level required_auth, bool peek_auth = false, typename F>
    void
    register_route_sync(ss::httpd::path_description const& path, F handler) {
        path.set(
          _server._routes,
          [this,
           handler](ss::httpd::const_req req) -> ss::json::json_return_type {
              const auto auth_state = apply_auth<required_auth>(req);
              log_request(req, auth_state);

              const auto url = req.get_url();
              try {
                  if constexpr (peek_auth) {
                      return handler(req, auth_state);
                  } else {
                      return handler(req);
                  }
              } catch (...) {
                  log_exception(url, auth_state, std::current_exception());
                  throw;
              }
          });
    }

    /**
     * Variant of register_route_raw_sync for routes that require async
     * processing.
     * This is 'raw' in the sense that less auto-serialization is going on,
     * and the handler has direct access to the `reply` object
     */
    template<auth_level required_auth, bool peek_auth = false, typename F>
    void register_route_raw_async(
      ss::httpd::path_description const& path, F handler) {
        auto wrapped_handler = [this, handler](
                                 std::unique_ptr<ss::http::request> req,
                                 std::unique_ptr<ss::http::reply> rep)
          -> ss::future<std::unique_ptr<ss::http::reply>> {
            auto auth_state = apply_auth<required_auth>(*req);

            // Note: a request is only logged if it does not throw
            // from authenticate().
            log_request(*req, auth_state);

            // Intercept exceptions
            const auto url = req->get_url();
            if constexpr (peek_auth) {
                return ss::futurize_invoke(
                         handler, std::move(req), std::move(rep), auth_state)
                  .handle_exception(
                    exception_intercepter<
                      decltype(handler(
                                 std::move(req), std::move(rep), auth_state)
                                 .get0())>(url, auth_state));

            } else {
                return ss::futurize_invoke(
                         handler, std::move(req), std::move(rep))
                  .handle_exception(
                    exception_intercepter<
                      decltype(handler(std::move(req), std::move(rep)).get0())>(
                      url, auth_state));
            }
        };

        auto handler_f = new ss::httpd::function_handler{
          std::move(wrapped_handler), "json"};

        path.set(_server._routes, handler_f);
    }

    template<auth_level required_auth>
    void register_route_raw_sync(
      ss::httpd::path_description const& path,
      ss::httpd::handle_function handler) {
        auto handler_f = new ss::httpd::function_handler{
          [this, handler](ss::httpd::const_req req, ss::http::reply& reply) {
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

    using request_handler_fn
      = ss::noncopyable_function<ss::future<std::unique_ptr<ss::http::reply>>(
        std::unique_ptr<ss::http::request>, std::unique_ptr<ss::http::reply>)>;

    /**
     * Handler implementation to allow control over the reply. Accepts a handler
     * function which takes as parameters both the http request and the reply.
     * The supplied function can set the reply status code and content.
     */
    template<auth_level required_auth>
    struct handler_impl final : public ss::httpd::handler_base {
        handler_impl(admin_server& server, request_handler_fn handler)
          : _server{server}
          , _handler{std::move(handler)} {}

        ss::future<std::unique_ptr<ss::http::reply>> handle(
          [[maybe_unused]] const ss::sstring& path,
          std::unique_ptr<ss::http::request> request,
          std::unique_ptr<ss::http::reply> reply) override {
            auto auth_state = _server.apply_auth<required_auth>(*request);
            _server.log_request(*request, auth_state);

            const auto url = request->get_url();
            return ss::futurize_invoke(
                     _handler, std::move(request), std::move(reply))
              .handle_exception(
                _server.exception_intercepter<
                  decltype(_handler(std::move(request), std::move(reply))
                             .get0())>(url, auth_state));
        }

        admin_server& _server;
        request_handler_fn _handler;
    };

    template<auth_level required_auth>
    void register_route(
      ss::httpd::path_description const& path, request_handler_fn handler) {
        path.set(
          _server._routes,
          new handler_impl<required_auth>{*this, std::move(handler)});
    }

    void log_request(
      const ss::http::request& req,
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
    void register_usage_routes();
    void register_self_test_routes();
    void register_cluster_routes();
    void register_shadow_indexing_routes();

    ss::future<ss::json::json_return_type> patch_cluster_config_handler(
      std::unique_ptr<ss::http::request>, const request_auth_result&);

    /// Raft routes
    ss::future<ss::json::json_return_type>
      raft_transfer_leadership_handler(std::unique_ptr<ss::http::request>);

    /// Security routes
    ss::future<ss::json::json_return_type>
      create_user_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      delete_user_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      update_user_handler(std::unique_ptr<ss::http::request>);

    /// Kafka routes
    ss::future<ss::json::json_return_type>
      kafka_transfer_leadership_handler(std::unique_ptr<ss::http::request>);

    /// Feature routes
    ss::future<ss::json::json_return_type>
      put_feature_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      put_license_handler(std::unique_ptr<ss::http::request>);

    /// Broker routes
    ss::future<ss::json::json_return_type>
      get_broker_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      decomission_broker_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      get_decommission_progress_handler(std::unique_ptr<ss::http::request>);

    ss::future<ss::json::json_return_type>
      recomission_broker_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      start_broker_maintenance_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      stop_broker_maintenance_handler(std::unique_ptr<ss::http::request>);

    /// Register partition routes
    ss::future<ss::json::json_return_type>
      get_partition_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      get_topic_partitions_handler(std::unique_ptr<ss::http::request>);

    ss::future<ss::json::json_return_type>
      get_transactions_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type> get_transactions_inner_handler(
      cluster::partition_manager&,
      model::ntp,
      std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      mark_transaction_expired_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      cancel_partition_reconfig_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      unclean_abort_partition_reconfig_handler(
        std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      set_partition_replicas_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      trigger_on_demand_rebalance_handler(std::unique_ptr<ss::http::request>);

    /// Transaction routes
    ss::future<ss::json::json_return_type>
      get_all_transactions_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      delete_partition_handler(std::unique_ptr<ss::http::request>);

    /// Cluster routes
    ss::future<ss::json::json_return_type>
      get_partition_balancer_status_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      cancel_all_partitions_reconfigs_handler(
        std::unique_ptr<ss::http::request>);

    /// Shadow indexing routes
    ss::future<ss::json::json_return_type>
      sync_local_state_handler(std::unique_ptr<ss::http::request>);
    ss::future<std::unique_ptr<ss::http::reply>>
      initiate_topic_scan_and_recovery(
        std::unique_ptr<ss::http::request>, std::unique_ptr<ss::http::reply>);
    ss::future<ss::json::json_return_type>
    query_automated_recovery(std::unique_ptr<ss::http::request> req);
    ss::future<ss::json::json_return_type>
    get_partition_cloud_storage_status(std::unique_ptr<ss::http::request> req);
    ss::future<std::unique_ptr<ss::http::reply>> get_manifest(
      std::unique_ptr<ss::http::request> req,
      std::unique_ptr<ss::http::reply> rep);

    /// Self test routes
    ss::future<ss::json::json_return_type>
      self_test_start_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      self_test_stop_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      self_test_get_results_handler(std::unique_ptr<ss::http::request>);

    ss::future<ss::json::json_return_type>
      get_partition_state_handler(std::unique_ptr<ss::http::request>);

    // Debug routes
    ss::future<ss::json::json_return_type>
      cloud_storage_usage_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      restart_service_handler(std::unique_ptr<ss::http::request>);

    ss::future<> throw_on_error(
      ss::http::request& req,
      std::error_code ec,
      model::ntp const& ntp,
      model::node_id id = model::node_id{-1}) const;
    ss::future<ss::httpd::redirect_exception>
    redirect_to_leader(ss::http::request& req, model::ntp const& ntp) const;

    ss::future<ss::json::json_return_type> cancel_node_partition_moves(
      ss::http::request& req, cluster::partition_move_direction direction);

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

    ss::future<> restart_redpanda_service(service_kind service);

    ss::httpd::http_server _server;
    admin_server_cfg _cfg;
    ss::sharded<cluster::partition_manager>& _partition_manager;
    ss::sharded<coproc::partition_manager>& _cp_partition_manager;
    cluster::controller* _controller;
    ss::sharded<cluster::shard_table>& _shard_table;
    ss::sharded<cluster::metadata_cache>& _metadata_cache;
    ss::sharded<rpc::connection_cache>& _connection_cache;
    request_authenticator _auth;
    bool _ready{false};
    ss::sharded<cluster::node_status_table>& _node_status_table;
    ss::sharded<cluster::self_test_frontend>& _self_test_frontend;
    ss::sharded<kafka::usage_manager>& _usage_manager;
    pandaproxy::rest::api* _http_proxy;
    pandaproxy::schema_registry::api* _schema_registry;
    ss::sharded<cloud_storage::topic_recovery_service>& _topic_recovery_service;
    ss::sharded<cluster::topic_recovery_status_frontend>&
      _topic_recovery_status_frontend;
    // Value before the temporary override
    std::chrono::milliseconds _default_blocked_reactor_notify;
    ss::timer<> _blocked_reactor_notify_reset_timer;
};
