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
#include "base/type_traits.h"
#include "cloud_storage/fwd.h"
#include "cluster/fwd.h"
#include "cluster/tx_gateway_frontend.h"
#include "cluster/types.h"
#include "config/endpoint_tls_config.h"
#include "debug_bundle/fwd.h"
#include "finjector/stress_fiber.h"
#include "kafka/server/fwd.h"
#include "model/metadata.h"
#include "pandaproxy/rest/fwd.h"
#include "pandaproxy/schema_registry/fwd.h"
#include "redpanda/admin/debug_bundle.h"
#include "resource_mgmt/cpu_profiler.h"
#include "resource_mgmt/memory_sampling.h"
#include "rpc/connection_cache.h"
#include "security/audit/schemas/iam.h"
#include "security/audit/schemas/utils.h"
#include "security/fwd.h"
#include "security/request_auth.h"
#include "security/types.h"
#include "storage/node.h"
#include "transform/fwd.h"

#include <seastar/core/do_with.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>
#include <seastar/http/exception.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/json_path.hh>
#include <seastar/http/request.hh>
#include <seastar/json/json_elements.hh>
#include <seastar/util/bool_class.hh>
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

namespace cloud_storage {
struct topic_recovery_service;
}

extern ss::logger adminlog;

class admin_server {
public:
    explicit admin_server(
      admin_server_cfg,
      ss::sharded<stress_fiber_manager>&,
      ss::sharded<cluster::partition_manager>&,
      ss::sharded<raft::group_manager>&,
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
      ss::sharded<cluster::topic_recovery_status_frontend>&,
      ss::sharded<storage::node>&,
      ss::sharded<memory_sampling>&,
      ss::sharded<cloud_storage::cache>&,
      ss::sharded<resources::cpu_profiler>&,
      ss::sharded<transform::service>*,
      ss::sharded<security::audit::audit_log_manager>&,
      std::unique_ptr<cluster::tx_manager_migrator>&,
      ss::sharded<kafka::server>&,
      ss::sharded<cluster::tx_gateway_frontend>&,
      ss::sharded<debug_bundle::service>&);

    ss::future<> start();
    ss::future<> stop();

    void set_ready() { _ready = true; }

    struct string_conversion_exception
      : public default_control_character_thrower {
        using default_control_character_thrower::
          default_control_character_thrower;
        [[noreturn]] [[gnu::cold]] void conversion_error() override {
            throw ss::httpd::bad_request_exception(
              "Parameter contained invalid control characters: "
              + get_sanitized_string());
        }
    };

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

    using httpd_authorized = ss::bool_class<struct httpd_authorized>;
    void audit_authz(
      ss::httpd::const_req req,
      const request_auth_result& auth_result,
      httpd_authorized authorized,
      std::optional<std::string_view> reason = std::nullopt);

    void audit_authn(
      ss::httpd::const_req req, const request_auth_result& auth_result);

    void audit_authn_failure(
      ss::httpd::const_req req,
      const security::credential_user& username,
      const ss::sstring& reason);

    void do_audit_authn(
      ss::httpd::const_req req,
      security::audit::authentication_event_options options);

    /**
     * Authenticate, and raise if `required_auth` is not met by
     * the credential (or pass if authentication is disabled).
     */
    template<auth_level required_auth>
    request_auth_result apply_auth(ss::httpd::const_req req) {
        auto auth_state = [this, &req]() -> request_auth_result {
            try {
                return _auth.authenticate(req);
            } catch (const unauthorized_user_exception& e) {
                audit_authn_failure(req, e.get_username(), e.what());
                throw;
            } catch (const ss::httpd::base_exception& e) {
                audit_authn_failure(
                  req, security::credential_user{"{{unknown}}"}, e.what());
                throw;
            }
        }();

        try {
            if constexpr (required_auth == auth_level::superuser) {
                auth_state.require_superuser();
            } else if constexpr (required_auth == auth_level::user) {
                auth_state.require_authenticated();
            } else if constexpr (required_auth == auth_level::publik) {
                auth_state.pass();
            } else {
                static_assert(
                  base::unsupported_value<required_auth>::value,
                  "Invalid auth_level");
            }

        } catch (const ss::httpd::base_exception& ex) {
            audit_authn(req, auth_state);
            audit_authz(req, auth_state, httpd_authorized::no, ex.what());
            throw;
        }

        audit_authn(req, auth_state);
        audit_authz(req, auth_state, httpd_authorized::yes);

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

    static model::ntp
    parse_ntp_from_request(ss::httpd::parameters& param, model::ns ns);

    static model::ntp parse_ntp_from_request(ss::httpd::parameters& param);

    static ss::future<json::Document> parse_json_body(ss::http::request* req);

    static model::node_id parse_broker_id(const ss::http::request& req);

    static bool str_to_bool(std::string_view s);

    /**
     * Helper for binding handlers to routes, which also adds in
     * authentication step and common request logging.  Expects
     * `handler` to be a ss::httpd::future_json_function, or variant
     * with an extra request_auth_state argument if peek_auth is true.
     */
    template<auth_level required_auth, bool peek_auth = false, typename F>
    void register_route(const ss::httpd::path_description& path, F handler) {
        path.set(
          _server._routes,
          [this, handler](std::unique_ptr<ss::http::request> req)
            -> ss::future<ss::json::json_return_type> {
              auto auth_state = apply_auth<required_auth>(*req);
              return ss::do_with(
                std::move(auth_state),
                [this, handler, req = std::move(req)](
                  const auto& auth_state) mutable {
                    // Note: a request is only logged if it does not throw
                    // from authenticate().
                    log_request(*req, auth_state);

                    // Intercept exceptions
                    const auto url = req->get_url();
                    if constexpr (peek_auth) {
                        return ss::futurize_invoke(
                                 handler, std::move(req), auth_state)
                          .handle_exception(
                            exception_intercepter<std::remove_reference_t<
                              decltype(handler(std::move(req), auth_state)
                                         .get())>>(url, auth_state));

                    } else {
                        return ss::futurize_invoke(handler, std::move(req))
                          .handle_exception(
                            exception_intercepter<std::remove_reference_t<
                              decltype(handler(std::move(req)).get())>>(
                              url, auth_state));
                    }
                });
          });
    }

    /**
     * Variant of register_route for synchronous handlers.
     * \tparam F An ss::httpd::json_request_function, or variant
     *    with an extra request_auth_state argument if peek_auth is true.
     */
    template<auth_level required_auth, bool peek_auth = false, typename F>
    void
    register_route_sync(const ss::httpd::path_description& path, F handler) {
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
      const ss::httpd::path_description& path, F handler) {
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
                    exception_intercepter<std::remove_reference_t<
                      decltype(handler(
                                 std::move(req), std::move(rep), auth_state)
                                 .get())>>(url, auth_state));

            } else {
                return ss::futurize_invoke(
                         handler, std::move(req), std::move(rep))
                  .handle_exception(
                    exception_intercepter<std::remove_reference_t<
                      decltype(handler(std::move(req), std::move(rep)).get())>>(
                      url, auth_state));
            }
        };

        auto handler_f = new ss::httpd::function_handler{
          std::move(wrapped_handler), "json"};

        path.set(_server._routes, handler_f);
    }

    template<auth_level required_auth>
    void register_route_raw_sync(
      const ss::httpd::path_description& path,
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
                _server.exception_intercepter<std::remove_reference_t<
                  decltype(_handler(std::move(request), std::move(reply))
                             .get())>>(url, auth_state));
        }

        admin_server& _server;
        request_handler_fn _handler;
    };

    template<auth_level required_auth>
    void register_route(
      const ss::httpd::path_description& path, request_handler_fn handler) {
        path.set(
          _server._routes,
          new handler_impl<required_auth>{*this, std::move(handler)});
    }

    static bool need_redirect_to_leader(
      model::ntp ntp, ss::sharded<cluster::metadata_cache>& metadata_cache);

    static model::ntp
    parse_ntp_from_query_param(const std::unique_ptr<ss::http::request>& req);

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
    void register_cluster_partitions_routes();
    void register_shadow_indexing_routes();
    void register_wasm_transform_routes();
    void register_recovery_mode_routes();
    void register_data_migration_routes();
    void register_topic_routes();
    void register_debug_bundle_routes();

    ss::future<ss::json::json_return_type> patch_cluster_config_handler(
      std::unique_ptr<ss::http::request>, const request_auth_result&);

    /// Raft routes
    ss::future<ss::json::json_return_type>
      raft_transfer_leadership_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      get_raft_recovery_status_handler(std::unique_ptr<ss::http::request>);

    /// Security routes
    ss::future<ss::json::json_return_type>
      create_user_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      delete_user_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      update_user_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      oidc_whoami_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
    oidc_keys_cache_invalidate_handler(std::unique_ptr<ss::http::request> req);
    ss::future<ss::json::json_return_type>
    oidc_revoke_handler(std::unique_ptr<ss::http::request> req);
    ss::future<ss::json::json_return_type> list_user_roles_handler(
      std::unique_ptr<ss::http::request>, request_auth_result);

    ss::future<std::unique_ptr<ss::http::reply>> create_role_handler(
      std::unique_ptr<ss::http::request> req,
      std::unique_ptr<ss::http::reply> rep);
    ss::future<ss::json::json_return_type>
    list_roles_handler(std::unique_ptr<ss::http::request> req);
    ss::future<ss::json::json_return_type>
    get_role_handler(std::unique_ptr<ss::http::request> req);
    ss::future<ss::json::json_return_type>
    update_role_handler(std::unique_ptr<ss::http::request> req);
    ss::future<std::unique_ptr<ss::http::reply>> delete_role_handler(
      std::unique_ptr<ss::http::request> req,
      std::unique_ptr<ss::http::reply> rep);

    ss::future<ss::json::json_return_type>
    update_role_members_handler(std::unique_ptr<ss::http::request> req);

    /// Kafka routes
    ss::future<ss::json::json_return_type>
      kafka_transfer_leadership_handler(std::unique_ptr<ss::http::request>);

    /// Feature routes
    ss::future<ss::json::json_return_type>
      put_feature_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      put_license_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      get_enterprise_handler(std::unique_ptr<ss::http::request>);

    /// Broker routes

    ss::future<ss::json::json_return_type>
      get_broker_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      decomission_broker_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      get_decommission_progress_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type> get_broker_uuids_handler();

    ss::future<ss::json::json_return_type>
      recomission_broker_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      start_broker_maintenance_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      stop_broker_maintenance_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      reset_crash_tracking(std::unique_ptr<ss::http::request>);

    /// Register partition routes
    ss::future<ss::json::json_return_type>
      get_partition_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      get_topic_partitions_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      get_majority_lost_partitions(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      force_recover_partitions_from_nodes(std::unique_ptr<ss::http::request>);

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
      force_set_partition_replicas_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      set_partition_replica_core_handler(std::unique_ptr<ss::http::request>);

    ss::future<ss::json::json_return_type>
      trigger_on_demand_rebalance_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      trigger_shard_rebalance_handler(std::unique_ptr<ss::http::request>);

    ss::future<ss::json::json_return_type>
      get_reconfigurations_handler(std::unique_ptr<ss::http::request>);

    /// Transaction routes
    ss::future<ss::json::json_return_type>
      get_all_transactions_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      delete_partition_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      find_tx_coordinator_handler(std::unique_ptr<ss::http::request>);

    /// Cluster routes
    ss::future<ss::json::json_return_type>
      get_partition_balancer_status_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      cancel_all_partitions_reconfigs_handler(
        std::unique_ptr<ss::http::request>);

    /// Cluster partition routes
    ss::future<ss::json::json_return_type>
      post_cluster_partitions_topic_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      post_cluster_partitions_topic_partition_handler(
        std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      get_cluster_partitions_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      get_cluster_partitions_topic_handler(std::unique_ptr<ss::http::request>);

    /// Shadow indexing routes
    ss::future<ss::json::json_return_type>
      sync_local_state_handler(std::unique_ptr<ss::http::request>);
    ss::future<std::unique_ptr<ss::http::reply>> unsafe_reset_metadata(
      std::unique_ptr<ss::http::request>, std::unique_ptr<ss::http::reply>);
    ss::future<std::unique_ptr<ss::http::reply>>
      unsafe_reset_metadata_from_cloud(
        std::unique_ptr<ss::http::request>, std::unique_ptr<ss::http::reply>);
    ss::future<std::unique_ptr<ss::http::reply>>
      initiate_topic_scan_and_recovery(
        std::unique_ptr<ss::http::request>, std::unique_ptr<ss::http::reply>);
    ss::future<ss::json::json_return_type>
    query_automated_recovery(std::unique_ptr<ss::http::request> req);
    ss::future<std::unique_ptr<ss::http::reply>> initialize_cluster_recovery(
      std::unique_ptr<ss::http::request>, std::unique_ptr<ss::http::reply>);
    ss::future<ss::json::json_return_type>
    get_cluster_recovery(std::unique_ptr<ss::http::request> req);
    ss::future<ss::json::json_return_type>
    get_partition_cloud_storage_status(std::unique_ptr<ss::http::request> req);
    ss::future<ss::json::json_return_type>
    get_cloud_storage_lifecycle(std::unique_ptr<ss::http::request> req);
    ss::future<ss::json::json_return_type>
    delete_cloud_storage_lifecycle(std::unique_ptr<ss::http::request> req);
    ss::future<ss::json::json_return_type>
    post_cloud_storage_cache_trim(std::unique_ptr<ss::http::request> req);
    ss::future<std::unique_ptr<ss::http::reply>> get_manifest(
      std::unique_ptr<ss::http::request> req,
      std::unique_ptr<ss::http::reply> rep);
    ss::future<ss::json::json_return_type>
      get_cloud_storage_anomalies(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      reset_scrubbing_metadata(std::unique_ptr<ss::http::request>);

    /// Self test routes
    ss::future<ss::json::json_return_type>
      self_test_start_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      self_test_stop_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      self_test_get_results_handler(std::unique_ptr<ss::http::request>);

    ss::future<ss::json::json_return_type>
      get_partition_state_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      get_local_storage_usage_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      get_disk_stat_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      put_disk_stat_handler(std::unique_ptr<ss::http::request>);

    // Debug routes
    ss::future<ss::json::json_return_type>
      cpu_profile_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      get_local_offsets_translated_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      cloud_storage_usage_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      restart_service_handler(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      sampled_memory_profile_handler(std::unique_ptr<ss::http::request>);

    ss::future<ss::json::json_return_type> get_node_uuid_handler();
    ss::future<ss::json::json_return_type>
      override_node_uuid_handler(std::unique_ptr<ss::http::request>);

    // Transform routes
    ss::future<ss::json::json_return_type>
      deploy_transform(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      list_transforms(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      delete_transform(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      list_committed_offsets(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      garbage_collect_committed_offsets(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      patch_transform_metadata(std::unique_ptr<ss::http::request>);

    // Data migration routes
    ss::future<std::unique_ptr<ss::http::reply>> list_data_migrations(
      std::unique_ptr<ss::http::request>, std::unique_ptr<ss::http::reply>);
    ss::future<std::unique_ptr<ss::http::reply>> get_data_migration(
      std::unique_ptr<ss::http::request>, std::unique_ptr<ss::http::reply>);
    ss::future<std::unique_ptr<ss::http::reply>> add_data_migration(
      std::unique_ptr<ss::http::request>, std::unique_ptr<ss::http::reply>);
    ss::future<ss::json::json_return_type>
      execute_migration_action(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      delete_migration(std::unique_ptr<ss::http::request>);

    // Topic routes
    ss::future<ss::json::json_return_type>
      mount_topics(std::unique_ptr<ss::http::request>);
    ss::future<ss::json::json_return_type>
      unmount_topics(std::unique_ptr<ss::http::request>);

    // Debug Bundle routes
    ss::future<std::unique_ptr<ss::http::reply>> post_debug_bundle(
      std::unique_ptr<ss::http::request>, std::unique_ptr<ss::http::reply>);
    ss::future<std::unique_ptr<ss::http::reply>> get_debug_bundle(
      std::unique_ptr<ss::http::request>, std::unique_ptr<ss::http::reply>);
    ss::future<std::unique_ptr<ss::http::reply>> delete_debug_bundle(
      std::unique_ptr<ss::http::request>, std::unique_ptr<ss::http::reply>);
    ss::future<std::unique_ptr<ss::http::reply>> get_debug_bundle_file(
      std::unique_ptr<ss::http::request>, std::unique_ptr<ss::http::reply>);
    ss::future<std::unique_ptr<ss::http::reply>> delete_debug_bundle_file(
      std::unique_ptr<ss::http::request>, std::unique_ptr<ss::http::reply>);

    ss::future<> throw_on_error(
      ss::http::request& req,
      std::error_code ec,
      const model::ntp& ntp,
      model::node_id id = model::node_id{-1}) const;
    ss::future<ss::httpd::redirect_exception>
    redirect_to_leader(ss::http::request& req, const model::ntp& ntp) const;

    ss::future<ss::json::json_return_type> cancel_node_partition_moves(
      ss::http::request& req, cluster::partition_move_direction direction);

    struct level_reset {
        using time_point = ss::timer<>::clock::time_point;

        level_reset(ss::log_level level, std::optional<time_point> expires)
          : level(level)
          , expires(expires) {}

        ss::log_level level;
        std::optional<time_point> expires;
    };

    ss::timer<> _log_level_timer;
    absl::flat_hash_map<ss::sstring, level_reset> _log_level_resets;

    void rearm_log_level_timer();
    void log_level_timer_handler();

    ss::future<> restart_redpanda_service(service_kind service);

    ss::httpd::http_server _server;
    admin_server_cfg _cfg;
    ss::sharded<stress_fiber_manager>& _stress_fiber_manager;
    ss::sharded<cluster::partition_manager>& _partition_manager;
    ss::sharded<raft::group_manager>& _raft_group_manager;
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
    ss::sharded<storage::node>& _storage_node;
    ss::sharded<memory_sampling>& _memory_sampling_service;
    ss::sharded<cloud_storage::cache>& _cloud_storage_cache;
    ss::sharded<resources::cpu_profiler>& _cpu_profiler;
    ss::sharded<transform::service>* _transform_service;
    ss::sharded<security::audit::audit_log_manager>& _audit_mgr;
    std::unique_ptr<cluster::tx_manager_migrator>& _tx_manager_migrator;
    ss::sharded<kafka::server>& _kafka_server;
    ss::sharded<cluster::tx_gateway_frontend>& _tx_gateway_frontend;
    ss::sharded<debug_bundle::service>& _debug_bundle_service;
    ss::sharded<debug_bundle::file_handler> _debug_bundle_file_handler;

    // Value before the temporary override
    std::chrono::milliseconds _default_blocked_reactor_notify;
    ss::timer<> _blocked_reactor_notify_reset_timer;
};
