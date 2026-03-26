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

#include "redpanda/admin/server.h"

#include "base/vlog.h"
#include "bytes/iostream.h"
#include "cluster/controller.h"
#include "cluster/errc.h"
#include "cluster/fwd.h"
#include "cluster/members_table.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/topics_frontend.h"
#include "cluster/tx_gateway_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "config/endpoint_tls_config.h"
#include "json/document.h"
#include "metrics/metrics.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "net/dns.h"
#include "net/tls_certificate_probe.h"
#include "raft/types.h"
#include "redpanda/admin/api-doc/cluster_config.json.hh"
#include "redpanda/admin/api-doc/partition.json.hh"
#include "redpanda/admin/services/broker.h"
#include "redpanda/admin/util.h"
#include "rpc/errc.h"
#include "rpc/rpc_utils.h"
#include "security/audit/audit_log_manager.h"
#include "security/audit/schemas/iam.h"
#include "security/audit/schemas/types.h"
#include "security/audit/types.h"
#include "serde/protobuf/rpc.h"
#include "ssx/future-util.h"
#include "ssx/sformat.h"
#include "utils/unresolved_address.h"
#include "wasm/errc.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/prometheus.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/http/api_docs.hh>
#include <seastar/http/exception.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/json_path.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>
#include <seastar/http/url.hh>
#include <seastar/json/json_elements.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/net/tls.hh>
#include <seastar/util/log.hh>
#include <seastar/util/short_streams.hh>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/lexical_cast.hpp>
#include <fmt/core.h>

#include <chrono>
#include <exception>
#include <ranges>
#include <stdexcept>
#include <system_error>
#include <unordered_map>

using namespace std::chrono_literals;

ss::logger adminlog{"admin_api_server"};

static constexpr auto audit_svc_name = "Redpanda Admin HTTP Server";
static constexpr auto retry_after_seconds = 1;

namespace {

security::audit::authentication::used_cleartext
is_cleartext(const ss::sstring& protocol) {
    return boost::iequals(protocol, "https")
             ? security::audit::authentication::used_cleartext::no
             : security::audit::authentication::used_cleartext::yes;
}

security::audit::authentication_event_options make_authn_event_options(
  ss::httpd::const_req req, const request_auth_result& auth_result) {
    return {
      .auth_protocol = auth_result.get_sasl_mechanism(),
      .server_addr = net::unresolved_address{req.get_server_address()},
      .svc_name = audit_svc_name,
      .client_addr = net::unresolved_address{req.get_client_address()},
      .is_cleartext = is_cleartext(req.get_protocol_name()),
      .user = {
        .name = auth_result.get_username().empty() ? "{{anonymous}}"
                                                   : auth_result.get_username(),
        .type_id = auth_result.is_authenticated()
                     ? (auth_result.is_superuser()
                          ? security::audit::user::type::admin
                          : security::audit::user::type::user)
                     : security::audit::user::type::unknown,
        .groups = security::acl_principals_to_audit_groups(
          auth_result.get_groups())}};
}

security::audit::authentication_event_options make_authn_event_options(
  ss::httpd::const_req req,
  const security::credential_user& username,
  const ss::sstring& reason) {
    return {
      .server_addr = net::unresolved_address{req.get_server_address()},
      .svc_name = audit_svc_name,
      .client_addr = net::unresolved_address{req.get_client_address()},
      .is_cleartext = is_cleartext(req.get_protocol_name()),
      .user
      = {.name = username, .type_id = security::audit::user::type::unknown},
      .error_reason = reason};
}

std::string_view strip_query_param(const ss::sstring& url) {
    auto pos = url.find('?');
    if (pos == ss::sstring::npos) {
        return {url};
    }
    return {url.begin(), pos};
};

bool escape_hatch_request(ss::httpd::const_req req) {
    /// The following "break glass" mechanism allows the cluster config
    /// API to be hit in the case the user desires to disable auditing
    /// so the cluster can continue to make progress in the event auditing
    /// is not working as expected.
    static const auto allowed_requests = std::to_array(
      {ss::httpd::cluster_config_json::get_cluster_config_status,
       ss::httpd::cluster_config_json::get_cluster_config_schema,
       ss::httpd::cluster_config_json::patch_cluster_config,
       ss::httpd::cluster_config_json::get_cluster_config});

    return std::ranges::any_of(
      allowed_requests,
      [method = req._method,
       url = req._url](const ss::httpd::path_description& d) {
          return d.path == strip_query_param(url)
                 && d.operations.method == ss::httpd::str2type(method);
      });
}
} // namespace

model::ntp admin_server::parse_ntp_from_request(
  ss::httpd::parameters& param, model::ns ns) {
    auto topic = model::topic(param.get_decoded_param("topic"));

    model::partition_id partition;
    try {
        partition = model::partition_id(
          std::stoi(param.get_decoded_param("partition")));
    } catch (...) {
        throw ss::httpd::bad_param_exception(
          fmt::format(
            "Partition id must be an integer: {}",
            param.get_decoded_param("partition")));
    }

    if (partition() < 0) {
        throw ss::httpd::bad_param_exception(
          fmt::format("Invalid partition id {}", partition));
    }

    return {std::move(ns), std::move(topic), partition};
}

model::ntp admin_server::parse_ntp_from_request(ss::httpd::parameters& param) {
    return parse_ntp_from_request(
      param, model::ns(param.get_decoded_param("namespace")));
}

model::ntp admin_server::parse_ntp_from_query_param(
  const std::unique_ptr<ss::http::request>& req) {
    auto ns = req->get_query_param("namespace");
    auto topic = req->get_query_param("topic");
    auto partition_str = req->get_query_param("partition_id");
    model::partition_id partition;
    try {
        partition = model::partition_id(std::stoi(partition_str));
    } catch (...) {
        throw ss::httpd::bad_param_exception(
          fmt::format("Partition must be an integer: {}", partition_str));
    }

    if (partition() < 0) {
        throw ss::httpd::bad_param_exception(
          fmt::format("Invalid partition id {}", partition));
    }

    return {std::move(ns), std::move(topic), partition};
}

admin_server::admin_server(
  admin_server_cfg cfg,
  ss::sharded<stress_fiber_manager>& looper,
  ss::sharded<cluster::partition_manager>& pm,
  ss::sharded<raft::group_manager>& rgm,
  cluster::controller* controller,
  ss::sharded<cluster::shard_table>& st,
  ss::sharded<cluster::metadata_cache>& metadata_cache,
  ss::sharded<rpc::connection_cache>& connection_cache,
  ss::sharded<cluster::node_status_table>& node_status_table,
  ss::sharded<cluster::self_test_frontend>& self_test_frontend,
  ss::sharded<kafka::usage_manager>& usage_manager,
  pandaproxy::rest::api* http_proxy,
  pandaproxy::schema_registry::api* schema_registry,
  ss::sharded<cloud_storage::topic_recovery_service>& topic_recovery_svc,
  ss::sharded<cluster::topic_recovery_status_frontend>&
    topic_recovery_status_frontend,
  ss::sharded<storage::node>& storage_node,
  ss::sharded<memory_sampling>& memory_sampling_service,
  ss::sharded<cloud_io::cache>& cloud_storage_cache,
  ss::sharded<resources::cpu_profiler>& cpu_profiler,
  ss::sharded<transform::service>* transform_service,
  ss::sharded<security::audit::audit_log_manager>& audit_mgr,
  std::unique_ptr<cluster::tx_manager_migrator>& tx_manager_migrator,
  ss::sharded<kafka::server>& kafka_server,
  ss::sharded<cluster::tx_gateway_frontend>& tx_gateway_frontend,
  ss::sharded<debug_bundle::service>& debug_bundle_service,
  ss::sharded<admin::kafka_connections_service>& kafka_connections_service)
  : _log_level_timer([this] { log_level_timer_handler(); })
  , _server("admin")
  , _cfg(std::move(cfg))
  , _stress_fiber_manager(looper)
  , _partition_manager(pm)
  , _raft_group_manager(rgm)
  , _controller(controller)
  , _shard_table(st)
  , _metadata_cache(metadata_cache)
  , _connection_cache(connection_cache)
  , _auth(
      config::shard_local_cfg().admin_api_require_auth.bind(),
      config::shard_local_cfg().superusers.bind(),
      _controller)
  , _node_status_table(node_status_table)
  , _self_test_frontend(self_test_frontend)
  , _usage_manager(usage_manager)
  , _http_proxy(http_proxy)
  , _schema_registry(schema_registry)
  , _topic_recovery_service(topic_recovery_svc)
  , _topic_recovery_status_frontend(topic_recovery_status_frontend)
  , _storage_node(storage_node)
  , _memory_sampling_service(memory_sampling_service)
  , _cloud_storage_cache(cloud_storage_cache)
  , _cpu_profiler(cpu_profiler)
  , _transform_service(transform_service)
  , _audit_mgr(audit_mgr)
  , _tx_manager_migrator(tx_manager_migrator)
  , _kafka_server(kafka_server)
  , _tx_gateway_frontend(tx_gateway_frontend)
  , _debug_bundle_service(debug_bundle_service)
  , _kafka_connections_service(kafka_connections_service)
  , _default_blocked_reactor_notify(
      ss::engine().get_blocked_reactor_notify_ms())
  , _memory_semaphore(_cfg.max_memory_usage_bytes, "admin/server-mem") {
    _server.set_content_streaming(true);
    _server.set_keepalive_parameters(
      ss::net::tcp_keepalive_params{
        .idle = std::chrono::seconds{120},
        .interval = std::chrono::seconds{60},
        .count = 3,
      });
}

namespace {
class rpc_handler : public ss::httpd::handler_base {
public:
    rpc_handler(
      ss::noncopyable_function<request_auth_result(const ss::http::request&)>
        authenticate_request,
      serde::pb::rpc::route_descriptor descriptor)
      : _authenticate_request(std::move(authenticate_request))
      , _descriptor(std::move(descriptor)) {}

    ss::future<std::unique_ptr<ss::http::reply>> handle(
      const ss::sstring&,
      std::unique_ptr<ss::http::request> req,
      std::unique_ptr<ss::http::reply> rep) override {
        try {
            auto auth_result = check_authentication(*req);
            auto ctx = make_context(*req);
            ctx.set_value(std::move(auth_result));
            auto is_proto = ctx.content_type
                            == serde::pb::rpc::content_type::proto;
            iobuf request_payload = co_await extract_payload(std::move(req));
            iobuf reply_payload = co_await _descriptor.handler(
              std::move(ctx), std::move(request_payload));
            // Write the content type as binary, as seastar doesn't have an
            // application type for `application/proto`. We'll overwrite the
            // correct content-type in the next line.
            rep->write_body(
              "bin",
              [payload = std::move(reply_payload)](
                ss::output_stream<char>& writer) mutable {
                  return write_iobuf_to_output_stream(
                    payload.share(0, payload.size_bytes()), writer);
              });
            rep->set_content_type(
              is_proto ? "application/proto" : "application/json");
        } catch (const serde::pb::rpc::base_exception& e) {
            rep = e.handle(std::move(rep));
        } catch (...) {
            vlog(
              adminlog.warn,
              "Unhandled exception in RPC handler for {}/{}: {}",
              _descriptor.service_name,
              _descriptor.method_name,
              std::current_exception());
            rep = serde::pb::rpc::internal_exception().handle(std::move(rep));
        }
        rep->done();
        co_return rep;
    }

private:
    request_auth_result check_authentication(const ss::http::request& req) {
        try {
            return _authenticate_request(req);
        } catch (const ss::httpd::base_exception& e) {
            switch (e.status()) {
            case seastar::http::reply::status_type::unauthorized:
                throw serde::pb::rpc::unauthenticated_exception(e.str());
            case seastar::http::reply::status_type::forbidden:
                throw serde::pb::rpc::permission_denied_exception(e.str());
            case seastar::http::reply::status_type::service_unavailable:
                throw serde::pb::rpc::unavailable_exception(e.str());
            default:
                throw serde::pb::rpc::internal_exception(e.str());
            }
        } catch (...) {
            throw serde::pb::rpc::internal_exception();
        }
    }
    serde::pb::rpc::context make_context(const ss::http::request& req) {
        serde::pb::rpc::context ctx;
        auto ct = req.get_header("Content-Type");
        if (ct != "application/proto" && ct != "application/json") {
            throw serde::pb::rpc::unimplemented_exception(
              ss::format(
                "Only application/proto and application/json Content-Type is "
                "supported, got: {}",
                ct));
        }
        ctx.content_type = ct == "application/proto"
                             ? serde::pb::rpc::content_type::proto
                             : serde::pb::rpc::content_type::json;
        ctx.service_name = _descriptor.service_name;
        ctx.method_name = _descriptor.method_name;
        return ctx;
    }
    ss::future<iobuf> extract_payload(std::unique_ptr<ss::http::request> req) {
        constexpr auto max_request_size = 10_MiB;
        if (req->content_length > max_request_size) {
            throw serde::pb::rpc::invalid_argument_exception(
              "request too large");
        }
        // Despite the name, this does not read exactly, but up to.
        auto payload = co_await read_iobuf_exactly(
          *req->content_stream, max_request_size);
        if (!req->content_stream->eof()) {
            throw serde::pb::rpc::invalid_argument_exception(
              "request too large");
        }
        co_return payload;
    }

    ss::noncopyable_function<request_auth_result(const ss::http::request&)>
      _authenticate_request;
    serde::pb::rpc::route_descriptor _descriptor;
};
} // namespace

void admin_server::add_service(
  std::unique_ptr<serde::pb::rpc::base_service> service) {
    vlog(adminlog.debug, "Registering RPC service: {}", service->name());
    for (auto& route : service->all_routes()) {
        vlog(adminlog.debug, "Registering RPC route: {}", route.path);
        ss::httpd::path_description path{
          route.path,
          ss::httpd::operation_type::POST,
          route.path,
          /*path_parameters=*/{},
          /*mandatory_params=*/{},
        };
        ss::noncopyable_function<request_auth_result(const ss::http::request&)>
          auth_handler;
        switch (route.authz_level) {
        case serde::pb::rpc::authz_level::unauthenticated:
            auth_handler = [this](const ss::http::request& req) {
                auto auth_result = apply_auth<publik>(req);
                log_request(req, auth_result);
                return auth_result;
            };
            break;
        case serde::pb::rpc::authz_level::user:
            auth_handler = [this](const ss::http::request& req) {
                auto auth_result = apply_auth<user>(req);
                log_request(req, auth_result);
                return auth_result;
            };
            break;
        case serde::pb::rpc::authz_level::superuser:
            auth_handler = [this](const ss::http::request& req) {
                auto auth_result = apply_auth<superuser>(req);
                log_request(req, auth_result);
                return auth_result;
            };
            break;
        }
        path.set(
          _server._routes,
          new rpc_handler(std::move(auth_handler), std::move(route)));
    }
    _services.push_back(std::move(service));
}

ss::future<iobuf>
admin_server::handle_rpc_request(serde::pb::rpc::context ctx, iobuf buf) {
    auto it = std::ranges::find(
      _services,
      ctx.service_name,
      [](const std::unique_ptr<serde::pb::rpc::base_service>& service) {
          return service->name();
      });
    if (it == _services.end()) {
        throw serde::pb::rpc::unimplemented_exception(
          fmt::format("Service {} not found", ctx.service_name));
    }
    for (const auto& route : (*it)->all_routes()) {
        if (route.method_name == ctx.method_name) {
            return route.handler(std::move(ctx), std::move(buf));
        }
    }
    throw serde::pb::rpc::unimplemented_exception(
      fmt::format("Method {}.{} not found", ctx.service_name, ctx.method_name));
}

ss::future<> admin_server::start() {
    // NOTE: This isn't normally where services should be registered
    // but this service is special as it has some reflection based methods.
    admin::proxy::client client(
      _controller->self(), &_connection_cache, [this] {
          return _controller->get_members_table().local().node_ids();
      });
    add_service(
      std::make_unique<admin::broker_service_impl>(
        std::move(client), &_services));

    co_await _debug_bundle_file_handler.start();

    _blocked_reactor_notify_reset_timer.set_callback([this] {
        return ss::smp::invoke_on_all([ms = _default_blocked_reactor_notify] {
            ss::engine().update_blocked_reactor_notify_ms(ms);
        });
    });

    configure_metrics_route();
    configure_admin_routes();

    co_await configure_listeners();

    vlog(
      adminlog.info,
      "Started HTTP admin service listening at {}",
      _cfg.endpoints);
}

ss::future<> admin_server::stop() {
    _blocked_reactor_notify_reset_timer.cancel();
    vlog(adminlog.debug, "admin_server::stop() - timer cancelled");
    _memory_semaphore.broken();
    vlog(adminlog.debug, "admin_server::stop() - memory semaphore broken");
    co_await _server.stop();
    vlog(adminlog.debug, "admin_server::stop() - _server stopped");
    co_await _debug_bundle_file_handler.stop();
}

void admin_server::configure_admin_routes() {
    auto rb = ss::make_shared<ss::httpd::api_registry_builder20>(
      _cfg.admin_api_docs_dir, "/v1");

    auto insert_comma = [](ss::output_stream<char>& os) {
        return os.write(",\n");
    };
    rb->set_api_doc(_server._routes);
    rb->register_api_file(_server._routes, "header");
    rb->register_api_file(_server._routes, "config");
    rb->register_function(_server._routes, insert_comma);
    rb->register_api_file(_server._routes, "cluster_config");
    rb->register_function(_server._routes, insert_comma);
    rb->register_api_file(_server._routes, "raft");
    rb->register_function(_server._routes, insert_comma);
    rb->register_api_file(_server._routes, "kafka");
    rb->register_function(_server._routes, insert_comma);
    rb->register_api_file(_server._routes, "partition");
    rb->register_function(_server._routes, insert_comma);
    rb->register_api_file(_server._routes, "security");
    rb->register_function(_server._routes, insert_comma);
    rb->register_api_file(_server._routes, "status");
    rb->register_function(_server._routes, insert_comma);
    rb->register_api_file(_server._routes, "features");
    rb->register_function(_server._routes, insert_comma);
    rb->register_api_file(_server._routes, "hbadger");
    rb->register_function(_server._routes, insert_comma);
    rb->register_api_file(_server._routes, "broker");
    rb->register_function(_server._routes, insert_comma);
    rb->register_api_file(_server._routes, "transaction");
    rb->register_function(_server._routes, insert_comma);
    rb->register_api_file(_server._routes, "debug");
    rb->register_function(_server._routes, insert_comma);
    rb->register_api_file(_server._routes, "cluster");
    rb->register_function(_server._routes, insert_comma);
    rb->register_api_file(_server._routes, "transform");
    rb->register_function(_server._routes, insert_comma);
    rb->register_api_file(_server._routes, "debug_bundle");
    register_config_routes();
    register_cluster_config_routes();
    register_raft_routes();
    register_kafka_routes();
    register_security_routes();
    register_status_routes();
    register_features_routes();
    register_broker_routes();
    register_partition_routes();
    register_hbadger_routes();
    register_transaction_routes();
    register_debug_routes();
    register_usage_routes();
    register_self_test_routes();
    register_cluster_routes();
    register_shadow_indexing_routes();
    register_wasm_transform_routes();
    register_data_migration_routes();
    register_topic_routes();
    register_debug_bundle_routes();
    /**
     * Special REST apis active only in recovery mode
     */
    if (config::node().recovery_mode_enabled) {
        register_recovery_mode_routes();
    }
}

/**
 * A helper around rapidjson's Parse that checks for errors & raises
 * seastar HTTP exception.  Without that check, something as simple
 * as an empty request body causes a redpanda crash via a rapidjson
 * assertion when trying to GetObject on the resulting document.
 */
ss::future<json::Document>
admin_server::parse_json_body(ss::http::request* req) {
    json::Document doc;
    auto content = co_await ss::util::read_entire_stream_contiguous(
      *req->content_stream);
    doc.Parse(content);
    if (doc.HasParseError()) {
        throw ss::httpd::bad_request_exception(
          fmt::format("JSON parse error: {}", doc.GetParseError()));
    } else {
        co_return doc;
    }
}

ss::future<std::optional<json::Document>>
admin_server::parse_optional_json_body(ss::http::request* req) {
    json::Document doc;
    auto content = co_await ss::util::read_entire_stream_contiguous(
      *req->content_stream);
    if (content.empty()) {
        co_return std::nullopt;
    }
    doc.Parse(content);
    if (doc.HasParseError()) {
        throw ss::httpd::bad_request_exception(
          fmt::format("JSON parse error: {}", doc.GetParseError()));
    } else {
        co_return doc;
    }
}

namespace {

/**
 * Helper for requests with decimal_integer URL query parameters.
 *
 * Throws a bad_request exception if the parameter is present but not
 * an integer.
 */
std::optional<uint64_t>
get_integer_query_param(const ss::http::request& req, std::string_view key) {
    if (!req.has_query_param(key)) {
        return std::nullopt;
    }

    const ss::sstring& str_param = req.get_query_param(key);
    try {
        return std::stoull(str_param);
    } catch (const std::invalid_argument&) {
        throw ss::httpd::bad_request_exception(
          fmt::format("Parameter {} must be an integer", key));
    }
}

} // namespace

void admin_server::configure_metrics_route() {
    ss::prometheus::config private_config;
    private_config.prefix = "vectorized";
    private_config.handle = ss::metrics::default_handle();
    private_config.route = "/metrics";
    ss::prometheus::add_prometheus_routes(_server, private_config).get();

    ss::prometheus::config public_config;
    public_config.prefix = "redpanda";
    public_config.handle = metrics::public_metrics_handle;
    public_config.route = "/public_metrics";
    ss::prometheus::add_prometheus_routes(_server, public_config).get();
}

ss::future<> admin_server::configure_listeners() {
    // We will remember any endpoint that is listening
    // on an external address and does not have mTLS,
    // for emitting a warning later if user/pass auth is disabled.
    std::optional<model::broker_endpoint> insecure_ep;

    for (auto& ep : _cfg.endpoints) {
        // look for credentials matching current endpoint
        auto tls_it = std::find_if(
          _cfg.endpoints_tls.begin(),
          _cfg.endpoints_tls.end(),
          [&ep](const config::endpoint_tls_config& c) {
              return c.name == ep.name;
          });

        const bool localhost = ep.address.host() == "127.0.0.1"
                               || ep.address.host() == "localhost"
                               || ep.address.host() == "localhost.localdomain"
                               || ep.address.host() == "::1";

        ss::shared_ptr<ss::tls::server_credentials> cred;
        if (tls_it != _cfg.endpoints_tls.end()) {
            cred = co_await net::build_reloadable_server_credentials_with_probe(
              tls_it->config,
              "admin",
              tls_it->name,
              [](
                const std::unordered_set<ss::sstring>& updated,
                const std::exception_ptr& eptr) {
                  rpc::log_certificate_reload_event(
                    adminlog, "API TLS", updated, eptr);
              });
            if (!localhost && !tls_it->config.get_require_client_auth()) {
                insecure_ep = ep;
            }
        } else {
            if (!localhost) {
                insecure_ep = ep;
            }
        }

        auto resolved = co_await net::resolve_dns(ep.address);
        co_await ss::with_scheduling_group(_cfg.sg, [this, cred, resolved] {
            return _server.listen(resolved, cred);
        });
    }

    if (
      insecure_ep.has_value()
      && !config::shard_local_cfg().admin_api_require_auth()) {
        auto& ep = insecure_ep.value();
        vlog(
          adminlog.warn,
          "Insecure Admin API listener on {}:{}, consider enabling "
          "`admin_api_require_auth`",
          ep.address.host(),
          ep.address.port());
    }
}

void admin_server::audit_authz(
  ss::httpd::const_req req,
  const request_auth_result& auth_result,
  httpd_authorized authorized,
  std::optional<std::string_view> reason) {
    vlog(adminlog.trace, "Attempting to audit authz for {}", req.format_url());
    auto success = _audit_mgr.local().enqueue_api_activity_event(
      security::audit::event_type::admin,
      req,
      auth_result,
      audit_svc_name,
      bool(authorized),
      reason);
    if (!success) {
        bool is_allowed = escape_hatch_request(req);

        if (!is_allowed) {
            vlog(
              adminlog.error,
              "Failed to audit authorization request for endpoint: {}",
              req.format_url());
            throw ss::httpd::base_exception(
              "Failed to audit authorization request",
              ss::http::reply::status_type::service_unavailable);
        }

        vlog(
          adminlog.error,
          "Request to authorize user to modify or view cluster configuration "
          "was not audited due "
          "to audit queues being full");
    }
}

void admin_server::audit_authn(
  ss::httpd::const_req req, const request_auth_result& auth_result) {
    do_audit_authn(req, make_authn_event_options(req, auth_result));
}

void admin_server::audit_authn_failure(
  ss::httpd::const_req req,
  const security::credential_user& username,
  const ss::sstring& reason) {
    do_audit_authn(req, make_authn_event_options(req, username, reason));
}

void admin_server::do_audit_authn(
  ss::httpd::const_req req,
  security::audit::authentication_event_options options) {
    vlog(adminlog.trace, "Attempting to audit authn for {}", req.format_url());
    auto success = _audit_mgr.local().enqueue_authn_event(std::move(options));

    if (!success) {
        bool is_allowed = escape_hatch_request(req);

        if (!is_allowed) {
            vlog(
              adminlog.error,
              "Failed to audit authentication request for endpoint: {}",
              req.format_url());
            throw ss::httpd::base_exception(
              "Failed to audit authentication request",
              ss::http::reply::status_type::service_unavailable);
        }

        vlog(
          adminlog.error,
          "Request authenticate user to modify or view cluster configuration "
          "was not audited due "
          "to audit queues being full");
    }
}

void admin_server::log_request(
  const ss::http::request& req, const request_auth_result& auth_state) const {
    vlog(
      adminlog.debug,
      "[{}] {} {}",
      auth_state.get_username().size() > 0 ? auth_state.get_username()
                                           : "_anonymous",
      req._method,
      req.get_url());
}

void admin_server::log_exception(
  const ss::sstring& url,
  const request_auth_result& auth_state,
  std::exception_ptr eptr) const {
    using http_status = ss::http::reply::status_type;
    using http_status_ut = std::underlying_type_t<http_status>;
    const auto log_ex = [&](
                          std::optional<http_status_ut> status = std::nullopt) {
        std::stringstream os;
        const auto username
          = (auth_state.get_username().size() > 0 ? auth_state.get_username() : "_anonymous");
        /// Strip URL of query parameters in the case sensitive information
        /// might have been passed
        fmt::print(
          os,
          "[{}] exception intercepted - url: [{}]",
          username,
          url.substr(0, url.find('?')));
        if (status) {
            fmt::print(os, " http_return_status[{}]", *status);
        }
        fmt::print(os, " reason - {}", eptr);
        return os.str();
    };

    if (ssx::is_shutdown_exception(eptr)) {
        vlog(adminlog.debug, "{}", log_ex());
    } else {
        try {
            std::rethrow_exception(eptr);
        } catch (const ss::httpd::base_exception& ex) {
            const auto status = static_cast<http_status_ut>(ex.status());
            if (ex.status() == http_status::internal_server_error) {
                vlog(adminlog.error, "{}", log_ex(status));
            } else if (status >= 400) {
                vlog(adminlog.warn, "{}", log_ex(status));
            }
        } catch (...) {
            vlog(adminlog.error, "{}", log_ex());
        }
    }
}

void admin_server::rearm_log_level_timer() {
    vassert(
      ss::this_shard_id() == ss::shard_id{0},
      "Log levels should only be managed on shard 0");

    _log_level_timer.cancel();

    if (_log_level_resets.empty()) {
        return;
    }

    auto reset_values = _log_level_resets | std::views::values;
    auto& lvl_rst = *std::ranges::min_element(
      reset_values, std::less<>{}, [](const level_reset& l) {
          return l.expires.value_or(ss::timer<>::clock::time_point::max());
      });
    if (lvl_rst.expires.has_value()) {
        _log_level_timer.arm(lvl_rst.expires.value());
    }
}

void admin_server::log_level_timer_handler() {
    vassert(
      ss::this_shard_id() == ss::shard_id{0},
      "Log levels should only be managed on shard 0");
    std::ranges::for_each(_log_level_resets, [](auto& pr) {
        auto& [name, lr] = pr;
        if (lr.expires.has_value() && lr.expires <= ss::timer<>::clock::now()) {
            ss::global_logger_registry().set_logger_level(name, lr.level);
            vlog(
              adminlog.info,
              "Expiring log level for {{{}}} to {}",
              name,
              lr.level);
            // we've reset this logger to its default level, which
            // should never expire
            lr.expires.reset();
        }
    });
    rearm_log_level_timer();
}

ss::future<ss::httpd::redirect_exception> admin_server::redirect_to_leader(
  ss::http::request& req, const model::ntp& ntp) const {
    auto leader_id_opt = _metadata_cache.local().get_leader_id(ntp);

    if (!leader_id_opt.has_value()) {
        vlog(adminlog.info, "Can't redirect, no leader for ntp {}", ntp);

        throw ss::httpd::base_exception(
          fmt::format(
            "Partition {} does not have a leader, cannot redirect", ntp),
          ss::http::reply::status_type::service_unavailable);
    }

    if (leader_id_opt.value() == *config::node().node_id()) {
        vlog(
          adminlog.info,
          "Can't redirect to leader from leader node ({})",
          leader_id_opt.value());
        throw ss::httpd::base_exception(
          fmt::format("Leader not available"),
          ss::http::reply::status_type::service_unavailable);
    }

    auto leader_opt = _metadata_cache.local().get_node_metadata(
      leader_id_opt.value());
    if (!leader_opt.has_value()) {
        throw ss::httpd::base_exception(
          fmt::format(
            "Partition {} leader {} metadata not available",
            ntp,
            leader_id_opt.value()),
          ss::http::reply::status_type::service_unavailable);
    }
    auto leader = leader_opt.value();

    // Heuristic for finding peer's admin API interface that is accessible
    // from the client that sent this request:
    // - if the host in the Host header matches one of our advertised kafka
    //   addresses, then assume that the peer's advertised kafka address
    //   with the same index will also be their public admin API address.
    //   - a match in this case might be an exact match or the appearance
    //     that the advertised address is a subdomain of request hostname.
    //     this allows redirection of requests made through a headless service
    //     in a k8s environment, but is not intended to cover all such cases,
    //     since the service hostname could be anything. Context:
    //     https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/#pod-s-hostname-and-subdomain-fields
    // - Assume that the peer is listening on the same port that the client
    //   used to make this request (i.e. the port in Host)
    //
    // This will work reliably if all node configs have symmetric kafka listener
    // sections (i.e. all specify the same number of listeners in the same
    // order, for example all nodes have an internal and an external listener in
    // that order), and the hostname used for connecting to the admin API
    // matches one of the hostnames used for a kafka listener.
    //
    // The generic fallback if the heuristic fails is to use the peer's
    // internal RPC address.  This works if the user is e.g. connecting
    // by IP address to a k8s cluster's internal pod IP.

    auto host_hdr = req.get_header("host");

    std::string port;        // String like :123, or blank for default port
    std::string target_host; // Guessed admin API hostname of peer

    if (host_hdr.empty()) {
        vlog(
          adminlog.debug,
          "redirect: Missing Host header, falling back to internal RPC "
          "address");

        // Misbehaving client.  Guess peer address.
        port = fmt::format(
          ":{}", config::node_config().admin()[0].address.port());

    } else {
        // Assumption: the peer will be listening on the same port that this
        // request was sent to: parse the port out of the Host header
        auto colon = host_hdr.find(":");
        if (colon == ss::sstring::npos) {
            // Admin is being served on a standard port, leave port string blank
        } else {
            port = host_hdr.substr(colon);
        }

        auto req_hostname = host_hdr.substr(0, colon);

        // See if this hostname is one of our kafka advertised addresses
        auto kafka_endpoints = config::node().advertised_kafka_api();
        auto match_i = std::find_if(
          kafka_endpoints.begin(),
          kafka_endpoints.end(),
          [req_hostname](const model::broker_endpoint& be) {
              std::string_view be_host{be.address.host()};

              // exact match suggests that the request was directed to this
              // particular broker
              if (be_host == req_hostname) {
                  return true;
              }

              // otherwise if the advertised host appears to be a subdomain of
              // the request host, asume the request came through a headless
              // service and call that a match
              auto idx = be_host.find_first_of('.');
              if (
                idx == std::string_view::npos || idx + 1 == be_host.length()) {
                  return false;
              }
              return be_host.substr(idx + 1) == req_hostname;
          });

        if (match_i != kafka_endpoints.end()) {
            auto listener_idx = size_t(
              std::distance(kafka_endpoints.begin(), match_i));

            auto leader_advertised_addrs
              = leader.broker.kafka_advertised_listeners();
            if (leader_advertised_addrs.size() < listener_idx + 1) {
                vlog(
                  adminlog.debug,
                  "redirect: leader has no advertised address at matching "
                  "index for {}, "
                  "falling back to internal RPC address",
                  req_hostname);
                target_host = leader.broker.rpc_address().host();
            } else {
                target_host
                  = leader_advertised_addrs[listener_idx].address.host();
            }
        } else {
            vlog(
              adminlog.debug,
              "redirect: {} did not match any kafka listeners, redirecting to "
              "peer's internal RPC address",
              req_hostname);
            target_host = leader.broker.rpc_address().host();
        }
    }

    std::optional<int> retry_after = std::nullopt;
    static const ss::sstring redirect_str = "redirect";
    req._url = req.parse_query_param();

    // Check for redirect query parameter.
    const auto num_redirects
      = get_integer_query_param(req, redirect_str).value_or(0) + 1;

    // Add some backoff to the client request every other redirect.
    // In the case of two consecutive re-directs, it is clear that
    // leadership has not yet become consistent. We append backoff to
    // the client to let it settle before retrying. However, upon the
    // next request being made, it may result in a valid redirect to the
    // new leader, and no backoff should be added. This leads to adding
    // client backoff every other redirect.
    req.set_query_param(redirect_str, ss::to_sstring(num_redirects));
    if (num_redirects % 2 == 0) {
        retry_after = retry_after_seconds;
        vlog(
          adminlog.debug,
          "Multiple redirects ({}) detected. Setting Retry-After: {}",
          num_redirects,
          retry_after_seconds);
    }

    auto url = fmt::format(
      "{}://{}{}{}",
      req.get_protocol_name(),
      target_host,
      port,
      req.format_url());

    vlog(
      adminlog.info, "Redirecting admin API call to {} leader at {}", ntp, url);

    co_return ss::httpd::redirect_exception(
      url, ss::http::reply::status_type::temporary_redirect, retry_after);
}

bool admin_server::need_redirect_to_leader(
  model::ntp ntp, ss::sharded<cluster::metadata_cache>& metadata_cache) {
    auto leader_id_opt = metadata_cache.local().get_leader_id(ntp);
    if (!leader_id_opt.has_value()) {
        throw ss::httpd::base_exception(
          fmt::format(
            "Partition {} does not have a leader, cannot redirect", ntp),
          ss::http::reply::status_type::service_unavailable);
    }

    return leader_id_opt.value() != *config::node().node_id();
}

model::node_id admin_server::parse_broker_id(const ss::http::request& req) {
    try {
        return model::node_id(
          boost::lexical_cast<model::node_id::type>(req.get_path_param("id")));
    } catch (...) {
        throw ss::httpd::bad_param_exception(
          fmt::format(
            "Broker id: {}, must be an integer", req.get_path_param("id")));
    }
}

namespace {
ss::future<std::vector<ss::httpd::partition_json::partition_result>>
map_partition_results(std::vector<cluster::move_cancellation_result> results) {
    std::vector<ss::httpd::partition_json::partition_result> ret;
    ret.reserve(results.size());

    for (cluster::move_cancellation_result& r : results) {
        ss::httpd::partition_json::partition_result result;
        result.ns = std::move(r.ntp.ns)();
        result.topic = std::move(r.ntp.tp.topic)();
        result.partition = r.ntp.tp.partition;
        result.result = cluster::make_error_code(r.result).message();
        ret.push_back(std::move(result));
        co_await ss::maybe_yield();
    }
    co_return ret;
}

} // namespace

/**
 * Throw an appropriate seastar HTTP exception if we saw
 * a redpanda error during a request.
 *
 * @param ec  error code, may be from any subsystem
 * @param ntp on errors like not_leader, redirect to the leader of this NTP
 * @param id  optional node ID, for operations that acted on a particular
 *            node and would like it referenced in per-node cluster errors
 */
ss::future<> admin_server::throw_on_error(
  ss::http::request& req,
  std::error_code ec,
  const model::ntp& ntp,
  model::node_id id) const {
    if (!ec) {
        co_return;
    }

    if (ec.category() == cluster::error_category()) {
        switch (cluster::errc(ec.value())) {
        case cluster::errc::node_does_not_exists:
            throw ss::httpd::not_found_exception(
              fmt::format("broker with id {} not found", id));
        case cluster::errc::invalid_node_operation:
            throw ss::httpd::bad_request_exception(
              fmt::format(
                "can not update broker {} state, invalid state transition "
                "requested",
                id));
        case cluster::errc::timeout:
            throw ss::httpd::base_exception(
              fmt::format("Timeout: {}", ec.message()),
              ss::http::reply::status_type::gateway_timeout);
        case cluster::errc::replication_error:
        case cluster::errc::update_in_progress:
        case cluster::errc::leadership_changed:
        case cluster::errc::waiting_for_recovery:
        case cluster::errc::no_leader_controller:
        case cluster::errc::shutting_down:
            throw ss::httpd::base_exception(
              fmt::format("Service unavailable ({})", ec.message()),
              ss::http::reply::status_type::service_unavailable);
        case cluster::errc::not_leader:
            throw co_await redirect_to_leader(req, ntp);
        case cluster::errc::not_leader_controller:
            throw co_await redirect_to_leader(req, model::controller_ntp);
        case cluster::errc::no_update_in_progress:
            throw ss::httpd::bad_request_exception(
              "Cannot cancel partition move operation as there is no move "
              "in progress");
        case cluster::errc::throttling_quota_exceeded:
            throw ss::httpd::base_exception(
              fmt::format("Too many requests: {}", ec.message()),
              ss::http::reply::status_type::too_many_requests);
        case cluster::errc::topic_already_exists:
        case cluster::errc::topic_not_exists:
        case cluster::errc::transform_does_not_exist:
        case cluster::errc::transform_invalid_update:
        case cluster::errc::transform_invalid_create:
        case cluster::errc::transform_invalid_source:
        case cluster::errc::transform_invalid_environment:
        case cluster::errc::source_topic_not_exists:
        case cluster::errc::source_topic_still_in_use:
        case cluster::errc::invalid_partition_operation:
            throw ss::httpd::bad_request_exception(
              fmt::format("{}", ec.message()));
        case cluster::errc::transform_count_limit_exceeded: {
            const size_t max_transforms
              = config::shard_local_cfg()
                  .data_transforms_per_core_memory_reservation.value()
                / config::shard_local_cfg()
                    .data_transforms_per_function_memory_limit.value();
            throw ss::httpd::bad_request_exception(
              ss::format(
                "The limit of transforms has been reached ({}), more "
                "memory must be configured via {}",
                max_transforms,
                config::shard_local_cfg()
                  .data_transforms_per_core_memory_reservation.name()));
        }
        case cluster::errc::invalid_data_migration_state:
        case cluster::errc::data_migration_already_exists:
        case cluster::errc::data_migration_invalid_resources:
        case cluster::errc::data_migration_invalid_definition:
        case cluster::errc::data_migrations_disabled:
        case cluster::errc::resource_is_being_migrated:
            throw ss::httpd::bad_request_exception(
              fmt::format("{}", ec.message()));
        case cluster::errc::data_migration_not_exists:
            throw ss::httpd::base_exception(
              fmt::format("Data migration does not exist: {}", ec.message()),
              ss::http::reply::status_type::not_found);
        default:
            throw ss::httpd::server_error_exception(
              fmt::format("Unexpected cluster error: {}", ec.message()));
        }
    } else if (ec.category() == raft::error_category()) {
        switch (raft::errc(ec.value())) {
        case raft::errc::exponential_backoff:
        case raft::errc::disconnected_endpoint:
        case raft::errc::configuration_change_in_progress:
        case raft::errc::leadership_transfer_in_progress:
        case raft::errc::shutting_down:
        case raft::errc::replicated_entry_truncated:
            throw ss::httpd::base_exception(
              fmt::format("Not ready: {}", ec.message()),
              ss::http::reply::status_type::service_unavailable);
        case raft::errc::timeout:
            throw ss::httpd::base_exception(
              fmt::format("Timeout: {}", ec.message()),
              ss::http::reply::status_type::gateway_timeout);
        case raft::errc::transfer_to_current_leader:
            co_return;
        case raft::errc::not_leader:
            throw co_await redirect_to_leader(req, ntp);
        case raft::errc::node_does_not_exists:
        case raft::errc::not_voter:
            // node_does_not_exist is a 400 rather than a 404, because it
            // comes up in the context of a destination for leader transfer,
            // rather than a node ID appearing in a URL path.
            throw ss::httpd::bad_request_exception(
              fmt::format("Invalid request: {}", ec.message()));
        default:
            throw ss::httpd::server_error_exception(
              fmt::format("Unexpected raft error: {}", ec.message()));
        }
    } else if (ec.category() == cluster::tx::error_category()) {
        switch (cluster::tx::errc(ec.value())) {
        case cluster::tx::errc::leader_not_found:
            throw co_await redirect_to_leader(req, ntp);
        case cluster::tx::errc::pid_not_found:
            throw ss::httpd::not_found_exception(
              fmt_with_ctx(fmt::format, "Can not find pid for ntp:{}", ntp));
        case cluster::tx::errc::tx_id_not_found:
            throw ss::httpd::not_found_exception(fmt_with_ctx(
              fmt::format, "Unable to find requested transactional id"));
        case cluster::tx::errc::partition_not_found: {
            ss::sstring error_msg;
            if (
              ntp.tp.topic == model::tx_manager_topic
              && ntp.ns == model::kafka_internal_namespace) {
                error_msg = fmt::format("Can not find ntp:{}", ntp);
            } else {
                error_msg = fmt::format(
                  "Can not find partition({}) in transaction for delete", ntp);
            }
            throw ss::httpd::bad_request_exception(error_msg);
        }
        case cluster::tx::errc::not_coordinator:
            throw ss::httpd::base_exception(
              fmt::format(
                "Node not a coordinator or coordinator leader is not "
                "stabilized yet: {}",
                ec.message()),
              ss::http::reply::status_type::service_unavailable);
        case cluster::tx::errc::stale:
            throw ss::httpd::base_exception(
              fmt::format(
                "Stale request, check the transaction state before retrying: "
                "{}",
                ec.message()),
              ss::http::reply::status_type::unprocessable_entity);

        default:
            throw ss::httpd::server_error_exception(
              fmt::format("Unexpected tx_error error: {}", ec.message()));
        }
    } else if (ec.category() == rpc::error_category()) {
        switch (rpc::errc(ec.value())) {
        case rpc::errc::success:
            co_return;
        case rpc::errc::disconnected_endpoint:
        case rpc::errc::exponential_backoff:
        case rpc::errc::shutting_down:
        case rpc::errc::service_unavailable:
        case rpc::errc::missing_node_rpc_client:
            throw ss::httpd::base_exception(
              fmt::format("Not ready: {}", ec.message()),
              ss::http::reply::status_type::service_unavailable);
        case rpc::errc::client_request_timeout:
        case rpc::errc::connection_timeout:
            throw ss::httpd::base_exception(
              fmt::format("Timeout: {}", ec.message()),
              ss::http::reply::status_type::gateway_timeout);
        case rpc::errc::service_error:
        case rpc::errc::method_not_found:
        case rpc::errc::version_not_supported:
        case rpc::errc::unknown:
            throw ss::httpd::server_error_exception(
              fmt::format("Unexpected error: {}", ec.message()));
        }
    } else if (ec.category() == wasm::error_category()) {
        switch (wasm::errc(ec.value())) {
        case wasm::errc::invalid_module_missing_abi:
            throw ss::httpd::bad_request_exception(
              "Invalid WebAssembly - the binary is missing required transform "
              "functions. Check the broker support for the version of the Data "
              "Transforms SDK being used.");
        case wasm::errc::invalid_module_unsupported_sr:
            throw ss::httpd::bad_request_exception(
              "Invalid WebAssembly - the binary is using an unsupported Schema "
              "Registry client. Does the broker support this version of the "
              "Data Transforms Schema Registry SDK?");
        case wasm::errc::invalid_module_missing_wasi:
            throw ss::httpd::bad_request_exception(
              "invalid WebAssembly - missing required WASI functions");
        case wasm::errc::invalid_module:
            throw ss::httpd::bad_request_exception(
              "invalid WebAssembly module");
        default:
            throw ss::httpd::server_error_exception(
              fmt::format("Unexpected error: {}", ec.message()));
        }
    } else {
        throw ss::httpd::server_error_exception(
          fmt::format("Unexpected error: {}", ec.message()));
    }
}

ss::future<ss::json::json_return_type>
admin_server::cancel_node_partition_moves(
  ss::http::request& req, cluster::partition_move_direction direction) {
    auto node_id = parse_broker_id(req);
    auto res = co_await _controller->get_topics_frontend()
                 .local()
                 .cancel_moving_partition_replicas_node(
                   node_id, direction, model::timeout_clock::now() + 5s);

    if (res.has_error()) {
        co_await throw_on_error(
          req, res.error(), model::controller_ntp, node_id);
    }

    co_return ss::json::json_return_type(
      co_await map_partition_results(std::move(res.value())));
}

bool admin_server::str_to_bool(std::string_view s) {
    if (s == "0" || s == "false" || s == "False") {
        return false;
    } else {
        return true;
    }
}
