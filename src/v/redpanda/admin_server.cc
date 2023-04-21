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

#include "redpanda/admin_server.h"

#include "archival/ntp_archiver_service.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote_partition.h"
#include "cluster/cloud_storage_size_reducer.h"
#include "cluster/cluster_utils.h"
#include "cluster/config_frontend.h"
#include "cluster/controller.h"
#include "cluster/controller_api.h"
#include "cluster/controller_stm.h"
#include "cluster/errc.h"
#include "cluster/feature_manager.h"
#include "cluster/fwd.h"
#include "cluster/health_monitor_frontend.h"
#include "cluster/health_monitor_types.h"
#include "cluster/members_backend.h"
#include "cluster/members_frontend.h"
#include "cluster/members_table.h"
#include "cluster/metadata_cache.h"
#include "cluster/node_status_table.h"
#include "cluster/partition_balancer_backend.h"
#include "cluster/partition_balancer_rpc_service.h"
#include "cluster/partition_manager.h"
#include "cluster/security_frontend.h"
#include "cluster/self_test_frontend.h"
#include "cluster/shard_table.h"
#include "cluster/topic_recovery_service.h"
#include "cluster/topic_recovery_status_frontend.h"
#include "cluster/topic_recovery_status_rpc_handler.h"
#include "cluster/topics_frontend.h"
#include "cluster/tx_gateway_frontend.h"
#include "cluster/types.h"
#include "cluster_config_schema_util.h"
#include "config/configuration.h"
#include "config/endpoint_tls_config.h"
#include "features/feature_table.h"
#include "finjector/hbadger.h"
#include "json/document.h"
#include "json/schema.h"
#include "json/stringbuffer.h"
#include "json/validator.h"
#include "json/writer.h"
#include "kafka/server/usage_manager.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "net/dns.h"
#include "pandaproxy/rest/api.h"
#include "pandaproxy/schema_registry/api.h"
#include "raft/types.h"
#include "redpanda/admin/api-doc/broker.json.h"
#include "redpanda/admin/api-doc/cluster.json.h"
#include "redpanda/admin/api-doc/cluster_config.json.h"
#include "redpanda/admin/api-doc/config.json.h"
#include "redpanda/admin/api-doc/debug.json.h"
#include "redpanda/admin/api-doc/features.json.h"
#include "redpanda/admin/api-doc/hbadger.json.h"
#include "redpanda/admin/api-doc/partition.json.h"
#include "redpanda/admin/api-doc/raft.json.h"
#include "redpanda/admin/api-doc/security.json.h"
#include "redpanda/admin/api-doc/shadow_indexing.json.h"
#include "redpanda/admin/api-doc/status.json.h"
#include "redpanda/admin/api-doc/transaction.json.h"
#include "redpanda/admin/api-doc/usage.json.h"
#include "rpc/errc.h"
#include "security/acl.h"
#include "security/credential_store.h"
#include "security/scram_algorithm.h"
#include "security/scram_authenticator.h"
#include "security/scram_credential.h"
#include "ssx/future-util.h"
#include "ssx/metrics.h"
#include "utils/string_switch.h"
#include "utils/utf8.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/prometheus.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/http/api_docs.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>
#include <seastar/http/url.hh>
#include <seastar/util/log.hh>
#include <seastar/util/variant_utils.hh>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/lexical_cast/bad_lexical_cast.hpp>
#include <fmt/core.h>

#include <charconv>
#include <chrono>
#include <limits>
#include <stdexcept>
#include <system_error>
#include <type_traits>
#include <unordered_map>

using namespace std::chrono_literals;

static ss::logger logger{"admin_api_server"};

// Helpers for partition routes
namespace {

struct string_conversion_exception : public default_control_character_thrower {
    using default_control_character_thrower::default_control_character_thrower;
    [[noreturn]] [[gnu::cold]] void conversion_error() override {
        throw ss::httpd::bad_request_exception(
          "Parameter contained invalid control characters: "
          + get_sanitized_string());
    }
};

model::ntp parse_ntp_from_request(ss::httpd::parameters& param, model::ns ns) {
    auto topic = model::topic(param["topic"]);

    model::partition_id partition;
    try {
        partition = model::partition_id(std::stoi(param["partition"]));
    } catch (...) {
        throw ss::httpd::bad_param_exception(fmt::format(
          "Partition id must be an integer: {}", param["partition"]));
    }

    if (partition() < 0) {
        throw ss::httpd::bad_param_exception(
          fmt::format("Invalid partition id {}", partition));
    }

    return {std::move(ns), std::move(topic), partition};
}

model::ntp parse_ntp_from_request(ss::httpd::parameters& param) {
    return parse_ntp_from_request(param, model::ns(param["namespace"]));
}

model::ntp
parse_ntp_from_query_param(const std::unique_ptr<ss::http::request>& req) {
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

} // namespace

admin_server::admin_server(
  admin_server_cfg cfg,
  ss::sharded<cluster::partition_manager>& pm,
  ss::sharded<coproc::partition_manager>& cpm,
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
    topic_recovery_status_frontend)
  : _log_level_timer([this] { log_level_timer_handler(); })
  , _server("admin")
  , _cfg(std::move(cfg))
  , _partition_manager(pm)
  , _cp_partition_manager(cpm)
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
  , _default_blocked_reactor_notify(
      ss::engine().get_blocked_reactor_notify_ms()) {}

ss::future<> admin_server::start() {
    _blocked_reactor_notify_reset_timer.set_callback([this] {
        return ss::smp::invoke_on_all([ms = _default_blocked_reactor_notify] {
            ss::engine().update_blocked_reactor_notify_ms(ms);
        });
    });
    configure_metrics_route();
    configure_admin_routes();

    co_await configure_listeners();

    vlog(
      logger.info,
      "Started HTTP admin service listening at {}",
      _cfg.endpoints);
}

ss::future<> admin_server::stop() {
    _blocked_reactor_notify_reset_timer.cancel();
    return _server.stop();
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
}

static json::validator make_set_replicas_validator() {
    const std::string schema = R"(
{
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "node_id": {
                "type": "number"
            },
            "core": {
                "type": "number"
            }
        },
        "required": [
            "node_id",
            "core"
        ],
        "additionalProperties": false
    }
}
)";
    return json::validator(schema);
}

/**
 * A helper around rapidjson's Parse that checks for errors & raises
 * seastar HTTP exception.  Without that check, something as simple
 * as an empty request body causes a redpanda crash via a rapidjson
 * assertion when trying to GetObject on the resulting document.
 */
static json::Document parse_json_body(ss::http::request const& req) {
    json::Document doc;
    doc.Parse(req.content.data());
    if (doc.Parse(req.content.data()).HasParseError()) {
        throw ss::httpd::bad_request_exception(
          fmt::format("JSON parse error: {}", doc.GetParseError()));
    } else {
        return doc;
    }
}

/**
 * A helper to apply a schema validator to a request and on error,
 * string-ize any schema errors in the 400 response to help
 * caller see what went wrong.
 */
static void
apply_validator(json::validator& validator, json::Document const& doc) {
    validator.schema_validator.Reset();
    validator.schema_validator.ResetError();

    if (!doc.Accept(validator.schema_validator)) {
        json::StringBuffer val_buf;
        json::Writer<json::StringBuffer> w{val_buf};
        validator.schema_validator.GetError().Accept(w);
        auto s = ss::sstring{val_buf.GetString(), val_buf.GetSize()};
        throw ss::httpd::bad_request_exception(
          fmt::format("JSON request body does not conform to schema: {}", s));
    }
}

/**
 * Helper for requests with boolean URL query parameters that should
 * be treated as false if absent, or true if "true" (case insensitive) or "1"
 */
static bool
get_boolean_query_param(const ss::http::request& req, std::string_view name) {
    auto key = ss::sstring(name);
    if (!req.query_parameters.contains(key)) {
        return false;
    }

    const ss::sstring& str_param = req.query_parameters.at(key);
    return ss::http::request::case_insensitive_cmp()(str_param, "true")
           || str_param == "1";
}

void admin_server::configure_metrics_route() {
    ss::prometheus::add_prometheus_routes(
      _server,
      {.metric_help = "redpanda metrics",
       .prefix = "vectorized",
       .handle = ss::metrics::default_handle(),
       .route = "/metrics"})
      .get();
    ss::prometheus::add_prometheus_routes(
      _server,
      {.metric_help = "redpanda metrics",
       .prefix = "redpanda",
       .handle = ssx::metrics::public_metrics_handle,
       .route = "/public_metrics"})
      .get();
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
            auto builder = co_await tls_it->config.get_credentials_builder();
            if (builder) {
                cred = co_await builder->build_reloadable_server_credentials(
                  [](
                    const std::unordered_set<ss::sstring>& updated,
                    const std::exception_ptr& eptr) {
                      cluster::log_certificate_reload_event(
                        logger, "API TLS", updated, eptr);
                  });
            }

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
          logger.warn,
          "Insecure Admin API listener on {}:{}, consider enabling "
          "`admin_api_require_auth`",
          ep.address.host(),
          ep.address.port());
    }
}

void admin_server::log_request(
  const ss::http::request& req, const request_auth_result& auth_state) const {
    vlog(
      logger.debug,
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

    try {
        std::rethrow_exception(eptr);
    } catch (ss::httpd::base_exception& ex) {
        const auto status = static_cast<http_status_ut>(ex.status());
        if (ex.status() == http_status::internal_server_error) {
            vlog(logger.error, "{}", log_ex(status));
        } else if (status >= 400) {
            vlog(logger.warn, "{}", log_ex(status));
        }
    } catch (...) {
        vlog(logger.error, "{}", log_ex());
    }
}

void admin_server::rearm_log_level_timer() {
    _log_level_timer.cancel();

    auto next = std::min_element(
      _log_level_resets.begin(),
      _log_level_resets.end(),
      [](const auto& a, const auto& b) {
          return a.second.expires < b.second.expires;
      });

    if (next != _log_level_resets.end()) {
        _log_level_timer.arm(next->second.expires);
    }
}

void admin_server::log_level_timer_handler() {
    for (auto it = _log_level_resets.begin(); it != _log_level_resets.end();) {
        if (it->second.expires <= ss::timer<>::clock::now()) {
            vlog(
              logger.info,
              "Expiring log level for {{{}}} to {}",
              it->first,
              it->second.level);
            ss::global_logger_registry().set_logger_level(
              it->first, it->second.level);
            _log_level_resets.erase(it++);
        } else {
            ++it;
        }
    }
    rearm_log_level_timer();
}

ss::future<ss::httpd::redirect_exception> admin_server::redirect_to_leader(
  ss::http::request& req, model::ntp const& ntp) const {
    auto leader_id_opt = _metadata_cache.local().get_leader_id(ntp);

    if (!leader_id_opt.has_value()) {
        vlog(logger.info, "Can't redirect, no leader for ntp {}", ntp);

        throw ss::httpd::base_exception(
          fmt::format(
            "Partition {} does not have a leader, cannot redirect", ntp),
          ss::http::reply::status_type::service_unavailable);
    }

    if (leader_id_opt.value() == *config::node().node_id()) {
        vlog(
          logger.info,
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
          logger.debug,
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
          [req_hostname](model::broker_endpoint const& be) {
              return be.address.host() == req_hostname;
          });
        if (match_i != kafka_endpoints.end()) {
            auto listener_idx = size_t(
              std::distance(kafka_endpoints.begin(), match_i));

            auto leader_advertised_addrs
              = leader.broker.kafka_advertised_listeners();
            if (leader_advertised_addrs.size() < listener_idx + 1) {
                vlog(
                  logger.debug,
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
              logger.debug,
              "redirect: {} did not match any kafka listeners, redirecting to "
              "peer's internal RPC address",
              req_hostname);
            target_host = leader.broker.rpc_address().host();
        }
    }

    auto url = fmt::format(
      "{}://{}{}{}", req.get_protocol_name(), target_host, port, req._url);

    vlog(
      logger.info, "Redirecting admin API call to {} leader at {}", ntp, url);

    co_return ss::httpd::redirect_exception(
      url, ss::http::reply::status_type::temporary_redirect);
}

namespace {
bool need_redirect_to_leader(
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

model::node_id parse_broker_id(const ss::http::request& req) {
    try {
        return model::node_id(
          boost::lexical_cast<model::node_id::type>(req.param["id"]));
    } catch (...) {
        throw ss::httpd::bad_param_exception(
          fmt::format("Broker id: {}, must be an integer", req.param["id"]));
    }
}

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

ss::httpd::broker_json::maintenance_status fill_maintenance_status(
  const std::optional<cluster::drain_manager::drain_status>& status) {
    ss::httpd::broker_json::maintenance_status ret;
    if (status) {
        const auto& s = status.value();
        ret.draining = true;
        ret.finished = s.finished;
        ret.errors = s.errors;
        ret.partitions = s.partitions.value_or(0);
        ret.transferring = s.transferring.value_or(0);
        ret.eligible = s.eligible.value_or(0);
        ret.failed = s.failed.value_or(0);
    } else {
        ret.draining = false;
        // ensure that the output json has all fields
        ret.finished = false;
        ret.errors = false;
        ret.partitions = 0;
        ret.transferring = 0;
        ret.eligible = 0;
        ret.failed = 0;
    }
    return ret;
}

// Fetch brokers from the members table and enrich with
// metadata from the health monitor.
ss::future<std::vector<ss::httpd::broker_json::broker>>
get_brokers(cluster::controller* const controller) {
    cluster::node_report_filter filter;

    return controller->get_health_monitor()
      .local()
      .get_cluster_health(
        cluster::cluster_report_filter{
          .node_report_filter = std::move(filter),
        },
        cluster::force_refresh::no,
        model::no_timeout)
      .then([controller](result<cluster::cluster_health_report> h_report) {
          if (h_report.has_error()) {
              throw ss::httpd::base_exception(
                fmt::format(
                  "Unable to get cluster health: {}",
                  h_report.error().message()),
                ss::http::reply::status_type::service_unavailable);
          }

          std::map<model::node_id, ss::httpd::broker_json::broker> broker_map;

          // Collect broker information from the members table.
          auto& members_table = controller->get_members_table().local();
          for (auto& [id, nm] : members_table.nodes()) {
              ss::httpd::broker_json::broker b;
              b.node_id = id;
              b.num_cores = nm.broker.properties().cores;
              if (nm.broker.rack()) {
                  b.rack = *nm.broker.rack();
              }
              b.membership_status = fmt::format(
                "{}", nm.state.get_membership_state());

              // These fields are defaults that will be overwritten with
              // data from the health report.
              b.is_alive = true;
              b.maintenance_status = fill_maintenance_status(std::nullopt);

              b.internal_rpc_address = nm.broker.rpc_address().host();
              b.internal_rpc_port = nm.broker.rpc_address().port();

              broker_map[id] = b;
          }

          // Enrich the broker information with data from the health report.
          for (auto& ns : h_report.value().node_states) {
              auto it = broker_map.find(ns.id);
              if (it == broker_map.end()) {
                  continue;
              }

              it->second.is_alive = static_cast<bool>(ns.is_alive);

              auto r_it = std::find_if(
                h_report.value().node_reports.begin(),
                h_report.value().node_reports.end(),
                [id = ns.id](const cluster::node_health_report& nhr) {
                    return nhr.id == id;
                });

              if (r_it != h_report.value().node_reports.end()) {
                  it->second.version = r_it->local_state.redpanda_version;
                  it->second.maintenance_status = fill_maintenance_status(
                    r_it->drain_status);

                  auto add_disk = [&ds_list = it->second.disk_space](
                                    const storage::disk& ds) {
                      ss::httpd::broker_json::disk_space_info dsi;
                      dsi.path = ds.path;
                      dsi.free = ds.free;
                      dsi.total = ds.total;
                      ds_list.push(dsi);
                  };
                  add_disk(r_it->local_state.data_disk);
                  if (!r_it->local_state.shared_disk()) {
                      add_disk(r_it->local_state.get_cache_disk());
                  }
              }
          }

          std::vector<ss::httpd::broker_json::broker> brokers;
          brokers.reserve(broker_map.size());

          for (auto&& broker : broker_map) {
              brokers.push_back(std::move(broker.second));
          }

          return ss::make_ready_future<decltype(brokers)>(std::move(brokers));
      });
};

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
  model::ntp const& ntp,
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
            throw ss::httpd::bad_request_exception(fmt::format(
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
    } else if (ec.category() == cluster::tx_error_category()) {
        switch (cluster::tx_errc(ec.value())) {
        case cluster::tx_errc::leader_not_found:
            throw co_await redirect_to_leader(req, ntp);
        case cluster::tx_errc::pid_not_found:
            throw ss::httpd::not_found_exception(
              fmt_with_ctx(fmt::format, "Can not find pid for ntp:{}", ntp));
        case cluster::tx_errc::partition_not_found: {
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
        case cluster::tx_errc::not_coordinator:
            throw ss::httpd::base_exception(
              fmt::format(
                "Node not a coordinator or coordinator leader is not "
                "stabilized yet: {}",
                ec.message()),
              ss::http::reply::status_type::service_unavailable);

        default:
            throw ss::httpd::server_error_exception(
              fmt::format("Unexpected tx_error error: {}", ec.message()));
        }
    } else if (ec.category() == rpc::error_category()) {
        switch (rpc::errc(ec.value())) {
        case rpc::errc::success:
            co_return;
        case rpc::errc::disconnected_endpoint:
            [[fallthrough]];
        case rpc::errc::exponential_backoff:
            throw ss::httpd::base_exception(
              fmt::format("Not ready: {}", ec.message()),
              ss::http::reply::status_type::service_unavailable);
        case rpc::errc::client_request_timeout:
        case rpc::errc::connection_timeout:
            throw ss::httpd::base_exception(
              fmt::format("Timeout: {}", ec.message()),
              ss::http::reply::status_type::gateway_timeout);
        case rpc::errc::service_error:
        case rpc::errc::missing_node_rpc_client:
        case rpc::errc::method_not_found:
        case rpc::errc::version_not_supported:
        case rpc::errc::unknown:
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

bool str_to_bool(std::string_view s) {
    if (s == "0" || s == "false" || s == "False") {
        return false;
    } else {
        return true;
    }
}

void admin_server::register_config_routes() {
    register_route_raw_sync<superuser>(
      ss::httpd::config_json::get_config,
      [](ss::httpd::const_req, ss::http::reply& reply) {
          json::StringBuffer buf;
          json::Writer<json::StringBuffer> writer(buf);
          config::shard_local_cfg().to_json(
            writer, config::redact_secrets::yes);

          reply.set_status(ss::http::reply::status_type::ok, buf.GetString());
          return "";
      });

    register_route_raw_sync<superuser>(
      ss::httpd::cluster_config_json::get_cluster_config,
      [](ss::httpd::const_req req, ss::http::reply& reply) {
          json::StringBuffer buf;
          json::Writer<json::StringBuffer> writer(buf);

          bool include_defaults = true;
          auto include_defaults_str = req.get_query_param("include_defaults");
          if (!include_defaults_str.empty()) {
              include_defaults = str_to_bool(include_defaults_str);
          }

          config::shard_local_cfg().to_json(
            writer,
            config::redact_secrets::yes,
            [include_defaults](config::base_property& p) {
                return include_defaults || !p.is_default();
            });

          reply.set_status(ss::http::reply::status_type::ok, buf.GetString());
          return "";
      });

    register_route_raw_sync<superuser>(
      ss::httpd::config_json::get_node_config,
      [](ss::httpd::const_req, ss::http::reply& reply) {
          json::StringBuffer buf;
          json::Writer<json::StringBuffer> writer(buf);
          config::node().to_json(writer, config::redact_secrets::yes);

          reply.set_status(ss::http::reply::status_type::ok, buf.GetString());
          return "";
      });

    register_route_raw_sync<superuser>(
      ss::httpd::config_json::get_loggers,
      [](ss::httpd::const_req, ss::http::reply& reply) {
          json::StringBuffer buf;
          json::Writer<json::StringBuffer> writer(buf);
          writer.StartArray();
          for (const auto& name :
               ss::global_logger_registry().get_all_logger_names()) {
              writer.StartObject();
              writer.Key("name");
              writer.String(name);
              writer.EndObject();
          }
          writer.EndArray();
          reply.set_status(ss::http::reply::status_type::ok, buf.GetString());
          return "";
      });

    register_route<superuser>(
      ss::httpd::config_json::set_log_level,
      [this](std::unique_ptr<ss::http::request> req) {
          ss::sstring name;
          if (!ss::http::internal::url_decode(req->param["name"], name)) {
              throw ss::httpd::bad_param_exception(fmt::format(
                "Invalid parameter 'name' got {{{}}}", req->param["name"]));
          }
          validate_no_control(name, string_conversion_exception{name});

          // current level: will be used revert after a timeout (optional)
          ss::log_level cur_level;
          try {
              cur_level = ss::global_logger_registry().get_logger_level(name);
          } catch (std::out_of_range&) {
              throw ss::httpd::bad_param_exception(fmt::format(
                "Cannot set log level: unknown logger {{{}}}", name));
          }

          // decode new level
          ss::log_level new_level;
          try {
              new_level = boost::lexical_cast<ss::log_level>(
                req->get_query_param("level"));
          } catch (const boost::bad_lexical_cast& e) {
              throw ss::httpd::bad_param_exception(fmt::format(
                "Cannot set log level for {{{}}}: unknown level {{{}}}",
                name,
                req->get_query_param("level")));
          }

          // how long should the new log level be active
          std::optional<std::chrono::seconds> expires;
          if (auto e = req->get_query_param("expires"); !e.empty()) {
              try {
                  expires = std::chrono::seconds(
                    boost::lexical_cast<unsigned int>(e));
              } catch (const boost::bad_lexical_cast& e) {
                  throw ss::httpd::bad_param_exception(fmt::format(
                    "Cannot set log level for {{{}}}: invalid expires value "
                    "{{{}}}",
                    name,
                    e));
              }
          }

          vlog(
            logger.info,
            "Set log level for {{{}}}: {} -> {}",
            name,
            cur_level,
            new_level);

          ss::global_logger_registry().set_logger_level(name, new_level);

          if (!expires) {
              // if no expiration was given, then use some reasonable default
              // that will prevent the system from remaining in a non-optimal
              // state (e.g. trace logging) indefinitely.
              expires = std::chrono::minutes(10);
          }

          // expires=0 is same as not specifying it at all
          if (expires->count() > 0) {
              auto when = ss::timer<>::clock::now() + expires.value();
              auto res = _log_level_resets.try_emplace(name, cur_level, when);
              if (!res.second) {
                  res.first->second.expires = when;
              }
          } else {
              // perm change. no need to store prev level
              _log_level_resets.erase(name);
          }

          rearm_log_level_timer();

          return ss::make_ready_future<ss::json::json_return_type>(
            ss::json::json_void());
      });
}

static json::validator make_cluster_config_validator() {
    const std::string schema = R"(
{
    "type": "object",
    "properties": {
        "upsert": {
            "type": "object"
        },
        "remove": {
            "type": "array",
            "items": "string"
        }
    },
    "additionalProperties": false,
    "required": ["upsert", "remove"]
}
)";
    return json::validator(schema);
}

ss::sstring
join_properties(const std::vector<std::reference_wrapper<
                  const config::property<std::optional<ss::sstring>>>>& props) {
    ss::sstring result = "";
    for (size_t idx = 0; const auto& prop : props) {
        if (idx == props.size() - 1) {
            result += ss::sstring{prop.get().name()};
        } else {
            result += ssx::sformat("{}, ", prop.get().name());
        }

        ++idx;
    };

    return result;
}

/**
 * This function provides special case validation for configuration
 * properties that need to check other properties' values as well
 * as their own.
 *
 * Ideally this would be built into the config_store/property generic
 * interfaces, but that's a lot of plumbing for a few relatively simple
 * checks, so for the moment we just do the checks here by hand.
 */
void config_multi_property_validation(
  ss::sstring const& username,
  cluster::config_update_request const& req,
  config::configuration const& updated_config,
  std::map<ss::sstring, ss::sstring>& errors) {
    absl::flat_hash_set<ss::sstring> modified_keys;
    for (const auto& i : req.upsert) {
        modified_keys.insert(i.key);
    }

    if (
      (modified_keys.contains("admin_api_require_auth")
       || modified_keys.contains("superusers"))
      && updated_config.admin_api_require_auth()) {
        // We are switching on admin_api_require_auth.  Apply rules to prevent
        // the user "locking themselves out of the house".
        const bool auth_was_enabled
          = config::shard_local_cfg().admin_api_require_auth();

        // There must be some superusers defined
        const auto& superusers = updated_config.superusers();
        absl::flat_hash_set<ss::sstring> superusers_set(
          superusers.begin(), superusers.end());
        if (superusers.empty()) {
            // Some superusers must be defined, or nobody will be able
            // to use the admin API after this request.
            errors["admin_api_require_auth"] = "No superusers defined";
        } else if (!superusers_set.contains(username) && !auth_was_enabled) {
            // When enabling auth, user making the change must be in the list of
            // superusers, or they would be locking themselves out.
            errors["admin_api_require_auth"] = "May only be set by a superuser";
        }
    }

    if (updated_config.cloud_storage_enabled()) {
        // The properties that cloud_storage::configuration requires
        // to be set if cloud storage is enabled.
        using config_properties_seq = std::vector<std::reference_wrapper<
          const config::property<std::optional<ss::sstring>>>>;

        config_properties_seq properties{};

        if (
          updated_config.cloud_storage_credentials_source
          == model::cloud_credentials_source::config_file) {
            config_properties_seq s3_properties = {
              std::ref(updated_config.cloud_storage_region),
              std::ref(updated_config.cloud_storage_bucket),
              std::ref(updated_config.cloud_storage_access_key),
              std::ref(updated_config.cloud_storage_secret_key),
            };

            config_properties_seq abs_properties = {
              std::ref(updated_config.cloud_storage_azure_storage_account),
              std::ref(updated_config.cloud_storage_azure_container),
              std::ref(updated_config.cloud_storage_azure_shared_key),
            };

            std::array<config_properties_seq, 2> valid_configurations = {
              s3_properties, abs_properties};

            bool is_valid_configuration = std::any_of(
              valid_configurations.begin(),
              valid_configurations.end(),
              [](const auto& config) {
                  return std::none_of(
                    config.begin(), config.end(), [](const auto& prop) {
                        return prop() == std::nullopt;
                    });
              });

            if (!is_valid_configuration) {
                errors["cloud_storage_enabled"] = ssx::sformat(
                  "To enable cloud storage you need to configure S3 or Azure "
                  "Blob Storage access. For S3 {} must be set. For ABS {} "
                  "must be set",
                  join_properties(s3_properties),
                  join_properties(abs_properties));
            }
        } else {
            // TODO(vlad): When we add support for non-config file auth
            // methods for ABS, handling here should be updated too.
            config_properties_seq properties = {
              std::ref(updated_config.cloud_storage_region),
              std::ref(updated_config.cloud_storage_bucket),
            };

            for (auto& p : properties) {
                if (p() == std::nullopt) {
                    errors[ss::sstring(p.get().name())]
                      = "Must be set when cloud storage enabled";
                }
            }
        }
    }
}

void admin_server::register_cluster_config_routes() {
    register_route<superuser>(
      ss::httpd::cluster_config_json::get_cluster_config_status,
      [this](std::unique_ptr<ss::http::request>) {
          auto& cfg = _controller->get_config_manager();
          return cfg
            .invoke_on(
              cluster::controller_stm_shard,
              [](cluster::config_manager& manager) {
                  return manager.get_projected_status();
              })
            .then([](auto statuses) {
                std::vector<
                  ss::httpd::cluster_config_json::cluster_config_status>
                  res;

                for (const auto& s : statuses) {
                    vlog(logger.trace, "status: {}", s.second);
                    auto& rs = res.emplace_back();
                    rs.node_id = s.first;
                    rs.restart = s.second.restart;
                    rs.config_version = s.second.version;

                    // Workaround: seastar json_list hides empty lists by
                    // default.  This complicates API clients, so always push
                    // in a dummy element to get _set=true on json_list (this
                    // is then cleared in the subsequent operator=).
                    rs.invalid.push(ss::sstring("hack"));
                    rs.unknown.push(ss::sstring("hack"));

                    rs.invalid = s.second.invalid;
                    rs.unknown = s.second.unknown;
                }

                return ss::json::json_return_type(std::move(res));
            });
      });

    register_route<publik>(
      ss::httpd::cluster_config_json::get_cluster_config_schema,
      [](std::unique_ptr<ss::http::request>) {
          return ss::make_ready_future<ss::json::json_return_type>(
            util::generate_json_schema(config::shard_local_cfg()));
      });

    register_route<superuser, true>(
      ss::httpd::cluster_config_json::patch_cluster_config,
      [this](
        std::unique_ptr<ss::http::request> req,
        request_auth_result const& auth_state) {
          return patch_cluster_config_handler(std::move(req), auth_state);
      });
}

ss::future<ss::json::json_return_type>
admin_server::patch_cluster_config_handler(
  std::unique_ptr<ss::http::request> req,
  request_auth_result const& auth_state) {
    static thread_local auto cluster_config_validator(
      make_cluster_config_validator());

    if (!_controller->get_feature_table().local().is_active(
          features::feature::central_config)) {
        throw ss::httpd::bad_request_exception(
          "Central config feature not active (upgrade in progress?)");
    }

    auto doc = parse_json_body(*req);
    apply_validator(cluster_config_validator, doc);

    cluster::config_update_request update;

    // Deserialize removes
    const auto& json_remove = doc["remove"];
    for (const auto& v : json_remove.GetArray()) {
        update.remove.push_back(v.GetString());
    }

    // Deserialize upserts
    const auto& json_upsert = doc["upsert"];
    for (const auto& i : json_upsert.GetObject()) {
        // Re-serialize the individual value.  Our on-disk format
        // for property values is a YAML value (JSON is a subset
        // of YAML, so encoding with JSON is fine)
        json::StringBuffer val_buf;
        json::Writer<json::StringBuffer> w{val_buf};
        i.value.Accept(w);
        auto s = ss::sstring{val_buf.GetString(), val_buf.GetSize()};
        update.upsert.push_back({i.name.GetString(), s});
    }

    // Config property validation happens further down the line
    // at the point that properties are set on each node in
    // response to the deltas that we write to the controller log,
    // but we also do an early validation pass here to avoid writing
    // clearly wrong things into the log & give better feedback
    // to the API consumer.
    absl::flat_hash_set<ss::sstring> upsert_no_op_names;
    if (!get_boolean_query_param(*req, "force")) {
        // A scratch copy of configuration: we must not touch
        // the real live configuration object, that will be updated
        // by config_manager much after config is written to controller
        // log.
        config::configuration cfg;

        // Populate the temporary config object with existing values
        config::shard_local_cfg().for_each(
          [&cfg](const config::base_property& p) {
              auto& tmp_p = cfg.get(p.name());
              tmp_p = p;
          });

        // Configuration properties cannot do multi-property validation
        // themselves, so there is some special casing here for critical
        // properties.

        std::map<ss::sstring, ss::sstring> errors;
        for (const auto& [yaml_name, yaml_value] : update.upsert) {
            // Decode to a YAML object because that's what the property
            // interface expects.
            // Don't both catching ParserException: this was encoded
            // just a few lines above.
            auto val = YAML::Load(yaml_value);

            if (!cfg.contains(yaml_name)) {
                errors[yaml_name] = "Unknown property";
                continue;
            }
            auto& property = cfg.get(yaml_name);

            try {
                auto validation_err = property.validate(val);
                if (validation_err.has_value()) {
                    errors[yaml_name] = validation_err.value().error_message();
                    vlog(
                      logger.warn,
                      "Invalid {}: '{}' ({})",
                      yaml_name,
                      property.format_raw(yaml_value),
                      validation_err.value().error_message());
                } else {
                    // In case any property subclass might throw
                    // from it's value setter even after a non-throwing
                    // call to validate (if this happens validate() was
                    // implemented wrongly, but let's be safe)
                    auto changed = property.set_value(val);
                    if (!changed) {
                        upsert_no_op_names.insert(yaml_name);
                    }
                }
            } catch (YAML::BadConversion const& e) {
                // Be helpful, and give the user an example of what
                // the setting should look like, if we have one.
                ss::sstring example;
                auto example_opt = property.example();
                if (example_opt.has_value()) {
                    example = fmt::format(
                      ", for example '{}'", example_opt.value());
                }

                auto message = fmt::format(
                  "expected type {}{}", property.type_name(), example);

                // Special case: we get BadConversion for out-of-range
                // values on smaller integer sizes (e.g. too
                // large value to an int16_t property).
                // ("integer" is a magic string but it's a stable part
                //  of our outward interface)
                if (property.type_name() == "integer") {
                    int64_t n{0};
                    try {
                        n = val.as<int64_t>();
                        // It's a valid integer:
                        message = fmt::format("out of range: '{}'", n);
                    } catch (...) {
                        // This was not an out-of-bounds case, use
                        // the type error message
                    }
                }

                errors[yaml_name] = message;
                vlog(
                  logger.warn,
                  "Invalid {}: '{}' ({})",
                  yaml_name,
                  property.format_raw(yaml_value),
                  std::current_exception());
            } catch (...) {
                auto message = fmt::format("{}", std::current_exception());
                errors[yaml_name] = message;
                vlog(
                  logger.warn,
                  "Invalid {}: '{}' ({})",
                  yaml_name,
                  property.format_raw(yaml_value),
                  message);
            }
        }

        for (const auto& key : update.remove) {
            if (cfg.contains(key)) {
                cfg.get(key).reset();
            } else {
                errors[key] = "Unknown property";
            }
        }

        // After checking each individual property, check for
        // any multi-property validation errors
        config_multi_property_validation(
          auth_state.get_username(), update, cfg, errors);

        if (!errors.empty()) {
            json::StringBuffer buf;
            json::Writer<json::StringBuffer> w(buf);

            w.StartObject();
            for (const auto& e : errors) {
                w.Key(e.first.data(), e.first.size());
                w.String(e.second.data(), e.second.size());
            }
            w.EndObject();

            throw ss::httpd::base_exception(
              buf.GetString(),
              ss::http::reply::status_type::bad_request,
              "json");
        }
    }

    if (get_boolean_query_param(*req, "dry_run")) {
        auto current_version
          = co_await _controller->get_config_manager().invoke_on(
            cluster::config_manager::shard,
            [](cluster::config_manager& cm) { return cm.get_version(); });

        // A dry run doesn't really need a result, but it's simpler for
        // the API definition if we return the same structure as a
        // normal write.
        ss::httpd::cluster_config_json::cluster_config_write_result result;
        result.config_version = current_version;
        co_return ss::json::json_return_type(std::move(result));
    }

    if (
      update.upsert.size() == upsert_no_op_names.size()
      && update.remove.empty()) {
        vlog(
          logger.trace,
          "patch_cluster_config: ignoring request, {} upserts resulted "
          "in no-ops",
          update.upsert.size());
        auto current_version
          = co_await _controller->get_config_manager().invoke_on(
            cluster::config_manager::shard,
            [](cluster::config_manager& cm) { return cm.get_version(); });
        ss::httpd::cluster_config_json::cluster_config_write_result result;
        result.config_version = current_version;
        co_return ss::json::json_return_type(std::move(result));
    }

    vlog(
      logger.trace,
      "patch_cluster_config: {} upserts, {} removes",
      update.upsert.size(),
      update.remove.size());

    auto patch_result = co_await _controller->get_config_frontend().invoke_on(
      cluster::config_frontend::version_shard,
      [update = std::move(update)](cluster::config_frontend& fe) mutable {
          return fe.patch(std::move(update), model::timeout_clock::now() + 5s);
      });

    co_await throw_on_error(*req, patch_result.errc, model::controller_ntp);

    ss::httpd::cluster_config_json::cluster_config_write_result result;
    result.config_version = patch_result.version;
    co_return ss::json::json_return_type(std::move(result));
}

ss::future<ss::json::json_return_type>
admin_server::raft_transfer_leadership_handler(
  std::unique_ptr<ss::http::request> req) {
    raft::group_id group_id;
    try {
        group_id = raft::group_id(std::stoll(req->param["group_id"]));
    } catch (...) {
        throw ss::httpd::bad_param_exception(fmt::format(
          "Raft group id must be an integer: {}", req->param["group_id"]));
    }

    if (group_id() < 0) {
        throw ss::httpd::bad_param_exception(
          fmt::format("Invalid raft group id {}", group_id));
    }

    if (!_shard_table.local().contains(group_id)) {
        throw ss::httpd::not_found_exception(
          fmt::format("Raft group {} not found", group_id));
    }

    std::optional<model::node_id> target;
    if (auto node = req->get_query_param("target"); !node.empty()) {
        try {
            target = model::node_id(std::stoi(node));
        } catch (...) {
            throw ss::httpd::bad_param_exception(
              fmt::format("Target node id must be an integer: {}", node));
        }
        if (*target < 0) {
            throw ss::httpd::bad_param_exception(
              fmt::format("Invalid target node id {}", *target));
        }
    }

    vlog(
      logger.info,
      "Leadership transfer request for raft group {} to node {}",
      group_id,
      target);

    auto shard = _shard_table.local().shard_for(group_id);

    co_return co_await _partition_manager.invoke_on(
      shard,
      [group_id, target, this, req = std::move(req)](
        cluster::partition_manager& pm) mutable {
          auto partition = pm.partition_for(group_id);
          if (!partition) {
              throw ss::httpd::not_found_exception();
          }
          const auto ntp = partition->ntp();
          auto r = raft::transfer_leadership_request{
            .group = partition->group(),
            .target = target,
          };
          return partition->transfer_leadership(r).then(
            [this, ntp, req = std::move(req)](auto err) {
                return throw_on_error(*req, err, ntp).then([] {
                    return ss::json::json_return_type(ss::json::json_void());
                });
            });
      });
}

void admin_server::register_raft_routes() {
    register_route<superuser>(
      ss::httpd::raft_json::raft_transfer_leadership,
      [this](std::unique_ptr<ss::http::request> req) {
          return raft_transfer_leadership_handler(std::move(req));
      });
}

// TODO: factor out generic serialization from seastar http exceptions
static security::scram_credential
parse_scram_credential(const json::Document& doc) {
    if (!doc.IsObject()) {
        throw ss::httpd::bad_request_exception(fmt::format("Not an object"));
    }

    if (!doc.HasMember("algorithm") || !doc["algorithm"].IsString()) {
        throw ss::httpd::bad_request_exception(
          fmt::format("String algo missing"));
    }
    const auto algorithm = std::string_view(
      doc["algorithm"].GetString(), doc["algorithm"].GetStringLength());
    validate_no_control(algorithm, string_conversion_exception{algorithm});

    if (!doc.HasMember("password") || !doc["password"].IsString()) {
        throw ss::httpd::bad_request_exception(
          fmt::format("String password smissing"));
    }
    const auto password = doc["password"].GetString();
    validate_no_control(password, string_conversion_exception{"PASSWORD"});

    security::scram_credential credential;

    if (algorithm == security::scram_sha256_authenticator::name) {
        credential = security::scram_sha256::make_credentials(
          password, security::scram_sha256::min_iterations);

    } else if (algorithm == security::scram_sha512_authenticator::name) {
        credential = security::scram_sha512::make_credentials(
          password, security::scram_sha512::min_iterations);

    } else {
        throw ss::httpd::bad_request_exception(
          fmt::format("Unknown scram algorithm: {}", algorithm));
    }

    return credential;
}

static bool match_scram_credential(
  const json::Document& doc, const security::scram_credential& creds) {
    // Document is pre-validated via earlier parse_scram_credential call
    const auto password = ss::sstring(doc["password"].GetString());
    const auto algorithm = std::string_view(
      doc["algorithm"].GetString(), doc["algorithm"].GetStringLength());
    validate_no_control(algorithm, string_conversion_exception{algorithm});

    if (algorithm == security::scram_sha256_authenticator::name) {
        return security::scram_sha256::validate_password(
          password, creds.stored_key(), creds.salt(), creds.iterations());
    } else if (algorithm == security::scram_sha512_authenticator::name) {
        return security::scram_sha512::validate_password(
          password, creds.stored_key(), creds.salt(), creds.iterations());
    } else {
        throw ss::httpd::bad_request_exception(
          fmt::format("Unknown scram algorithm: {}", algorithm));
    }
}

bool is_no_op_user_write(
  security::credential_store& store,
  security::credential_user username,
  security::scram_credential credential) {
    auto user_opt = store.get<security::scram_credential>(username);
    if (user_opt.has_value()) {
        return user_opt.value() == credential;
    } else {
        return false;
    }
}

ss::future<ss::json::json_return_type>
admin_server::create_user_handler(std::unique_ptr<ss::http::request> req) {
    if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
        // In order that we can do a reliably ordered validation of
        // the request (and drop no-op requests), run on controller leader;
        throw co_await redirect_to_leader(*req, model::controller_ntp);
    }

    auto doc = parse_json_body(*req);

    auto credential = parse_scram_credential(doc);

    if (!doc.HasMember("username") || !doc["username"].IsString()) {
        throw ss::httpd::bad_request_exception(
          fmt::format("String username missing"));
    }

    auto username = security::credential_user(doc["username"].GetString());
    validate_no_control(username(), string_conversion_exception{username()});

    if (is_no_op_user_write(
          _controller->get_credential_store().local(), username, credential)) {
        vlog(
          logger.debug,
          "User {} already exists with matching credential",
          username);
        co_return ss::json::json_return_type(ss::json::json_void());
    }

    auto err
      = co_await _controller->get_security_frontend().local().create_user(
        username, credential, model::timeout_clock::now() + 5s);
    vlog(
      logger.debug, "Creating user '{}' {}:{}", username, err, err.message());

    if (err == cluster::errc::user_exists) {
        // Idempotency: if user is same as one that already exists,
        // suppress the user_exists error and return success.
        const auto& credentials_store
          = _controller->get_credential_store().local();
        std::optional<security::scram_credential> creds
          = credentials_store.get<security::scram_credential>(username);
        if (creds.has_value() && match_scram_credential(doc, creds.value())) {
            co_return ss::json::json_return_type(ss::json::json_void());
        }
    }

    co_await throw_on_error(*req, err, model::controller_ntp);
    co_return ss::json::json_return_type(ss::json::json_void());
}

ss::future<ss::json::json_return_type>
admin_server::delete_user_handler(std::unique_ptr<ss::http::request> req) {
    if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
        // In order that we can do a reliably ordered validation of
        // the request (and drop no-op requests), run on controller leader;
        throw co_await redirect_to_leader(*req, model::controller_ntp);
    }

    auto user = security::credential_user(req->param["user"]);

    if (!_controller->get_credential_store().local().contains(user)) {
        vlog(logger.debug, "User '{}' already gone during deletion", user);
        co_return ss::json::json_return_type(ss::json::json_void());
    }

    auto err
      = co_await _controller->get_security_frontend().local().delete_user(
        user, model::timeout_clock::now() + 5s);
    vlog(logger.debug, "Deleting user '{}' {}:{}", user, err, err.message());
    if (err == cluster::errc::user_does_not_exist) {
        // Idempotency: removing a non-existent user is successful.
        co_return ss::json::json_return_type(ss::json::json_void());
    }
    co_await throw_on_error(*req, err, model::controller_ntp);
    co_return ss::json::json_return_type(ss::json::json_void());
}

ss::future<ss::json::json_return_type>
admin_server::update_user_handler(std::unique_ptr<ss::http::request> req) {
    if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
        // In order that we can do a reliably ordered validation of
        // the request (and drop no-op requests), run on controller leader;
        throw co_await redirect_to_leader(*req, model::controller_ntp);
    }

    auto user = security::credential_user(req->param["user"]);

    auto doc = parse_json_body(*req);

    auto credential = parse_scram_credential(doc);

    if (is_no_op_user_write(
          _controller->get_credential_store().local(), user, credential)) {
        vlog(
          logger.debug,
          "User {} already exists with matching credential",
          user);
        co_return ss::json::json_return_type(ss::json::json_void());
    }

    auto err
      = co_await _controller->get_security_frontend().local().update_user(
        user, credential, model::timeout_clock::now() + 5s);
    vlog(logger.debug, "Updating user {}:{}", err, err.message());
    co_await throw_on_error(*req, err, model::controller_ntp);
    co_return ss::json::json_return_type(ss::json::json_void());
}

void admin_server::register_security_routes() {
    register_route<superuser>(
      ss::httpd::security_json::create_user,
      [this](std::unique_ptr<ss::http::request> req) {
          return create_user_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::security_json::delete_user,
      [this](std::unique_ptr<ss::http::request> req) {
          return delete_user_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::security_json::update_user,
      [this](std::unique_ptr<ss::http::request> req) {
          return update_user_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::security_json::list_users,
      [this](std::unique_ptr<ss::http::request> req) {
          bool include_ephemeral = req->get_query_param("include_ephemeral")
                                   == "true";
          constexpr auto is_ephemeral =
            [](security::credential_store::credential_types const& t) {
                return ss::visit(t, [](security::scram_credential const& c) {
                    return c.principal().has_value()
                           && c.principal().value().type()
                                == security::principal_type::ephemeral_user;
                });
            };

          std::vector<ss::sstring> users;
          for (const auto& [user, type] :
               _controller->get_credential_store().local()) {
              if (include_ephemeral || !is_ephemeral(type)) {
                  users.push_back(user());
              }
          }
          return ss::make_ready_future<ss::json::json_return_type>(
            std::move(users));
      });
}

ss::future<ss::json::json_return_type>
admin_server::kafka_transfer_leadership_handler(
  std::unique_ptr<ss::http::request> req) {
    auto ntp = parse_ntp_from_request(req->param);

    std::optional<model::node_id> target;
    if (auto node = req->get_query_param("target"); !node.empty()) {
        try {
            target = model::node_id(std::stoi(node));
        } catch (...) {
            throw ss::httpd::bad_param_exception(
              fmt::format("Target node id must be an integer: {}", node));
        }
        if (*target < 0) {
            throw ss::httpd::bad_param_exception(
              fmt::format("Invalid target node id {}", *target));
        }
    }

    vlog(
      logger.info,
      "Leadership transfer request for leader of topic-partition {} to node {}",
      ntp,
      target);

    auto shard = _shard_table.local().shard_for(ntp);
    if (!shard) {
        // This node is not a member of the raft group, redirect.
        throw co_await redirect_to_leader(*req, ntp);
    }

    co_return co_await _partition_manager.invoke_on(
      *shard,
      [ntp = std::move(ntp), target, this, req = std::move(req)](
        cluster::partition_manager& pm) mutable {
          auto partition = pm.get(ntp);
          if (!partition) {
              throw ss::httpd::not_found_exception();
          }
          auto r = raft::transfer_leadership_request{
            .group = partition->group(),
            .target = target,
          };
          return partition->transfer_leadership(r).then(
            [this, ntp, req = std::move(req)](auto err) {
                return throw_on_error(*req, err, ntp).then([] {
                    return ss::json::json_return_type(ss::json::json_void());
                });
            });
      });
}

void admin_server::register_kafka_routes() {
    register_route<superuser>(
      ss::httpd::partition_json::kafka_transfer_leadership,
      [this](std::unique_ptr<ss::http::request> req) {
          return kafka_transfer_leadership_handler(std::move(req));
      });
}

void admin_server::register_status_routes() {
    register_route<publik>(
      ss::httpd::status_json::ready,
      [this](std::unique_ptr<ss::http::request>) {
          std::unordered_map<ss::sstring, ss::sstring> status_map{
            {"status", _ready ? "ready" : "booting"}};
          return ss::make_ready_future<ss::json::json_return_type>(status_map);
      });
}

static json::validator make_feature_put_validator() {
    const std::string schema = R"(
{
    "type": "object",
    "properties": {
        "state": {
            "type": "string",
            "enum": ["active", "disabled"]
        }
    },
    "additionalProperties": false,
    "required": ["state"]
}
)";
    return json::validator(schema);
}

ss::future<ss::json::json_return_type>
admin_server::put_feature_handler(std::unique_ptr<ss::http::request> req) {
    static thread_local auto feature_put_validator(
      make_feature_put_validator());

    auto doc = parse_json_body(*req);
    apply_validator(feature_put_validator, doc);

    auto feature_name = req->param["feature_name"];

    if (!_controller->get_feature_table()
           .local()
           .resolve_name(feature_name)
           .has_value()) {
        throw ss::httpd::bad_request_exception("Unknown feature name");
    }

    cluster::feature_update_action action{.feature_name = feature_name};
    auto& new_state_str = doc["state"];
    if (new_state_str == "active") {
        action.action = cluster::feature_update_action::action_t::activate;
    } else if (new_state_str == "disabled") {
        action.action = cluster::feature_update_action::action_t::deactivate;
    } else {
        throw ss::httpd::bad_request_exception("Invalid state");
    }

    if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
        throw co_await redirect_to_leader(*req, model::controller_ntp);
    }

    auto& fm = _controller->get_feature_manager();
    auto err = co_await fm.invoke_on(
      cluster::feature_manager::backend_shard,
      [action](cluster::feature_manager& fm) {
          return fm.write_action(action);
      });
    if (err) {
        throw ss::httpd::bad_request_exception(fmt::format("{}", err));
    } else {
        co_return ss::json::json_void();
    }
}

ss::future<ss::json::json_return_type>
admin_server::put_license_handler(std::unique_ptr<ss::http::request> req) {
    auto& raw_license = req->content;
    if (raw_license.empty()) {
        throw ss::httpd::bad_request_exception(
          "Missing redpanda license from request body");
    }
    if (!_controller->get_feature_table().local().is_active(
          features::feature::license)) {
        throw ss::httpd::bad_request_exception(
          "Feature manager reports the cluster is not fully upgraded to "
          "accept license put requests");
    }

    try {
        boost::trim_if(raw_license, boost::is_any_of(" \n"));
        auto license = security::make_license(raw_license);
        if (license.is_expired()) {
            throw ss::httpd::bad_request_exception(
              fmt::format("License is expired: {}", license));
        }
        const auto& ft = _controller->get_feature_table().local();
        const auto& loaded_license = ft.get_license();
        if (loaded_license && (*loaded_license == license)) {
            /// Loaded license is idential to license in request, do
            /// nothing and return 200(OK)
            vlog(
              logger.info,
              "Attempted to load identical license, doing nothing: {}",
              license);
            co_return ss::json::json_void();
        }
        auto& fm = _controller->get_feature_manager();
        auto err = co_await fm.invoke_on(
          cluster::feature_manager::backend_shard,
          [license = std::move(license)](cluster::feature_manager& fm) mutable {
              return fm.update_license(std::move(license));
          });
        co_await throw_on_error(*req, err, model::controller_ntp);
    } catch (const security::license_malformed_exception& ex) {
        throw ss::httpd::bad_request_exception(
          fmt::format("License is malformed: {}", ex.what()));
    } catch (const security::license_invalid_exception& ex) {
        throw ss::httpd::bad_request_exception(
          fmt::format("License is invalid: {}", ex.what()));
    }
    co_return ss::json::json_void();
}

void admin_server::register_features_routes() {
    register_route<user>(
      ss::httpd::features_json::get_features,
      [this](std::unique_ptr<ss::http::request>) {
          ss::httpd::features_json::features_response res;

          const auto& ft = _controller->get_feature_table().local();
          auto version = ft.get_active_version();

          res.cluster_version = version;
          res.original_cluster_version = ft.get_original_version();
          res.node_earliest_version = ft.get_earliest_logical_version();
          res.node_latest_version = ft.get_latest_logical_version();
          for (const auto& fs : ft.get_feature_state()) {
              ss::httpd::features_json::feature_state item;
              vlog(
                logger.trace,
                "feature_state: {} {}",
                fs.spec.name,
                fs.get_state());
              item.name = ss::sstring(fs.spec.name);

              switch (fs.get_state()) {
              case features::feature_state::state::active:
                  item.state = ss::httpd::features_json::feature_state::
                    feature_state_state::active;
                  break;
              case features::feature_state::state::unavailable:
                  item.state = ss::httpd::features_json::feature_state::
                    feature_state_state::unavailable;
                  break;
              case features::feature_state::state::available:
                  item.state = ss::httpd::features_json::feature_state::
                    feature_state_state::available;
                  break;
              case features::feature_state::state::preparing:
                  item.state = ss::httpd::features_json::feature_state::
                    feature_state_state::preparing;
                  break;
              case features::feature_state::state::disabled_clean:
              case features::feature_state::state::disabled_active:
              case features::feature_state::state::disabled_preparing:
                  item.state = ss::httpd::features_json::feature_state::
                    feature_state_state::disabled;
                  break;
              }

              switch (fs.get_state()) {
              case features::feature_state::state::active:
              case features::feature_state::state::preparing:
              case features::feature_state::state::disabled_active:
              case features::feature_state::state::disabled_preparing:
                  item.was_active = true;
                  break;
              default:
                  item.was_active = false;
              }

              res.features.push(item);
          }

          return ss::make_ready_future<ss::json::json_return_type>(
            std::move(res));
      });

    register_route<superuser>(
      ss::httpd::features_json::put_feature,
      [this](std::unique_ptr<ss::http::request> req) {
          return put_feature_handler(std::move(req));
      });

    register_route<user>(
      ss::httpd::features_json::get_license,
      [this](std::unique_ptr<ss::http::request>) {
          if (!_controller->get_feature_table().local().is_active(
                features::feature::license)) {
              throw ss::httpd::bad_request_exception(
                "Feature manager reports the cluster is not fully upgraded to "
                "accept license get requests");
          }
          ss::httpd::features_json::license_response res;
          res.loaded = false;
          const auto& ft = _controller->get_feature_table().local();
          const auto& license = ft.get_license();
          if (license) {
              res.loaded = true;
              ss::httpd::features_json::license_contents lc;
              lc.format_version = license->format_version;
              lc.org = license->organization;
              lc.type = security::license_type_to_string(license->type);
              lc.expires = license->expiry.count();
              lc.sha256 = license->checksum;
              res.license = lc;
          }
          return ss::make_ready_future<ss::json::json_return_type>(
            std::move(res));
      });

    register_route<superuser>(
      ss::httpd::features_json::put_license,
      [this](std::unique_ptr<ss::http::request> req) {
          return put_license_handler(std::move(req));
      });
}

ss::future<ss::json::json_return_type>
admin_server::get_broker_handler(std::unique_ptr<ss::http::request> req) {
    model::node_id id = parse_broker_id(*req);
    auto node_meta = _metadata_cache.local().get_node_metadata(id);
    if (!node_meta) {
        throw ss::httpd::not_found_exception(
          fmt::format("broker with id: {} not found", id));
    }

    auto maybe_drain_status = co_await _controller->get_health_monitor()
                                .local()
                                .get_node_drain_status(
                                  id, model::time_from_now(5s));
    if (maybe_drain_status.has_error()) {
        throw ss::httpd::base_exception(
          fmt::format(
            "Unexpected error: {}", maybe_drain_status.error().message()),
          ss::http::reply::status_type::service_unavailable);
    }

    ss::httpd::broker_json::broker ret;
    ret.node_id = node_meta->broker.id();
    ret.internal_rpc_address = node_meta->broker.rpc_address().host();
    ret.internal_rpc_port = node_meta->broker.rpc_address().port();
    ret.num_cores = node_meta->broker.properties().cores;
    if (node_meta->broker.rack()) {
        ret.rack = node_meta->broker.rack().value();
    }
    ret.membership_status = fmt::format(
      "{}", node_meta->state.get_membership_state());
    ret.maintenance_status = fill_maintenance_status(
      maybe_drain_status.value());

    co_return ret;
}

ss::future<ss::json::json_return_type> admin_server::decomission_broker_handler(
  std::unique_ptr<ss::http::request> req) {
    model::node_id id = parse_broker_id(*req);

    auto ec
      = co_await _controller->get_members_frontend().local().decommission_node(
        id);

    co_await throw_on_error(*req, ec, model::controller_ntp, id);
    co_return ss::json::json_void();
}

ss::future<ss::json::json_return_type>
admin_server::get_decommission_progress_handler(
  std::unique_ptr<ss::http::request> req) {
    model::node_id id = parse_broker_id(*req);
    auto res
      = co_await _controller->get_api().local().get_node_decommission_progress(
        id, 5s + model::timeout_clock::now());
    if (!res) {
        if (res.error() == cluster::errc::node_does_not_exists) {
            throw ss::httpd::base_exception(
              fmt::format("Node {} does not exists", id),
              ss::http::reply::status_type::not_found);
        } else if (res.error() == cluster::errc::invalid_node_operation) {
            throw ss::httpd::base_exception(
              fmt::format("Node {} is not decommissioning", id),
              ss::http::reply::status_type::bad_request);
        }

        throw ss::httpd::base_exception(
          fmt::format(
            "Unable to get decommission status for {} - {}",
            id,
            res.error().message()),
          ss::http::reply::status_type::internal_server_error);
    }
    ss::httpd::broker_json::decommission_status ret;
    auto& decommission_progress = res.value();

    ret.replicas_left = decommission_progress.replicas_left;
    ret.finished = decommission_progress.finished;

    for (auto& p : decommission_progress.current_reconfigurations) {
        ss::httpd::broker_json::partition_reconfiguration_status status;
        status.ns = p.ntp.ns;
        status.topic = p.ntp.tp.topic;
        status.partition = p.ntp.tp.partition;
        auto added_replicas = cluster::subtract_replica_sets(
          p.current_assignment, p.previous_assignment);
        // we are only interested in reconfigurations where one replica was
        // added to the node
        if (added_replicas.size() != 1) {
            continue;
        }
        ss::httpd::broker_json::broker_shard moving_to{};
        moving_to.node_id = added_replicas.front().node_id();
        moving_to.core = added_replicas.front().shard;
        status.moving_to = moving_to;
        size_t left_to_move = 0;
        size_t already_moved = 0;
        for (auto replica_status : p.already_transferred_bytes) {
            left_to_move += (p.current_partition_size - replica_status.bytes);
            already_moved += replica_status.bytes;
        }
        status.bytes_left_to_move = left_to_move;
        status.bytes_moved = already_moved;
        status.partition_size = p.current_partition_size;
        // if no information from partitions is present yet, we may indicate
        // that everything have to be moved
        if (already_moved == 0 && left_to_move == 0) {
            status.bytes_left_to_move = p.current_partition_size;
        }
        ret.partitions.push(status);
    }

    co_return ret;
}

ss::future<ss::json::json_return_type> admin_server::recomission_broker_handler(
  std::unique_ptr<ss::http::request> req) {
    model::node_id id = parse_broker_id(*req);

    auto ec
      = co_await _controller->get_members_frontend().local().recommission_node(
        id);
    co_await throw_on_error(*req, ec, model::controller_ntp, id);
    co_return ss::json::json_void();
}

ss::future<ss::json::json_return_type>
admin_server::start_broker_maintenance_handler(
  std::unique_ptr<ss::http::request> req) {
    if (!_controller->get_feature_table().local().is_active(
          features::feature::maintenance_mode)) {
        throw ss::httpd::bad_request_exception(
          "Maintenance mode feature not active (upgrade in "
          "progress?)");
    }

    if (_controller->get_members_table().local().node_count() < 2) {
        throw ss::httpd::bad_request_exception(
          "Maintenance mode may not be used on a single node "
          "cluster");
    }

    model::node_id id = parse_broker_id(*req);
    auto ec = co_await _controller->get_members_frontend()
                .local()
                .set_maintenance_mode(id, true);
    co_await throw_on_error(*req, ec, model::controller_ntp, id);
    co_return ss::json::json_void();
}

ss::future<ss::json::json_return_type>
admin_server::stop_broker_maintenance_handler(
  std::unique_ptr<ss::http::request> req) {
    if (!_controller->get_feature_table().local().is_active(
          features::feature::maintenance_mode)) {
        throw ss::httpd::bad_request_exception(
          "Maintenance mode feature not active (upgrade in "
          "progress?)");
    }
    model::node_id id = parse_broker_id(*req);
    auto ec = co_await _controller->get_members_frontend()
                .local()
                .set_maintenance_mode(id, false);
    co_await throw_on_error(*req, ec, model::controller_ntp, id);
    co_return ss::json::json_void();
}

void admin_server::register_broker_routes() {
    register_route<user>(
      ss::httpd::broker_json::get_cluster_view,
      [this](std::unique_ptr<ss::http::request>) {
          return get_brokers(_controller)
            .then([this](std::vector<ss::httpd::broker_json::broker> brokers) {
                auto& members_table = _controller->get_members_table().local();

                ss::httpd::broker_json::cluster_view ret;
                ret.version = members_table.version();
                ret.brokers = std::move(brokers);

                return ss::json::json_return_type(std::move(ret));
            });
      });

    register_route<user>(
      ss::httpd::broker_json::get_brokers,
      [this](std::unique_ptr<ss::http::request>) {
          return get_brokers(_controller)
            .then([](std::vector<ss::httpd::broker_json::broker> brokers) {
                return ss::json::json_return_type(std::move(brokers));
            });
      });

    register_route<user>(
      ss::httpd::broker_json::get_broker,
      [this](std::unique_ptr<ss::http::request> req) {
          return get_broker_handler(std::move(req));
      });

    register_route<user>(
      ss::httpd::broker_json::get_decommission,
      [this](std::unique_ptr<ss::http::request> req) {
          return get_decommission_progress_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::broker_json::decommission,
      [this](std::unique_ptr<ss::http::request> req) {
          return decomission_broker_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::broker_json::recommission,
      [this](std::unique_ptr<ss::http::request> req) {
          return recomission_broker_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::broker_json::start_broker_maintenance,
      [this](std::unique_ptr<ss::http::request> req) {
          return start_broker_maintenance_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::broker_json::stop_broker_maintenance,
      [this](std::unique_ptr<ss::http::request> req) {
          return stop_broker_maintenance_handler(std::move(req));
      });

    /*
     * Unlike start|stop_broker_maintenace, the xxx_local_maintenance
     * versions below operate on local state only and could be used to force
     * a node out of maintenance mode if needed. they don't require the
     * feature flag because the feature is available locally.
     */
    register_route<superuser>(
      ss::httpd::broker_json::start_local_maintenance,
      [this](std::unique_ptr<ss::http::request>) {
          return _controller->get_drain_manager()
            .invoke_on_all(
              [](cluster::drain_manager& dm) { return dm.drain(); })
            .then(
              [] { return ss::json::json_return_type(ss::json::json_void()); });
      });

    register_route<superuser>(
      ss::httpd::broker_json::stop_local_maintenance,
      [this](std::unique_ptr<ss::http::request>) {
          return _controller->get_drain_manager()
            .invoke_on_all(
              [](cluster::drain_manager& dm) { return dm.restore(); })
            .then(
              [] { return ss::json::json_return_type(ss::json::json_void()); });
      });

    register_route<superuser>(
      ss::httpd::broker_json::get_local_maintenance,
      [this](std::unique_ptr<ss::http::request>) {
          return _controller->get_drain_manager().local().status().then(
            [](auto status) {
                ss::httpd::broker_json::maintenance_status res;
                res.draining = status.has_value();
                if (status.has_value()) {
                    res.finished = status->finished;
                    res.errors = status->errors;
                    if (status->partitions.has_value()) {
                        res.partitions = status->partitions.value();
                    }
                    if (status->eligible.has_value()) {
                        res.eligible = status->eligible.value();
                    }
                    if (status->transferring.has_value()) {
                        res.transferring = status->transferring.value();
                    }
                    if (status->failed.has_value()) {
                        res.failed = status->failed.value();
                    }
                }
                return ss::json::json_return_type(res);
            });
      });
    register_route<superuser>(
      ss::httpd::broker_json::cancel_partition_moves,
      [this](std::unique_ptr<ss::http::request> req) {
          return cancel_node_partition_moves(
            *req, cluster::partition_move_direction::all);
      });
}

ss::future<ss::json::json_return_type>
admin_server::get_transactions_handler(std::unique_ptr<ss::http::request> req) {
    const model::ntp ntp = parse_ntp_from_request(req->param);

    if (need_redirect_to_leader(ntp, _metadata_cache)) {
        throw co_await redirect_to_leader(*req, ntp);
    }

    auto shard = _shard_table.local().shard_for(ntp);
    // Strange situation, but need to check it
    if (!shard) {
        throw ss::httpd::server_error_exception(fmt_with_ctx(
          fmt::format, "Can not find shard for partition {}", ntp.tp));
    }

    co_return co_await _partition_manager.invoke_on(
      *shard,
      [ntp = std::move(ntp), req = std::move(req), this](
        cluster::partition_manager& pm) mutable {
          return get_transactions_inner_handler(
            pm, std::move(ntp), std::move(req));
      });
}

ss::future<ss::json::json_return_type>
admin_server::get_transactions_inner_handler(
  cluster::partition_manager& pm,
  model::ntp ntp,
  std::unique_ptr<ss::http::request> req) {
    auto partition = pm.get(ntp);
    if (!partition) {
        throw ss::httpd::server_error_exception(
          fmt_with_ctx(fmt::format, "Can not find partition {}", partition));
    }

    auto rm_stm_ptr = partition->rm_stm();

    if (!rm_stm_ptr) {
        throw ss::httpd::server_error_exception(fmt_with_ctx(
          fmt::format, "Can not get rm_stm for partition {}", partition));
    }

    auto transactions = co_await rm_stm_ptr->get_transactions();

    if (transactions.has_error()) {
        co_await throw_on_error(*req, transactions.error(), ntp);
    }
    ss::httpd::partition_json::transactions ans;

    auto offset_translator = partition->get_offset_translator_state();

    for (auto& [id, tx_info] : transactions.value()) {
        ss::httpd::partition_json::producer_identity pid;
        pid.id = id.get_id();
        pid.epoch = id.get_epoch();

        ss::httpd::partition_json::transaction new_tx;
        new_tx.producer_id = pid;
        new_tx.status = ss::sstring(tx_info.get_status());

        new_tx.lso_bound = offset_translator->from_log_offset(
          tx_info.lso_bound);

        auto staleness = tx_info.get_staleness();
        // -1 is returned for expired transaction, because how
        // long transaction do not do progress is useless for
        // expired tx.
        new_tx.staleness_ms
          = staleness.has_value()
              ? std::chrono::duration_cast<std::chrono::milliseconds>(
                  staleness.value())
                  .count()
              : -1;
        auto timeout = tx_info.get_timeout();
        // -1 is returned for expired transaction, because
        // timeout is useless for expired tx.
        new_tx.timeout_ms
          = timeout.has_value()
              ? std::chrono::duration_cast<std::chrono::milliseconds>(
                  timeout.value())
                  .count()
              : -1;

        if (tx_info.is_expired()) {
            ans.expired_transactions.push(new_tx);
        } else {
            ans.active_transactions.push(new_tx);
        }
    }

    co_return ss::json::json_return_type(ans);
}

ss::future<ss::json::json_return_type>
admin_server::mark_transaction_expired_handler(
  std::unique_ptr<ss::http::request> req) {
    const model::ntp ntp = parse_ntp_from_request(req->param);

    model::producer_identity pid;
    auto node = req->get_query_param("id");
    try {
        pid.id = std::stoi(node);
    } catch (...) {
        throw ss::httpd::bad_param_exception(fmt_with_ctx(
          fmt::format, "Transaction id must be an integer: {}", node));
    }
    node = req->get_query_param("epoch");
    try {
        int64_t epoch = std::stoi(node);
        if (
          epoch < std::numeric_limits<int16_t>::min()
          || epoch > std::numeric_limits<int16_t>::max()) {
            throw ss::httpd::bad_param_exception(
              fmt_with_ctx(fmt::format, "Invalid transaction epoch {}", epoch));
        }
        pid.epoch = epoch;
    } catch (...) {
        throw ss::httpd::bad_param_exception(fmt_with_ctx(
          fmt::format, "Transaction epoch must be an integer: {}", node));
    }

    vlog(logger.info, "Mark transaction expired for pid:{}", pid);

    if (need_redirect_to_leader(ntp, _metadata_cache)) {
        throw co_await redirect_to_leader(*req, ntp);
    }

    auto shard = _shard_table.local().shard_for(ntp);
    // Strange situation, but need to check it
    if (!shard) {
        throw ss::httpd::server_error_exception(fmt_with_ctx(
          fmt::format, "Can not find shard for partition {}", ntp.tp));
    }

    co_return co_await _partition_manager.invoke_on(
      *shard,
      [_ntp = std::move(ntp), pid, _req = std::move(req), this](
        cluster::partition_manager& pm) mutable
      -> ss::future<ss::json::json_return_type> {
          auto ntp = std::move(_ntp);
          auto req = std::move(_req);
          auto partition = pm.get(ntp);
          if (!partition) {
              return ss::make_exception_future<ss::json::json_return_type>(
                ss::httpd::server_error_exception(fmt_with_ctx(
                  fmt::format, "Can not find partition {}", partition)));
          }

          auto rm_stm_ptr = partition->rm_stm();

          if (!rm_stm_ptr) {
              return ss::make_exception_future<ss::json::json_return_type>(
                ss::httpd::server_error_exception(fmt_with_ctx(
                  fmt::format,
                  "Can not get rm_stm for partition {}",
                  partition)));
          }

          return rm_stm_ptr->mark_expired(pid).then(
            [this, ntp, req = std::move(req)](auto res) {
                return throw_on_error(*req, res, ntp).then([] {
                    return ss::json::json_return_type(ss::json::json_void());
                });
            });
      });
}

ss::future<ss::json::json_return_type>
admin_server::cancel_partition_reconfig_handler(
  std::unique_ptr<ss::http::request> req) {
    const auto ntp = parse_ntp_from_request(req->param);

    if (ntp == model::controller_ntp) {
        throw ss::httpd::bad_request_exception(
          fmt::format("Can't cancel controller reconfiguration"));
    }
    vlog(
      logger.debug,
      "Requesting cancelling of {} partition reconfiguration",
      ntp);

    auto err = co_await _controller->get_topics_frontend()
                 .local()
                 .cancel_moving_partition_replicas(
                   ntp,
                   model::timeout_clock::now()
                     + 10s); // NOLINT(cppcoreguidelines-avoid-magic-numbers)

    co_await throw_on_error(*req, err, model::controller_ntp);
    co_return ss::json::json_void();
}

ss::future<ss::json::json_return_type>
admin_server::unclean_abort_partition_reconfig_handler(
  std::unique_ptr<ss::http::request> req) {
    const auto ntp = parse_ntp_from_request(req->param);

    if (ntp == model::controller_ntp) {
        throw ss::httpd::bad_request_exception(
          "Can't unclean abort controller reconfiguration");
    }
    vlog(
      logger.warn,
      "Requesting unclean abort of {} partition reconfiguration",
      ntp);

    auto err = co_await _controller->get_topics_frontend()
                 .local()
                 .abort_moving_partition_replicas(
                   ntp,
                   model::timeout_clock::now()
                     + 10s); // NOLINT(cppcoreguidelines-avoid-magic-numbers)

    co_await throw_on_error(*req, err, model::controller_ntp);
    co_return ss::json::json_void();
}

ss::future<ss::json::json_return_type>
admin_server::set_partition_replicas_handler(
  std::unique_ptr<ss::http::request> req) {
    auto ntp = parse_ntp_from_request(req->param);

    if (ntp == model::controller_ntp) {
        throw ss::httpd::bad_request_exception(
          fmt::format("Can't reconfigure a controller"));
    }

    // make sure to call reset() before each use
    static thread_local json::validator set_replicas_validator(
      make_set_replicas_validator());

    auto doc = parse_json_body(*req);
    apply_validator(set_replicas_validator, doc);

    std::vector<model::broker_shard> replicas;
    if (!doc.IsArray()) {
        throw ss::httpd::bad_request_exception("Expected array");
    }
    for (auto& r : doc.GetArray()) {
        const auto& node_id_json = r["node_id"];
        const auto& core_json = r["core"];
        if (!node_id_json.IsInt() || !core_json.IsInt()) {
            throw ss::httpd::bad_request_exception(
              "`node_id` and `core` must be integers");
        }
        const auto node_id = model::node_id(r["node_id"].GetInt());
        const auto shard = static_cast<uint32_t>(r["core"].GetInt());

        // Validate node ID and shard - subsequent code assumes
        // they exist and may assert if not.
        bool is_valid
          = co_await _controller->get_topics_frontend().local().validate_shard(
            node_id, shard);
        if (!is_valid) {
            throw ss::httpd::bad_request_exception(fmt::format(
              "Replica set refers to non-existent node/shard (node "
              "{} "
              "shard {})",
              node_id,
              shard));
        }
        auto contains_already = std::find_if(
                                  replicas.begin(),
                                  replicas.end(),
                                  [node_id](const model::broker_shard& bs) {
                                      return bs.node_id == node_id;
                                  })
                                != replicas.end();
        if (contains_already) {
            throw ss::httpd::bad_request_exception(fmt::format(
              "All the replicas must be placed on separate nodes. "
              "Requested replica set contains node: {} more than "
              "once",
              node_id));
        }
        replicas.push_back(
          model::broker_shard{.node_id = node_id, .shard = shard});
    }

    auto current_assignment
      = _controller->get_topics_state().local().get_partition_assignment(ntp);

    // For a no-op change, just return success here, to avoid doing
    // all the raft writes and consensus restarts for a config change
    // that will do nothing.
    if (current_assignment && current_assignment->replicas == replicas) {
        vlog(
          logger.info,
          "Request to change ntp {} replica set to {}, no change",
          ntp,
          replicas);
        co_return ss::json::json_void();
    }

    vlog(
      logger.info, "Request to change ntp {} replica set to {}", ntp, replicas);

    auto err = co_await _controller->get_topics_frontend()
                 .local()
                 .move_partition_replicas(
                   ntp,
                   replicas,
                   model::timeout_clock::now()
                     + 10s); // NOLINT(cppcoreguidelines-avoid-magic-numbers)

    vlog(
      logger.debug,
      "Request to change ntp {} replica set to {}: err={}",
      ntp,
      replicas,
      err);

    co_await throw_on_error(*req, err, model::controller_ntp);
    co_return ss::json::json_void();
}

void admin_server::register_partition_routes() {
    /*
     * Get a list of partition summaries.
     */
    register_route<user>(
      ss::httpd::partition_json::get_partitions,
      [this](std::unique_ptr<ss::http::request>) {
          using summary = ss::httpd::partition_json::partition_summary;
          auto get_summaries =
            [](auto& partition_manager, bool materialized, auto get_leader) {
                return partition_manager.map_reduce0(
                  [materialized, get_leader](auto& pm) {
                      std::vector<summary> partitions;
                      partitions.reserve(pm.partitions().size());
                      for (const auto& it : pm.partitions()) {
                          summary p;
                          p.ns = it.first.ns;
                          p.topic = it.first.tp.topic;
                          p.partition_id = it.first.tp.partition;
                          p.core = ss::this_shard_id();
                          p.materialized = materialized;
                          p.leader = get_leader(it.second);
                          partitions.push_back(std::move(p));
                      }
                      return partitions;
                  },
                  std::vector<summary>{},
                  [](std::vector<summary> acc, std::vector<summary> update) {
                      acc.insert(acc.end(), update.begin(), update.end());
                      return acc;
                  });
            };
          auto f1 = get_summaries(_partition_manager, false, [](const auto& p) {
              return p->get_leader_id().value_or(model::node_id(-1))();
          });
          auto f2 = get_summaries(
            _cp_partition_manager, true, [](const auto&) { return -1; });
          return ss::when_all_succeed(std::move(f1), std::move(f2))
            .then([](auto summaries) {
                auto& [partitions, s2] = summaries;
                for (auto& s : s2) {
                    partitions.push_back(std::move(s));
                }
                return ss::json::json_return_type(std::move(partitions));
            });
      });

    register_route<user>(
      ss::httpd::partition_json::get_partitions_local_summary,
      [this](std::unique_ptr<ss::http::request>) {
          // This type mirrors partitions_local_summary, but satisfies
          // the seastar map_reduce requirement of being nothrow move
          // constructible.
          struct summary_t {
              uint64_t count{0};
              uint64_t leaderless{0};
              uint64_t under_replicated{0};
          };

          return _partition_manager
            .map_reduce0(
              [](auto& pm) {
                  summary_t s;
                  for (const auto& it : pm.partitions()) {
                      s.count += 1;
                      if (it.second->get_leader_id() == std::nullopt) {
                          s.leaderless += 1;
                      }
                      if (it.second->get_under_replicated() == std::nullopt) {
                          s.under_replicated += 1;
                      }
                  }
                  return s;
              },
              summary_t{},
              [](summary_t acc, summary_t update) {
                  acc.count += update.count;
                  acc.leaderless += update.leaderless;
                  acc.under_replicated += update.under_replicated;
                  return acc;
              })
            .then([](summary_t summary) {
                ss::httpd::partition_json::partitions_local_summary result;
                result.count = summary.count;
                result.leaderless = summary.leaderless;
                result.under_replicated = summary.under_replicated;
                return ss::json::json_return_type(std::move(result));
            });
      });
    register_route<user>(
      ss::httpd::partition_json::get_topic_partitions,
      [this](std::unique_ptr<ss::http::request> req) {
          return get_topic_partitions_handler(std::move(req));
      });

    /*
     * Get detailed information about a partition.
     */
    register_route<user>(
      ss::httpd::partition_json::get_partition,
      [this](std::unique_ptr<ss::http::request> req) {
          return get_partition_handler(std::move(req));
      });

    /*
     * Get detailed information about transactions for partition.
     */
    register_route<user>(
      ss::httpd::partition_json::get_transactions,
      [this](std::unique_ptr<ss::http::request> req) {
          return get_transactions_handler(std::move(req));
      });

    /*
     * Abort transaction for partition
     */
    register_route<superuser>(
      ss::httpd::partition_json::mark_transaction_expired,
      [this](std::unique_ptr<ss::http::request> req) {
          return mark_transaction_expired_handler(std::move(req));
      });
    register_route<superuser>(
      ss::httpd::partition_json::cancel_partition_reconfiguration,
      [this](std::unique_ptr<ss::http::request> req) {
          return cancel_partition_reconfig_handler(std::move(req));
      });
    register_route<superuser>(
      ss::httpd::partition_json::unclean_abort_partition_reconfiguration,
      [this](std::unique_ptr<ss::http::request> req) {
          return unclean_abort_partition_reconfig_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::partition_json::set_partition_replicas,
      [this](std::unique_ptr<ss::http::request> req) {
          return set_partition_replicas_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::partition_json::trigger_partitions_rebalance,
      [this](std::unique_ptr<ss::http::request> req) {
          return trigger_on_demand_rebalance_handler(std::move(req));
      });

    register_route<user>(
      ss::httpd::partition_json::get_partition_reconfigurations,
      [this](std::unique_ptr<ss::http::request>) {
          using reconfiguration = ss::httpd::partition_json::reconfiguration;
          std::vector<reconfiguration> ret;
          auto& in_progress
            = _controller->get_topics_state().local().updates_in_progress();

          ret.reserve(in_progress.size());
          for (auto& [ntp, status] : in_progress) {
              reconfiguration r;
              r.ns = ntp.ns;
              r.topic = ntp.tp.topic;
              r.partition = ntp.tp.partition;
              r.status = fmt::format("{}", status.get_state());

              for (auto& bs : status.get_previous_replicas()) {
                  ss::httpd::partition_json::assignment replica;
                  replica.node_id = bs.node_id;
                  replica.core = bs.shard;
                  r.previous_replicas.push(replica);
              }
              ret.push_back(std::move(r));
          }
          return ss::make_ready_future<ss::json::json_return_type>(ret);
      });
}
namespace {
ss::httpd::partition_json::partition
build_controller_partition(cluster::metadata_cache& cache) {
    ss::httpd::partition_json::partition p;
    p.ns = model::controller_ntp.ns;
    p.topic = model::controller_ntp.tp.topic;
    p.partition_id = model::controller_ntp.tp.partition;
    p.leader_id = -1;

    // Controller topic is on all nodes.  Report all nodes,
    // with the leader first.
    auto leader_opt = cache.get_controller_leader_id();
    if (leader_opt.has_value()) {
        ss::httpd::partition_json::assignment a;
        a.node_id = leader_opt.value();
        a.core = cluster::controller_stm_shard;
        p.replicas.push(a);
        p.leader_id = *leader_opt;
    }
    // special case, controller is raft group 0
    p.raft_group_id = 0;
    for (const auto& i : cache.node_ids()) {
        if (!leader_opt.has_value() || leader_opt.value() != i) {
            ss::httpd::partition_json::assignment a;
            a.node_id = i;
            a.core = cluster::controller_stm_shard;
            p.replicas.push(a);
        }
    }

    // Controller topic does not have a reconciliation state,
    // but include the field anyway to keep the API output
    // consistent.
    p.status = ssx::sformat("{}", cluster::reconciliation_status::done);
    return p;
}
} // namespace

ss::future<ss::json::json_return_type>
admin_server::get_partition_handler(std::unique_ptr<ss::http::request> req) {
    const model::ntp ntp = parse_ntp_from_request(req->param);
    const bool is_controller = ntp == model::controller_ntp;

    if (!is_controller && !_metadata_cache.local().contains(ntp)) {
        throw ss::httpd::not_found_exception(
          fmt::format("Could not find ntp: {}", ntp));
    }

    ss::httpd::partition_json::partition p;
    p.ns = ntp.ns;
    p.topic = ntp.tp.topic;
    p.partition_id = ntp.tp.partition;
    p.leader_id = -1;

    // Logic for fetching replicas+status is different for normal
    // topics vs. the special controller topic.
    if (is_controller) {
        return ss::make_ready_future<ss::json::json_return_type>(
          build_controller_partition(_metadata_cache.local()));

    } else {
        // Normal topic
        auto assignment
          = _controller->get_topics_state().local().get_partition_assignment(
            ntp);

        if (assignment) {
            for (auto& r : assignment->replicas) {
                ss::httpd::partition_json::assignment a;
                a.node_id = r.node_id;
                a.core = r.shard;
                p.replicas.push(a);
            }
            p.raft_group_id = assignment->group;
        }
        auto leader = _metadata_cache.local().get_leader_id(ntp);
        if (leader) {
            p.leader_id = *leader;
        }

        return _controller->get_api()
          .local()
          .get_reconciliation_state(ntp)
          .then([p](const cluster::ntp_reconciliation_state& state) mutable {
              p.status = ssx::sformat("{}", state.status());
              return ss::json::json_return_type(std::move(p));
          });
    }
}
ss::future<ss::json::json_return_type>
admin_server::get_topic_partitions_handler(
  std::unique_ptr<ss::http::request> req) {
    model::topic_namespace tp_ns(
      model::ns(req->param["namespace"]), model::topic(req->param["topic"]));
    const bool is_controller_topic = tp_ns.ns == model::controller_ntp.ns
                                     && tp_ns.tp
                                          == model::controller_ntp.tp.topic;

    // Logic for fetching replicas+status is different for normal
    // topics vs. the special controller topic.
    if (is_controller_topic) {
        co_return ss::json::json_return_type(
          build_controller_partition(_metadata_cache.local()));
    }

    auto tp_md = _metadata_cache.local().get_topic_metadata_ref(tp_ns);
    if (!tp_md) {
        throw ss::httpd::not_found_exception(
          fmt::format("Could not find topic: {}/{}", tp_ns.ns, tp_ns.tp));
    }
    using partition_t = ss::httpd::partition_json::partition;
    std::vector<partition_t> partitions;
    const auto& assignments = tp_md->get().get_assignments();
    partitions.reserve(assignments.size());
    // Normal topic
    for (const auto& p_as : assignments) {
        partition_t p;
        p.ns = tp_ns.ns;
        p.topic = tp_ns.tp;
        p.partition_id = p_as.id;
        p.raft_group_id = p_as.group;
        for (auto& r : p_as.replicas) {
            ss::httpd::partition_json::assignment a;
            a.node_id = r.node_id;
            a.core = r.shard;
            p.replicas.push(a);
        }
        auto leader = _metadata_cache.local().get_leader_id(tp_ns, p_as.id);
        if (leader) {
            p.leader_id = *leader;
        }
        partitions.push_back(std::move(p));
    }

    co_await ss::max_concurrent_for_each(
      partitions, 32, [this, &tp_ns](partition_t& p) {
          return _controller->get_api()
            .local()
            .get_reconciliation_state(model::ntp(
              tp_ns.ns, tp_ns.tp, model::partition_id(p.partition_id())))
            .then([&p](const cluster::ntp_reconciliation_state& state) mutable {
                p.status = ssx::sformat("{}", state.status());
            });
      });

    co_return ss::json::json_return_type(partitions);
}

ss::future<ss::json::json_return_type>
admin_server::trigger_on_demand_rebalance_handler(
  std::unique_ptr<ss::http::request> req) {
    auto ec = co_await _controller->get_members_backend().invoke_on(
      cluster::controller_stm_shard, [](cluster::members_backend& backend) {
          return backend.request_rebalance();
      });

    co_await throw_on_error(*req, ec, model::controller_ntp);
    co_return ss::json::json_return_type(ss::json::json_void());
}

void admin_server::register_hbadger_routes() {
    /**
     * we always register `v1/failure-probes` route. It will ALWAYS return
     * empty list of probes in production mode, and flag indicating that
     * honey badger is disabled
     */

    if constexpr (!finjector::honey_badger::is_enabled()) {
        register_route<user>(
          ss::httpd::hbadger_json::get_failure_probes,
          [](std::unique_ptr<ss::http::request>) {
              ss::httpd::hbadger_json::failure_injector_status status;
              status.enabled = false;
              return ss::make_ready_future<ss::json::json_return_type>(
                std::move(status));
          });
        return;
    }

    register_route<user>(
      ss::httpd::hbadger_json::get_failure_probes,
      [](std::unique_ptr<ss::http::request>) {
          auto modules = finjector::shard_local_badger().modules();
          ss::httpd::hbadger_json::failure_injector_status status;
          status.enabled = true;

          for (auto& m : modules) {
              ss::httpd::hbadger_json::failure_probes pr;
              pr.module = m.first.data();
              for (auto& p : m.second) {
                  pr.points.push(p.data());
              }
              status.probes.push(pr);
          }

          return ss::make_ready_future<ss::json::json_return_type>(
            std::move(status));
      });
    /*
     * Enable failure injector
     */
    static constexpr std::string_view delay_type = "delay";
    static constexpr std::string_view exception_type = "exception";
    static constexpr std::string_view terminate_type = "terminate";

    register_route<superuser>(
      ss::httpd::hbadger_json::set_failure_probe,
      [](std::unique_ptr<ss::http::request> req) {
          auto m = req->param["module"];
          auto p = req->param["point"];
          auto type = req->param["type"];
          vlog(
            logger.info,
            "Request to set failure probe of type '{}' in  '{}' at point "
            "'{}'",
            type,
            m,
            p);
          auto f = ss::now();

          if (type == delay_type) {
              f = ss::smp::invoke_on_all(
                [m, p] { finjector::shard_local_badger().set_delay(m, p); });
          } else if (type == exception_type) {
              f = ss::smp::invoke_on_all([m, p] {
                  finjector::shard_local_badger().set_exception(m, p);
              });
          } else if (type == terminate_type) {
              f = ss::smp::invoke_on_all([m, p] {
                  finjector::shard_local_badger().set_termination(m, p);
              });
          } else {
              throw ss::httpd::bad_param_exception(fmt::format(
                "Type parameter has to be one of "
                "['{}','{}','{}']",
                delay_type,
                exception_type,
                terminate_type));
          }

          return f.then(
            [] { return ss::json::json_return_type(ss::json::json_void()); });
      });
    /*
     * Remove all failure injectors at given point
     */
    register_route<superuser>(
      ss::httpd::hbadger_json::delete_failure_probe,
      [](std::unique_ptr<ss::http::request> req) {
          auto m = req->param["module"];
          auto p = req->param["point"];
          vlog(
            logger.info,
            "Request to unset failure probe '{}' at point '{}'",
            m,
            p);
          return ss::smp::invoke_on_all(
                   [m, p] { finjector::shard_local_badger().unset(m, p); })
            .then(
              [] { return ss::json::json_return_type(ss::json::json_void()); });
      });
}

ss::future<ss::json::json_return_type>
admin_server::get_all_transactions_handler(
  std::unique_ptr<ss::http::request> req) {
    if (!config::shard_local_cfg().enable_transactions) {
        throw ss::httpd::bad_request_exception("Transaction are disabled");
    }

    auto coordinator_partition_str = req->get_query_param(
      "coordinator_partition_id");
    int64_t coordinator_partition;
    try {
        coordinator_partition = std::stoi(coordinator_partition_str);
    } catch (...) {
        throw ss::httpd::bad_param_exception(fmt::format(
          "Partition must be an integer: {}", coordinator_partition_str));
    }

    if (coordinator_partition < 0) {
        throw ss::httpd::bad_param_exception(fmt::format(
          "Invalid coordinator partition {}", coordinator_partition));
    }

    model::ntp tx_ntp(
      model::tx_manager_nt.ns,
      model::tx_manager_nt.tp,
      model::partition_id(coordinator_partition));

    if (need_redirect_to_leader(tx_ntp, _metadata_cache)) {
        throw co_await redirect_to_leader(*req, tx_ntp);
    }

    auto& tx_frontend = _partition_manager.local().get_tx_frontend();
    if (!tx_frontend.local_is_initialized()) {
        throw ss::httpd::bad_request_exception("Can not get tx_frontend");
    }

    auto res = co_await tx_frontend.local()
                 .get_all_transactions_for_one_tx_partition(tx_ntp);
    if (!res.has_value()) {
        co_await throw_on_error(*req, res.error(), tx_ntp);
    }

    using tx_info = ss::httpd::transaction_json::transaction_summary;
    std::vector<tx_info> ans;
    ans.reserve(res.value().size());

    for (auto& tx : res.value()) {
        if (tx.status == cluster::tm_transaction::tx_status::tombstone) {
            continue;
        }

        tx_info new_tx;

        new_tx.transactional_id = tx.id;

        ss::httpd::transaction_json::producer_identity pid;
        pid.id = tx.pid.id;
        pid.epoch = tx.pid.epoch;
        new_tx.pid = pid;

        new_tx.tx_seq = tx.tx_seq;
        new_tx.etag = tx.etag;

        // The motivation behind mapping killed to aborting is to make
        // user not to think about the subtle differences between both
        // statuses
        if (tx.status == cluster::tm_transaction::tx_status::killed) {
            tx.status = cluster::tm_transaction::tx_status::aborting;
        }
        new_tx.status = ss::sstring(tx.get_status());

        new_tx.timeout_ms = tx.get_timeout().count();
        new_tx.staleness_ms = tx.get_staleness().count();

        for (auto& partition : tx.partitions) {
            ss::httpd::transaction_json::partition partition_info;
            partition_info.ns = partition.ntp.ns;
            partition_info.topic = partition.ntp.tp.topic;
            partition_info.partition_id = partition.ntp.tp.partition;
            partition_info.etag = partition.etag;

            new_tx.partitions.push(partition_info);
        }

        for (auto& group : tx.groups) {
            ss::httpd::transaction_json::group group_info;
            group_info.group_id = group.group_id;
            group_info.etag = group.etag;

            new_tx.groups.push(group_info);
        }

        ans.push_back(std::move(new_tx));
    }

    co_return ss::json::json_return_type(ans);
}

ss::future<ss::json::json_return_type>
admin_server::delete_partition_handler(std::unique_ptr<ss::http::request> req) {
    auto& tx_frontend = _partition_manager.local().get_tx_frontend();
    if (!tx_frontend.local_is_initialized()) {
        throw ss::httpd::bad_request_exception("Transaction are disabled");
    }
    auto transaction_id = req->param["transactional_id"];
    kafka::transactional_id tid(transaction_id);
    auto tx_ntp = tx_frontend.local().get_ntp(tid);
    if (!tx_ntp) {
        throw ss::httpd::bad_request_exception("Coordinator not available");
    }
    if (need_redirect_to_leader(*tx_ntp, _metadata_cache)) {
        throw co_await redirect_to_leader(*req, *tx_ntp);
    }

    auto ntp = parse_ntp_from_query_param(req);

    auto etag_str = req->get_query_param("etag");
    int64_t etag;
    try {
        etag = std::stoi(etag_str);
    } catch (...) {
        throw ss::httpd::bad_param_exception(
          fmt::format("Etag must be an integer: {}", etag_str));
    }

    if (etag < 0) {
        throw ss::httpd::bad_param_exception(
          fmt::format("Invalid etag {}", etag));
    }

    cluster::tm_transaction::tx_partition partition_for_delete{
      .ntp = ntp, .etag = model::term_id(etag)};

    vlog(
      logger.info,
      "Delete partition(ntp: {}, etag: {}) from transaction({})",
      ntp,
      etag,
      tid);

    auto res = co_await tx_frontend.local().delete_partition_from_tx(
      tid, partition_for_delete);
    co_await throw_on_error(*req, res, ntp);
    co_return ss::json::json_return_type(ss::json::json_void());
}
void admin_server::register_transaction_routes() {
    register_route<user>(
      ss::httpd::transaction_json::get_all_transactions,
      [this](std::unique_ptr<ss::http::request> req) {
          return get_all_transactions_handler(std::move(req));
      });

    register_route<user>(
      ss::httpd::transaction_json::delete_partition,
      [this](std::unique_ptr<ss::http::request> req) {
          return delete_partition_handler(std::move(req));
      });
}

static json::validator make_self_test_start_validator() {
    const std::string schema = R"(
{
    "type": "object",
    "properties": {
        "nodes": {
            "type": "array",
            "items": {
                "type": "number"
            }
        },
        "tests": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string"
                    }
                },
                "required": ["type"]
            }
        }
    },
    "required": [],
    "additionalProperties": false
}
)";
    return json::validator(schema);
}

ss::future<ss::json::json_return_type>
admin_server::self_test_start_handler(std::unique_ptr<ss::http::request> req) {
    static thread_local json::validator self_test_start_validator(
      make_self_test_start_validator());
    if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
        vlog(logger.debug, "Need to redirect self_test_start request");
        throw co_await redirect_to_leader(*req, model::controller_ntp);
    }
    auto doc = parse_json_body(*req);
    apply_validator(self_test_start_validator, doc);
    std::vector<model::node_id> ids;
    cluster::start_test_request r;
    if (!doc.IsNull()) {
        if (doc.HasMember("nodes")) {
            const auto& node_ids = doc["nodes"].GetArray();
            for (const auto& element : node_ids) {
                ids.emplace_back(element.GetInt());
            }
        } else {
            /// If not provided, default is to start the test on all nodes
            ids = _controller->get_members_table().local().node_ids();
        }
        if (doc.HasMember("tests")) {
            const auto& params = doc["tests"].GetArray();
            for (const auto& element : params) {
                const auto& obj = element.GetObject();
                const ss::sstring test_type(obj["type"].GetString());
                if (test_type == "disk") {
                    r.dtos.push_back(cluster::diskcheck_opts::from_json(obj));
                } else if (test_type == "network") {
                    r.ntos.push_back(cluster::netcheck_opts::from_json(obj));
                } else {
                    throw ss::httpd::bad_param_exception(
                      "Unknown self_test 'type', valid options are 'disk' or "
                      "'network'");
                }
            }
        } else {
            /// Default test run is to start 1 disk and 1 network test with
            /// default arguments
            r.dtos.push_back(cluster::diskcheck_opts{});
            r.ntos.push_back(cluster::netcheck_opts{});
        }
    }
    try {
        auto tid = co_await _self_test_frontend.invoke_on(
          cluster::self_test_frontend::shard,
          [r, ids](auto& self_test_frontend) {
              return self_test_frontend.start_test(r, ids);
          });
        vlog(logger.info, "Request to start self test succeeded: {}", tid);
        co_return ss::json::json_return_type(tid);
    } catch (const std::exception& ex) {
        throw ss::httpd::base_exception(
          fmt::format("Failed to start self test, reason: {}", ex),
          ss::http::reply::status_type::service_unavailable);
    }
}

ss::future<ss::json::json_return_type>
admin_server::self_test_stop_handler(std::unique_ptr<ss::http::request> req) {
    if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
        vlog(logger.info, "Need to redirect self_test_stop request");
        throw co_await redirect_to_leader(*req, model::controller_ntp);
    }
    auto r = co_await _self_test_frontend.invoke_on(
      cluster::self_test_frontend::shard,
      [](auto& self_test_frontend) { return self_test_frontend.stop_test(); });
    if (!r.finished()) {
        throw ss::httpd::base_exception(
          fmt::format(
            "Failed to stop one or more self_test jobs: {}",
            r.active_participant_ids()),
          ss::http::reply::status_type::service_unavailable);
    }
    vlog(logger.info, "Request to stop self test succeeded");
    co_return ss::json::json_void();
}

static ss::httpd::debug_json::self_test_result
self_test_result_to_json(const cluster::self_test_result& str) {
    ss::httpd::debug_json::self_test_result r;
    r.test_id = ss::sstring(str.test_id);
    r.name = str.name;
    r.info = str.info;
    r.test_type = str.test_type;
    r.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                   str.duration)
                   .count();
    r.timeouts = str.timeouts;
    if (str.warning) {
        r.warning = *str.warning;
    }
    if (str.error) {
        r.error = *str.error;
        return r;
    }
    r.p50 = str.p50;
    r.p90 = str.p90;
    r.p99 = str.p99;
    r.p999 = str.p999;
    r.max_latency = str.max;
    r.rps = str.rps;
    r.bps = str.bps;
    return r;
}

ss::future<ss::json::json_return_type>
admin_server::self_test_get_results_handler(
  std::unique_ptr<ss::http::request>) {
    namespace dbg_ns = ss::httpd::debug_json;
    std::vector<dbg_ns::self_test_node_report> reports;
    auto status = co_await _self_test_frontend.invoke_on(
      cluster::self_test_frontend::shard,
      [](auto& self_test_frontend) { return self_test_frontend.status(); });
    reports.reserve(status.results().size());
    for (const auto& [id, participant] : status.results()) {
        dbg_ns::self_test_node_report nr;
        nr.node_id = id;
        nr.status = cluster::self_test_status_as_string(participant.status());
        if (participant.response) {
            for (const auto& r : participant.response->results) {
                nr.results.push(self_test_result_to_json(r));
            }
        }
        reports.push_back(nr);
    }
    co_return ss::json::json_return_type(reports);
}

void admin_server::register_self_test_routes() {
    register_route<superuser>(
      ss::httpd::debug_json::self_test_start,
      [this](std::unique_ptr<ss::http::request> req) {
          return self_test_start_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::debug_json::self_test_stop,
      [this](std::unique_ptr<ss::http::request> req) {
          return self_test_stop_handler(std::move(req));
      });

    register_route<user>(
      ss::httpd::debug_json::self_test_status,
      [this](std::unique_ptr<ss::http::request> req) {
          return self_test_get_results_handler(std::move(req));
      });
}

ss::future<ss::json::json_return_type>
admin_server::cloud_storage_usage_handler(
  std::unique_ptr<ss::http::request> req) {
    auto batch_size
      = cluster::topic_table_partition_generator::default_batch_size;
    if (auto batch_size_param = req->get_query_param("batch_size");
        !batch_size_param.empty()) {
        try {
            batch_size = std::stoi(batch_size_param);
        } catch (...) {
            throw ss::httpd::bad_param_exception(fmt::format(
              "batch_size must be an integer: {}", batch_size_param));
        }
    }

    auto retries_allowed
      = cluster::cloud_storage_size_reducer::default_retries_allowed;
    if (auto retries_param = req->get_query_param("retries_allowed");
        !retries_param.empty()) {
        try {
            retries_allowed = std::stoi(retries_param);
        } catch (...) {
            throw ss::httpd::bad_param_exception(fmt::format(
              "retries_allowed must be an integer: {}", retries_param));
        }
    }

    cluster::cloud_storage_size_reducer reducer(
      _controller->get_topics_state(),
      _controller->get_members_table(),
      _controller->get_partition_leaders(),
      _connection_cache,
      batch_size,
      retries_allowed);

    auto res = co_await reducer.reduce();

    if (res) {
        co_return ss::json::json_return_type(res.value());
    } else {
        throw ss::httpd::base_exception(
          fmt::format("Failed to generate total cloud storage usage. "
                      "Please retry."),
          ss::http::reply::status_type::service_unavailable);
    }
}

static ss::json::json_return_type raw_data_to_usage_response(
  const std::vector<kafka::usage_window>& total_usage, bool include_open) {
    std::vector<ss::httpd::usage_json::usage_response> resp;
    resp.reserve(total_usage.size());
    for (size_t i = (include_open ? 0 : 1); i < total_usage.size(); ++i) {
        resp.emplace_back();
        const auto& e = total_usage[i];
        resp.back().begin_timestamp = e.begin;
        resp.back().end_timestamp = e.end;
        resp.back().open = e.is_open();
        resp.back().kafka_bytes_received_count = e.u.bytes_received;
        resp.back().kafka_bytes_sent_count = e.u.bytes_sent;
        if (e.u.bytes_cloud_storage) {
            resp.back().cloud_storage_bytes_gauge = *e.u.bytes_cloud_storage;
        } else {
            resp.back().cloud_storage_bytes_gauge = -1;
        }
    }
    if (include_open && !resp.empty()) {
        /// Handle case where client does not want to observe
        /// value of 0 for open buckets end timestamp
        auto& e = resp.at(0);
        vassert(e.open(), "Bucket not open when expecting to be");
        e.end_timestamp = std::chrono::duration_cast<std::chrono::seconds>(
                            ss::lowres_system_clock::now().time_since_epoch())
                            .count();
    }
    return resp;
}

void admin_server::register_usage_routes() {
    register_route<user>(
      ss::httpd::usage_json::get_usage,
      [this](std::unique_ptr<ss::http::request> req) {
          if (!config::shard_local_cfg().enable_usage()) {
              throw ss::httpd::bad_request_exception(
                "Usage tracking is not enabled");
          }
          bool include_open = false;
          auto include_open_str = req->get_query_param("include_open_bucket");
          vlog(
            logger.info,
            "Request to observe usage info, include_open_bucket={}",
            include_open_str);
          if (!include_open_str.empty()) {
              include_open = str_to_bool(include_open_str);
          }
          return _usage_manager
            .invoke_on(
              kafka::usage_manager::usage_manager_main_shard,
              [](kafka::usage_manager& um) { return um.get_usage_stats(); })
            .then([include_open](auto total_usage) {
                return raw_data_to_usage_response(total_usage, include_open);
            });
      });
}

namespace {

void fill_raft_state(
  ss::httpd::debug_json::partition_replica_state& replica,
  cluster::partition_state state) {
    ss::httpd::debug_json::raft_replica_state raft_state;
    auto& src = state.raft_state;
    raft_state.node_id = src.node();
    raft_state.term = src.term();
    raft_state.offset_translator_state = std::move(src.offset_translator_state);
    raft_state.group_configuration = std::move(src.group_configuration);
    raft_state.confirmed_term = src.confirmed_term();
    raft_state.flushed_offset = src.flushed_offset();
    raft_state.commit_index = src.commit_index();
    raft_state.majority_replicated_index = src.majority_replicated_index();
    raft_state.visibility_upper_bound_index
      = src.visibility_upper_bound_index();
    raft_state.last_quorum_replicated_index
      = src.last_quorum_replicated_index();
    raft_state.last_snapshot_term = src.last_snapshot_term();
    raft_state.received_snapshot_bytes = src.received_snapshot_bytes;
    raft_state.last_snapshot_index = src.last_snapshot_index();
    raft_state.received_snapshot_index = src.received_snapshot_index();
    raft_state.has_pending_flushes = src.has_pending_flushes;
    raft_state.is_leader = src.is_leader;
    raft_state.is_elected_leader = src.is_elected_leader;
    if (src.followers) {
        for (const auto& f : *src.followers) {
            ss::httpd::debug_json::raft_follower_state follower_state;
            follower_state.id = f.node();
            follower_state.last_flushed_log_index = f.last_flushed_log_index();
            follower_state.last_dirty_log_index = f.last_dirty_log_index();
            follower_state.match_index = f.match_index();
            follower_state.next_index = f.next_index();
            follower_state.last_sent_offset = f.last_sent_offset();
            follower_state.heartbeats_failed = f.heartbeats_failed;
            follower_state.is_learner = f.is_learner;
            follower_state.ms_since_last_heartbeat = f.ms_since_last_heartbeat;
            follower_state.last_sent_seq = f.last_sent_seq;
            follower_state.last_received_seq = f.last_received_seq;
            follower_state.last_successful_received_seq
              = f.last_successful_received_seq;
            follower_state.suppress_heartbeats = f.suppress_heartbeats;
            follower_state.is_recovering = f.is_recovering;
            raft_state.followers.push(std::move(follower_state));
        }
    }
    replica.raft_state = std::move(raft_state);
}
} // namespace

ss::future<ss::json::json_return_type>
admin_server::get_partition_state_handler(
  std::unique_ptr<ss::http::request> req) {
    const model::ntp ntp = parse_ntp_from_request(req->param);
    auto result
      = co_await _controller->get_topics_frontend().local().get_partition_state(
        ntp);
    if (result.has_error()) {
        throw ss::httpd::server_error_exception(fmt::format(
          "Error {} processing partition state for ntp: {}",
          result.error(),
          ntp));
    }

    ss::httpd::debug_json::partition_state response;
    response.ntp = fmt::format("{}", ntp);
    const auto& states = result.value();
    for (const auto& state : states) {
        ss::httpd::debug_json::partition_replica_state replica;
        replica.start_offset = state.start_offset;
        replica.committed_offset = state.committed_offset;
        replica.last_stable_offset = state.last_stable_offset;
        replica.high_watermark = state.high_water_mark;
        replica.dirty_offset = state.dirty_offset;
        replica.latest_configuration_offset = state.latest_configuration_offset;
        replica.revision_id = state.revision_id;
        replica.log_size_bytes = state.log_size_bytes;
        replica.non_log_disk_size_bytes = state.non_log_disk_size_bytes;
        replica.is_read_replica_mode_enabled
          = state.is_read_replica_mode_enabled;
        replica.read_replica_bucket = state.read_replica_bucket;
        replica.is_remote_fetch_enabled = state.is_remote_fetch_enabled;
        replica.is_cloud_data_available = state.is_cloud_data_available;
        replica.start_cloud_offset = state.start_cloud_offset;
        replica.next_cloud_offset = state.next_cloud_offset;
        fill_raft_state(replica, std::move(state));
        response.replicas.push(std::move(replica));
    }
    co_return ss::json::json_return_type(std::move(response));
}

void admin_server::register_debug_routes() {
    register_route<user>(
      ss::httpd::debug_json::reset_leaders_info,
      [this](std::unique_ptr<ss::http::request>) {
          vlog(logger.info, "Request to reset leaders info");
          return _metadata_cache
            .invoke_on_all([](auto& mc) { mc.reset_leaders(); })
            .then(
              [] { return ss::json::json_return_type(ss::json::json_void()); });
      });

    register_route<user>(
      ss::httpd::debug_json::refresh_disk_health_info,
      [this](std::unique_ptr<ss::http::request>) {
          vlog(logger.info, "Request to refresh disk health info");
          return _metadata_cache.local().refresh_health_monitor().then_wrapped(
            [](ss::future<> f) {
                if (f.failed()) {
                    auto eptr = f.get_exception();
                    vlog(
                      logger.error,
                      "failed to refresh disk health info: {}",
                      eptr);
                    return ss::make_exception_future<
                      ss::json::json_return_type>(
                      ss::httpd::server_error_exception(
                        "Could not refresh disk health info"));
                }
                return ss::make_ready_future<ss::json::json_return_type>(
                  ss::json::json_void());
            });
      });

    register_route<user>(
      ss::httpd::debug_json::get_leaders_info,
      [this](std::unique_ptr<ss::http::request>) {
          vlog(logger.info, "Request to get leaders info");
          using result_t = ss::httpd::debug_json::leader_info;
          std::vector<result_t> ans;

          auto leaders_info = _metadata_cache.local().get_leaders();
          ans.reserve(leaders_info.size());
          for (const auto& leader_info : leaders_info) {
              result_t info;
              info.ns = leader_info.tp_ns.ns;
              info.topic = leader_info.tp_ns.tp;
              info.partition_id = leader_info.pid;
              info.leader = leader_info.current_leader.has_value()
                              ? leader_info.current_leader.value()
                              : -1;
              info.previous_leader = leader_info.previous_leader.has_value()
                                       ? leader_info.previous_leader.value()
                                       : -1;
              info.last_stable_leader_term
                = leader_info.last_stable_leader_term;
              info.update_term = leader_info.update_term;
              info.partition_revision = leader_info.partition_revision;
              ans.push_back(std::move(info));
          }

          return ss::make_ready_future<ss::json::json_return_type>(ans);
      });

    register_route<user>(
      seastar::httpd::debug_json::get_peer_status,
      [this](std::unique_ptr<ss::http::request> req) {
          model::node_id id = parse_broker_id(*req);
          auto node_status = _node_status_table.local().get_node_status(id);

          if (!node_status) {
              throw ss::httpd::bad_param_exception(
                fmt::format("Unknown node with id {}", id));
          }

          auto delta = std::chrono::duration_cast<std::chrono::milliseconds>(
            rpc::clock_type::now() - node_status->last_seen);

          seastar::httpd::debug_json::peer_status ret;
          ret.since_last_status = delta.count();

          return ss::make_ready_future<ss::json::json_return_type>(ret);
      });

    register_route<user>(
      seastar::httpd::debug_json::is_node_isolated,
      [this](std::unique_ptr<ss::http::request>) {
          return ss::make_ready_future<ss::json::json_return_type>(
            _metadata_cache.local().is_node_isolated());
      });

    register_route<user>(
      seastar::httpd::debug_json::get_controller_status,
      [this](std::unique_ptr<ss::http::request>)
        -> ss::future<ss::json::json_return_type> {
          return _controller->get_last_applied_offset().then(
            [this](auto offset) {
                using result_t = ss::httpd::debug_json::controller_status;
                result_t ans;
                ans.last_applied_offset = offset;
                ans.start_offset = _controller->get_start_offset();
                ans.commited_index = _controller->get_commited_index();
                return ss::make_ready_future<ss::json::json_return_type>(
                  ss::json::json_return_type(ans));
            });
      });

    register_route<user>(
      seastar::httpd::debug_json::get_cloud_storage_usage,
      [this](std::unique_ptr<ss::http::request> req)
        -> ss::future<ss::json::json_return_type> {
          return cloud_storage_usage_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::debug_json::blocked_reactor_notify_ms,
      [this](std::unique_ptr<ss::http::request> req) {
          std::chrono::milliseconds timeout;
          if (auto e = req->get_query_param("timeout"); !e.empty()) {
              try {
                  timeout = std::clamp(
                    std::chrono::milliseconds(
                      boost::lexical_cast<long long>(e)),
                    1ms,
                    _default_blocked_reactor_notify);
              } catch (const boost::bad_lexical_cast&) {
                  throw ss::httpd::bad_param_exception(
                    fmt::format("Invalid parameter 'timeout' value {{{}}}", e));
              }
          }

          std::optional<std::chrono::seconds> expires;
          static constexpr std::chrono::seconds max_expire_time_sec
            = std::chrono::minutes(30);
          if (auto e = req->get_query_param("expires"); !e.empty()) {
              try {
                  expires = std::clamp(
                    std::chrono::seconds(boost::lexical_cast<long long>(e)),
                    1s,
                    max_expire_time_sec);
              } catch (const boost::bad_lexical_cast&) {
                  throw ss::httpd::bad_param_exception(
                    fmt::format("Invalid parameter 'expires' value {{{}}}", e));
              }
          }

          // This value is used when the expiration time is not set explicitly
          static constexpr std::chrono::seconds default_expiration_time
            = std::chrono::minutes(5);
          auto curr = ss::engine().get_blocked_reactor_notify_ms();

          vlog(
            logger.info,
            "Setting blocked_reactor_notify_ms from {} to {} for {} "
            "(default={})",
            curr,
            timeout.count(),
            expires.value_or(default_expiration_time),
            _default_blocked_reactor_notify);

          return ss::smp::invoke_on_all([timeout] {
                     ss::engine().update_blocked_reactor_notify_ms(timeout);
                 })
            .then([] {
                return ss::make_ready_future<ss::json::json_return_type>(
                  ss::json::json_void());
            })
            .finally([this, expires] {
                _blocked_reactor_notify_reset_timer.rearm(
                  ss::steady_clock_type::now()
                  + expires.value_or(default_expiration_time));
            });
      });

    register_route<user>(
      ss::httpd::debug_json::restart_service,
      [this](std::unique_ptr<ss::http::request> req) {
          return restart_service_handler(std::move(req));
      });

    register_route<user>(
      seastar::httpd::debug_json::get_partition_state,
      [this](std::unique_ptr<ss::http::request> req)
        -> ss::future<ss::json::json_return_type> {
          return get_partition_state_handler(std::move(req));
      });
}

ss::future<ss::json::json_return_type>
admin_server::get_partition_balancer_status_handler(
  std::unique_ptr<ss::http::request> req) {
    vlog(logger.debug, "Requested partition balancer status");

    using result_t = std::variant<
      cluster::partition_balancer_overview_reply,
      model::node_id,
      cluster::errc>;

    result_t result = co_await _controller->get_partition_balancer().invoke_on(
      cluster::partition_balancer_backend::shard,
      [](cluster::partition_balancer_backend& backend) {
          if (backend.is_leader()) {
              return result_t(backend.overview());
          } else {
              auto leader_id = backend.leader_id();
              if (leader_id) {
                  return result_t(leader_id.value());
              } else {
                  return result_t(cluster::errc::no_leader_controller);
              }
          }
      });

    cluster::partition_balancer_overview_reply overview;
    if (std::holds_alternative<cluster::partition_balancer_overview_reply>(
          result)) {
        overview = std::move(
          std::get<cluster::partition_balancer_overview_reply>(result));
    } else if (std::holds_alternative<model::node_id>(result)) {
        auto node_id = std::get<model::node_id>(result);
        vlog(
          logger.debug,
          "proxying the partition_balancer_overview call to node {}",
          node_id);
        auto rpc_result
          = co_await _connection_cache.local()
              .with_node_client<
                cluster::partition_balancer_rpc_client_protocol>(
                _controller->self(),
                ss::this_shard_id(),
                node_id,
                5s,
                [](cluster::partition_balancer_rpc_client_protocol cp) {
                    return cp.overview(
                      cluster::partition_balancer_overview_request{},
                      rpc::client_opts(5s));
                });

        if (rpc_result.has_error()) {
            co_await throw_on_error(
              *req, rpc_result.error(), model::controller_ntp);
        }

        overview = std::move(rpc_result.value().data);
    } else {
        co_await throw_on_error(
          *req, std::get<cluster::errc>(result), model::controller_ntp);
    }

    ss::httpd::cluster_json::partition_balancer_status ret;

    if (overview.error == cluster::errc::feature_disabled) {
        ret.status = "off";
        co_return ss::json::json_return_type(ret);
    } else if (overview.error != cluster::errc::success) {
        co_await throw_on_error(*req, overview.error, model::controller_ntp);
    }

    ret.status = fmt::format("{}", overview.status);

    if (overview.last_tick_time != model::timestamp::missing()) {
        ret.seconds_since_last_tick = (model::timestamp::now().value()
                                       - overview.last_tick_time.value())
                                      / 1000;
    }

    if (overview.violations) {
        ss::httpd::cluster_json::partition_balancer_violations ret_violations;
        for (const auto& n : overview.violations->unavailable_nodes) {
            ret_violations.unavailable_nodes.push(n.id);
        }
        for (const auto& n : overview.violations->full_nodes) {
            ret_violations.over_disk_limit_nodes.push(n.id);
        }
        ret.violations = ret_violations;
    }

    ret.current_reassignments_count
      = _controller->get_topics_state().local().updates_in_progress().size();

    co_return ss::json::json_return_type(ret);
}

ss::future<ss::json::json_return_type>
admin_server::cancel_all_partitions_reconfigs_handler(
  std::unique_ptr<ss::http::request> req) {
    vlog(
      logger.info, "Requested cancellation of all ongoing partition movements");

    auto res = co_await _controller->get_topics_frontend()
                 .local()
                 .cancel_moving_all_partition_replicas(
                   model::timeout_clock::now() + 5s);
    if (res.has_error()) {
        co_await throw_on_error(*req, res.error(), model::controller_ntp);
    }

    co_return ss::json::json_return_type(
      co_await map_partition_results(std::move(res.value())));
}
void admin_server::register_cluster_routes() {
    register_route<publik>(
      ss::httpd::cluster_json::get_cluster_health_overview,
      [this](std::unique_ptr<ss::http::request>) {
          vlog(logger.debug, "Requested cluster status");
          return _controller->get_health_monitor()
            .local()
            .get_cluster_health_overview(
              model::time_from_now(std::chrono::seconds(5)))
            .then([](auto health_overview) {
                ss::httpd::cluster_json::cluster_health_overview ret;
                ret.is_healthy = health_overview.is_healthy;
                ret.all_nodes._set = true;
                ret.nodes_down._set = true;
                ret.leaderless_partitions._set = true;
                ret.under_replicated_partitions._set = true;

                ret.all_nodes = health_overview.all_nodes;
                ret.nodes_down = health_overview.nodes_down;

                for (auto& ntp : health_overview.leaderless_partitions) {
                    ret.leaderless_partitions.push(fmt::format(
                      "{}/{}/{}", ntp.ns(), ntp.tp.topic(), ntp.tp.partition));
                }
                for (auto& ntp : health_overview.under_replicated_partitions) {
                    ret.under_replicated_partitions.push(fmt::format(
                      "{}/{}/{}", ntp.ns(), ntp.tp.topic(), ntp.tp.partition));
                }
                if (health_overview.controller_id) {
                    ret.controller_id = health_overview.controller_id.value();
                } else {
                    ret.controller_id = -1;
                }
                if (health_overview.bytes_in_cloud_storage) {
                    ret.bytes_in_cloud_storage
                      = health_overview.bytes_in_cloud_storage.value();
                } else {
                    ret.bytes_in_cloud_storage = -1;
                }

                return ss::json::json_return_type(ret);
            });
      });

    register_route<publik>(
      ss::httpd::cluster_json::get_partition_balancer_status,
      [this](std::unique_ptr<ss::http::request> req) {
          return get_partition_balancer_status_handler(std::move(req));
      });

    register_route<superuser>(
      ss::httpd::cluster_json::cancel_all_partitions_reconfigurations,
      [this](std::unique_ptr<ss::http::request> req) {
          return cancel_all_partitions_reconfigs_handler(std::move(req));
      });

    register_route_sync<publik>(
      ss::httpd::cluster_json::get_cluster_uuid,
      [this](ss::httpd::const_req) -> ss::json::json_return_type {
          vlog(logger.debug, "Requested cluster UUID");
          const std::optional<model::cluster_uuid>& cluster_uuid
            = _controller->get_storage().local().get_cluster_uuid();
          if (cluster_uuid) {
              ss::httpd::cluster_json::uuid ret;
              ret.cluster_uuid = fmt::format("{}", cluster_uuid);
              return ss::json::json_return_type(std::move(ret));
          }
          return ss::json::json_return_type(ss::json::json_void());
      });
}

ss::future<ss::json::json_return_type> admin_server::sync_local_state_handler(
  std::unique_ptr<ss::http::request> request) {
    struct manifest_reducer {
        ss::future<>
        operator()(std::optional<cloud_storage::partition_manifest>&& value) {
            _manifest = std::move(value);
            return ss::make_ready_future<>();
        }
        std::optional<cloud_storage::partition_manifest> get() && {
            return std::move(_manifest);
        }
        std::optional<cloud_storage::partition_manifest> _manifest;
    };

    vlog(logger.info, "Requested bucket syncup");
    auto ntp = parse_ntp_from_request(request->param, model::kafka_namespace);
    if (need_redirect_to_leader(ntp, _metadata_cache)) {
        vlog(logger.info, "Need to redirect bucket syncup request");
        throw co_await redirect_to_leader(*request, ntp);
    } else {
        auto result = co_await _partition_manager.map_reduce(
          manifest_reducer(), [ntp](cluster::partition_manager& p) {
              auto partition = p.get(ntp);
              if (partition) {
                  auto archiver = partition->archiver();
                  if (archiver) {
                      return archiver.value().get().maybe_truncate_manifest();
                  }
              }
              return ss::make_ready_future<
                std::optional<cloud_storage::partition_manifest>>(std::nullopt);
          });
        vlog(logger.info, "Requested bucket syncup completed");
        if (result) {
            std::stringstream sts;
            result->serialize(sts);
            vlog(logger.info, "Requested bucket syncup result {}", sts.str());
        } else {
            vlog(logger.info, "Requested bucket syncup result empty");
        }
    }
    co_return ss::json::json_return_type(ss::json::json_void());
}

ss::future<std::unique_ptr<ss::http::reply>>
admin_server::initiate_topic_scan_and_recovery(
  std::unique_ptr<ss::http::request> request,
  std::unique_ptr<ss::http::reply> reply) {
    reply->set_content_type("json");

    if (need_redirect_to_leader(model::controller_ntp, _metadata_cache)) {
        throw co_await redirect_to_leader(*request, model::controller_ntp);
    }

    if (!_topic_recovery_service.local_is_initialized()) {
        throw ss::httpd::bad_request_exception(
          "Topic recovery is not available. is cloud storage enabled?");
    }

    auto result = co_await _topic_recovery_service.invoke_on(
      cloud_storage::topic_recovery_service::shard_id,
      [&request](auto& svc) { return svc.start_recovery(*request); });

    if (result.status_code != ss::http::reply::status_type::accepted) {
        throw ss::httpd::base_exception{result.message, result.status_code};
    }

    auto payload = ss::httpd::shadow_indexing_json::init_recovery_result{};
    payload.status = result.message;

    reply->set_status(result.status_code, payload.to_json());
    co_return reply;
}

static ss::httpd::shadow_indexing_json::topic_recovery_status
map_status_to_json(cluster::single_status status) {
    ss::httpd::shadow_indexing_json::topic_recovery_status status_json;
    status_json.state = fmt::format("{}", status.state);

    for (const auto& count : status.download_counts) {
        ss::httpd::shadow_indexing_json::topic_download_counts c;
        c.topic_namespace = fmt::format("{}", count.tp_ns);
        c.pending_downloads = count.pending_downloads;
        c.successful_downloads = count.successful_downloads;
        c.failed_downloads = count.failed_downloads;
        status_json.topic_download_counts.push(c);
    }

    ss::httpd::shadow_indexing_json::recovery_request_params r;
    r.topic_names_pattern = status.request.topic_names_pattern.value_or("none");
    r.retention_bytes = status.request.retention_bytes.value_or(-1);
    r.retention_ms = status.request.retention_ms.value_or(-1ms).count();
    status_json.request = r;

    return status_json;
}

static ss::json::json_return_type serialize_topic_recovery_status(
  const cluster::status_response& cluster_status, bool extended) {
    if (!extended) {
        return map_status_to_json(cluster_status.status_log.back());
    }

    std::vector<ss::httpd::shadow_indexing_json::topic_recovery_status>
      status_log;
    status_log.reserve(cluster_status.status_log.size());
    for (const auto& entry : cluster_status.status_log) {
        status_log.push_back(map_status_to_json(entry));
    }

    return status_log;
}

ss::future<ss::json::json_return_type>
admin_server::query_automated_recovery(std::unique_ptr<ss::http::request> req) {
    ss::httpd::shadow_indexing_json::topic_recovery_status ret;
    ret.state = "inactive";

    if (
      !_topic_recovery_status_frontend.local_is_initialized()
      || !_topic_recovery_service.local_is_initialized()) {
        co_return ret;
    }

    auto controller_leader = _metadata_cache.local().get_leader_id(
      model::controller_ntp);

    if (!controller_leader) {
        throw ss::httpd::server_error_exception{
          "Unable to get controller leader, cannot get recovery status"};
    }

    auto extended = get_boolean_query_param(*req, "extended");
    if (controller_leader.value() == config::node().node_id.value()) {
        auto status_log = co_await _topic_recovery_service.invoke_on(
          cloud_storage::topic_recovery_service::shard_id,
          [](auto& svc) { return svc.recovery_status_log(); });
        co_return serialize_topic_recovery_status(
          cluster::map_log_to_response(std::move(status_log)), extended);
    }

    if (auto status = co_await _topic_recovery_status_frontend.local().status(
          controller_leader.value());
        status.has_value()) {
        co_return serialize_topic_recovery_status(status.value(), extended);
    }

    co_return ret;
}

static ss::httpd::shadow_indexing_json::partition_cloud_storage_status
map_status_to_json(cluster::partition_cloud_storage_status status) {
    ss::httpd::shadow_indexing_json::partition_cloud_storage_status json;

    json.cloud_storage_mode = fmt::format("{}", status.mode);

    if (status.since_last_manifest_upload) {
        json.ms_since_last_manifest_upload
          = status.since_last_manifest_upload->count();
    }
    if (status.since_last_segment_upload) {
        json.ms_since_last_segment_upload
          = status.since_last_segment_upload->count();
    }
    if (status.since_last_manifest_sync) {
        json.ms_since_last_manifest_sync
          = status.since_last_manifest_sync->count();
    }

    json.metadata_update_pending = status.cloud_metadata_update_pending;

    json.total_log_size_bytes = status.total_log_size_bytes;
    json.cloud_log_size_bytes = status.cloud_log_size_bytes;
    json.local_log_size_bytes = status.local_log_size_bytes;
    json.cloud_log_segment_count = status.cloud_log_segment_count;
    json.local_log_segment_count = status.local_log_segment_count;

    if (status.cloud_log_start_offset) {
        json.cloud_log_start_offset = status.cloud_log_start_offset.value()();
    }
    if (status.cloud_log_last_offset) {
        json.cloud_log_last_offset = status.cloud_log_last_offset.value()();
    }
    if (status.local_log_start_offset) {
        json.local_log_start_offset = status.local_log_start_offset.value()();
    }
    if (status.local_log_last_offset) {
        json.local_log_last_offset = status.local_log_last_offset.value()();
    }

    return json;
}

ss::future<ss::json::json_return_type>
admin_server::get_partition_cloud_storage_status(
  std::unique_ptr<ss::http::request> req) {
    const model::ntp ntp = parse_ntp_from_request(
      req->param, model::kafka_namespace);

    if (need_redirect_to_leader(ntp, _metadata_cache)) {
        throw co_await redirect_to_leader(*req, ntp);
    }

    const auto shard = _shard_table.local().shard_for(ntp);
    if (!shard) {
        throw ss::httpd::not_found_exception(fmt::format(
          "{} could not be found on the node. Perhaps it has been moved "
          "during the redirect.",
          ntp));
    }

    auto status = co_await _partition_manager.invoke_on(
      *shard,
      [&ntp](const auto& pm)
        -> std::optional<cluster::partition_cloud_storage_status> {
          const auto& partitions = pm.partitions();
          auto partition_iter = partitions.find(ntp);

          if (partition_iter == partitions.end()) {
              return std::nullopt;
          }

          return partition_iter->second->get_cloud_storage_status();
      });

    if (!status) {
        throw ss::httpd::not_found_exception(
          fmt::format("{} could not be found on shard {}.", ntp, *shard));
    }

    co_return map_status_to_json(*status);
}

ss::future<std::unique_ptr<ss::http::reply>> admin_server::get_manifest(
  std::unique_ptr<ss::http::request> req,
  std::unique_ptr<ss::http::reply> rep) {
    model::ntp ntp = parse_ntp_from_request(req->param, model::kafka_namespace);

    if (!_metadata_cache.local().contains(ntp)) {
        throw ss::httpd::not_found_exception(
          fmt::format("Could not find {} on the cluster", ntp));
    }

    if (need_redirect_to_leader(ntp, _metadata_cache)) {
        throw co_await redirect_to_leader(*req, ntp);
    }

    const auto shard = _shard_table.local().shard_for(ntp);
    if (!shard) {
        throw ss::httpd::not_found_exception(fmt::format(
          "Could not find {} on node {}", ntp, config::node().node_id.value()));
    }

    co_return co_await _partition_manager.invoke_on(
      *shard,
      [rep = std::move(rep), ntp = std::move(ntp), shard](auto& pm) mutable {
          auto partition = pm.get(ntp);
          if (!partition) {
              throw ss::httpd::not_found_exception(
                fmt::format("Could not find {} on shard {}", ntp, *shard));
          }

          if (!partition->remote_partition()) {
              throw ss::httpd::bad_request_exception(
                fmt::format("Cluster is not configured for cloud storage"));
          }

          // The 'remote_partition' 'ss::shared_ptr' belongs to the
          // shard with sid '*shard'. Hence, we need to ensure that
          // when Seastar calls into the labmda provided by read body,
          // all access to the pointer happens on its home shard.
          rep->write_body(
            "json",
            [part = std::move(partition),
             sid = *shard](ss::output_stream<char>&& output_stream) mutable {
                return ss::smp::submit_to(
                  sid,
                  [os = std::move(output_stream),
                   part = std::move(part)]() mutable {
                      return ss::do_with(
                        std::move(os),
                        std::move(part),
                        [](auto& os, auto& part) mutable {
                            return part->serialize_manifest_to_output_stream(os)
                              .finally([&os] { return os.close(); });
                        });
                  });
            });

          return ss::make_ready_future<std::unique_ptr<ss::http::reply>>(
            std::move(rep));
      });
}

void admin_server::register_shadow_indexing_routes() {
    register_route<superuser>(
      ss::httpd::shadow_indexing_json::sync_local_state,
      [this](std::unique_ptr<ss::http::request> req) {
          return sync_local_state_handler(std::move(req));
      });

    request_handler_fn recovery_handler = [this](auto req, auto reply) {
        return initiate_topic_scan_and_recovery(
          std::move(req), std::move(reply));
    };

    register_route<superuser>(
      ss::httpd::shadow_indexing_json::initiate_topic_scan_and_recovery,
      std::move(recovery_handler));

    register_route<superuser>(
      ss::httpd::shadow_indexing_json::query_automated_recovery,
      [this](auto req) { return query_automated_recovery(std::move(req)); });

    register_route<user>(
      ss::httpd::shadow_indexing_json::get_partition_cloud_storage_status,
      [this](auto req) {
          return get_partition_cloud_storage_status(std::move(req));
      });

    register_route_raw_async<user>(
      ss::httpd::shadow_indexing_json::get_manifest,
      [this](
        std::unique_ptr<ss::http::request> req,
        std::unique_ptr<ss::http::reply> rep) {
          return get_manifest(std::move(req), std::move(rep));
      });
}

constexpr std::string_view to_string_view(service_kind kind) {
    switch (kind) {
    case service_kind::schema_registry:
        return "schema-registry";
    case service_kind::http_proxy:
        return "http-proxy";
    }
    return "invalid";
}

template<typename E>
std::enable_if_t<std::is_enum_v<E>, std::optional<E>>
  from_string_view(std::string_view);

template<>
constexpr std::optional<service_kind>
from_string_view<service_kind>(std::string_view sv) {
    return string_switch<std::optional<service_kind>>(sv)
      .match(
        to_string_view(service_kind::schema_registry),
        service_kind::schema_registry)
      .match(to_string_view(service_kind::http_proxy), service_kind::http_proxy)
      .default_match(std::nullopt);
}

namespace {
template<typename service_t>
ss::future<>
try_service_restart(service_t* svc, std::string_view service_str_view) {
    if (svc == nullptr) {
        throw ss::httpd::server_error_exception(fmt::format(
          "{} is undefined. Is it set in the .yaml config file?",
          service_str_view));
    }

    try {
        co_await svc->restart();
    } catch (const std::exception& ex) {
        vlog(
          logger.error,
          "Unknown issue restarting {}: {}",
          service_str_view,
          ex.what());
        throw ss::httpd::server_error_exception(
          fmt::format("Unknown issue restarting {}", service_str_view));
    }
}
} // namespace

ss::future<> admin_server::restart_redpanda_service(service_kind service) {
    switch (service) {
    case service_kind::schema_registry:
        co_await try_service_restart(_schema_registry, to_string_view(service));
        break;
    case service_kind::http_proxy:
        co_await try_service_restart(_http_proxy, to_string_view(service));
        break;
    }
}

ss::future<ss::json::json_return_type>
admin_server::restart_service_handler(std::unique_ptr<ss::http::request> req) {
    auto service_param = req->get_query_param("service");
    std::optional<service_kind> service = from_string_view<service_kind>(
      service_param);
    if (!service.has_value()) {
        throw ss::httpd::not_found_exception(
          fmt::format("Invalid service: {}", service_param));
    }

    vlog(logger.info, "Restart redpanda service: {}", to_string_view(*service));
    co_await restart_redpanda_service(*service);
    co_return ss::json::json_return_type(ss::json::json_void());
}
