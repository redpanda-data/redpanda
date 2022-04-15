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

#include "cluster/cluster_utils.h"
#include "cluster/controller.h"
#include "cluster/controller_api.h"
#include "cluster/errc.h"
#include "cluster/feature_manager.h"
#include "cluster/feature_table.h"
#include "cluster/fwd.h"
#include "cluster/members_frontend.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_manager.h"
#include "cluster/security_frontend.h"
#include "cluster/shard_table.h"
#include "cluster/topics_frontend.h"
#include "cluster/tx_gateway_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "config/endpoint_tls_config.h"
#include "finjector/hbadger.h"
#include "json/document.h"
#include "json/schema.h"
#include "json/stringbuffer.h"
#include "json/writer.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record.h"
#include "net/dns.h"
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
#include "redpanda/admin/api-doc/status.json.h"
#include "redpanda/admin/api-doc/transaction.json.h"
#include "redpanda/request_auth.h"
#include "security/scram_algorithm.h"
#include "security/scram_authenticator.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/prometheus.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/http/api_docs.hh>
#include <seastar/http/httpd.hh>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/lexical_cast/bad_lexical_cast.hpp>
#include <fmt/core.h>

#include <limits>
#include <stdexcept>
#include <system_error>
#include <unordered_map>

using namespace std::chrono_literals;

static ss::logger logger{"admin_api_server"};

admin_server::admin_server(
  admin_server_cfg cfg,
  ss::sharded<cluster::partition_manager>& pm,
  ss::sharded<coproc::partition_manager>& cpm,
  cluster::controller* controller,
  ss::sharded<cluster::shard_table>& st,
  ss::sharded<cluster::metadata_cache>& metadata_cache)
  : _log_level_timer([this] { log_level_timer_handler(); })
  , _server("admin")
  , _cfg(std::move(cfg))
  , _partition_manager(pm)
  , _cp_partition_manager(cpm)
  , _controller(controller)
  , _shard_table(st)
  , _metadata_cache(metadata_cache)
  , _auth(
      config::shard_local_cfg().admin_api_require_auth.bind(), _controller) {}

ss::future<> admin_server::start() {
    configure_metrics_route();
    configure_dashboard();
    configure_admin_routes();

    co_await configure_listeners();

    vlog(
      logger.info,
      "Started HTTP admin service listening at {}",
      _cfg.endpoints);
}

ss::future<> admin_server::stop() { return _server.stop(); }

void admin_server::configure_admin_routes() {
    auto rb = ss::make_shared<ss::api_registry_builder20>(
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
    register_cluster_routes();
}

struct json_validator {
    explicit json_validator(const std::string& schema_text)
      : schema(make_schema_document(schema_text))
      , validator(schema) {}

    static json::SchemaDocument
    make_schema_document(const std::string& schema) {
        json::Document doc;
        if (doc.Parse(schema).HasParseError()) {
            throw std::runtime_error(
              fmt::format("Invalid schema document: {}", schema));
        }
        return json::SchemaDocument(doc);
    }

    const json::SchemaDocument schema;
    json::SchemaValidator validator;
};

static json_validator make_set_replicas_validator() {
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
    return json_validator(schema);
}

/**
 * A helper around rapidjson's Parse that checks for errors & raises
 * seastar HTTP exception.  Without that check, something as simple
 * as an empty request body causes a redpanda crash via a rapidjson
 * assertion when trying to GetObject on the resulting document.
 */
static json::Document parse_json_body(ss::httpd::request const& req) {
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
apply_validator(json_validator& validator, json::Document const& doc) {
    validator.validator.Reset();
    validator.validator.ResetError();

    if (!doc.Accept(validator.validator)) {
        json::StringBuffer val_buf;
        json::Writer<json::StringBuffer> w{val_buf};
        validator.validator.GetError().Accept(w);
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
get_boolean_query_param(const ss::httpd::request& req, std::string_view name) {
    auto key = ss::sstring(name);
    if (!req.query_parameters.contains(key)) {
        return false;
    }

    const ss::sstring& str_param = req.query_parameters.at(key);
    return ss::httpd::request::case_insensitive_cmp()(str_param, "true")
           || str_param == "1";
}

void admin_server::configure_dashboard() {
    if (_cfg.dashboard_dir) {
        auto handler = std::make_unique<dashboard_handler>(*_cfg.dashboard_dir);
        _server._routes.add(
          ss::httpd::operation_type::GET,
          ss::httpd::url("/dashboard").remainder("path"),
          handler.release());
    }
}

void admin_server::configure_metrics_route() {
    ss::prometheus::config metrics_conf;
    metrics_conf.metric_help = "redpanda metrics";
    metrics_conf.prefix = "vectorized";
    ss::prometheus::add_prometheus_routes(_server, metrics_conf).get();
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
  const ss::httpd::request& req, const request_auth_result& auth_state) const {
    vlog(
      logger.debug,
      "[{}] {}",
      auth_state.get_username().size() > 0 ? auth_state.get_username()
                                           : "_anonymous",
      req.get_url());
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
  ss::httpd::request& req, model::ntp const& ntp) const {
    auto leader_id_opt = _metadata_cache.local().get_leader_id(ntp);

    if (!leader_id_opt.has_value()) {
        vlog(logger.info, "Can't redirect, no leader for ntp {}", ntp);

        throw ss::httpd::base_exception(
          fmt::format(
            "Partition {} does not have a leader, cannot redirect", ntp),
          ss::httpd::reply::status_type::service_unavailable);
    }

    if (leader_id_opt.value() == config::node().node_id()) {
        vlog(
          logger.info,
          "Can't redirect to leader from leader node ({})",
          leader_id_opt.value());
        throw ss::httpd::base_exception(
          fmt::format("Leader not available"),
          ss::httpd::reply::status_type::service_unavailable);
    }

    auto leader_opt = _metadata_cache.local().get_broker(leader_id_opt.value());
    if (!leader_opt.has_value()) {
        throw ss::httpd::base_exception(
          fmt::format(
            "Partition {} leader {} metadata not available",
            ntp,
            leader_id_opt.value()),
          ss::httpd::reply::status_type::service_unavailable);
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

            auto leader_advertised_addrs = leader->kafka_advertised_listeners();
            if (leader_advertised_addrs.size() < listener_idx + 1) {
                vlog(
                  logger.debug,
                  "redirect: leader has no advertised address at matching "
                  "index for {}, "
                  "falling back to internal RPC address",
                  req_hostname);
                target_host = leader->rpc_address().host();
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
        }
    }

    auto url = fmt::format(
      "{}://{}{}{}", req.get_protocol_name(), target_host, port, req._url);

    vlog(
      logger.info, "Redirecting admin API call to {} leader at {}", ntp, url);

    co_return ss::httpd::redirect_exception(
      url, ss::httpd::reply::status_type::temporary_redirect);
}

namespace {
bool need_redirect_to_leader(
  model::ntp ntp, ss::sharded<cluster::metadata_cache>& metadata_cache) {
    auto leader_id_opt = metadata_cache.local().get_leader_id(ntp);
    if (!leader_id_opt.has_value()) {
        throw ss::httpd::base_exception(
          fmt::format(
            "Partition {} does not have a leader, cannot redirect", ntp),
          ss::httpd::reply::status_type::service_unavailable);
    }

    return leader_id_opt.value() != config::node().node_id();
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
  ss::httpd::request& req,
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
        case cluster::errc::update_in_progress:
        case cluster::errc::waiting_for_recovery:
        case cluster::errc::no_leader_controller:
            throw ss::httpd::base_exception(
              fmt::format("Not ready ({})", ec.message()),
              ss::httpd::reply::status_type::service_unavailable);
        case cluster::errc::not_leader:
            throw co_await redirect_to_leader(req, ntp);
        case cluster::errc::not_leader_controller:
            throw co_await redirect_to_leader(req, model::controller_ntp);
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
            throw ss::httpd::base_exception(
              fmt::format("Not ready: {}", ec.message()),
              ss::httpd::reply::status_type::service_unavailable);
        case raft::errc::transfer_to_current_leader:
            co_return;
        case raft::errc::not_leader:
            throw co_await redirect_to_leader(req, ntp);
        default:
            throw ss::httpd::server_error_exception(
              fmt::format("Unexpected raft error: {}", ec.message()));
        }
    } else if (ec.category() == cluster::tx_error_category()) {
        switch (cluster::tx_errc(ec.value())) {
        case cluster::tx_errc::leader_not_found:
            throw co_await redirect_to_leader(req, ntp);
        case cluster::tx_errc::pid_not_found:
            throw ss::httpd::bad_request_exception(
              fmt_with_ctx(fmt::format, "Can not find pid for ntp:{}", ntp));
        case cluster::tx_errc::partition_not_found: {
            ss::sstring error_msg;
            if (ntp == model::tx_manager_ntp) {
                error_msg = fmt::format("Can not find ntp:{}", ntp);
            } else {
                error_msg = fmt::format(
                  "Can not find partition({}) in transaction for delete", ntp);
            }
            throw ss::httpd::bad_request_exception(error_msg);
        }
        default:
            throw ss::httpd::server_error_exception(
              fmt::format("Unexpected tx_error error: {}", ec.message()));
        }
    } else {
        throw ss::httpd::server_error_exception(
          fmt::format("Unexpected error: {}", ec.message()));
    }
}

bool str_to_bool(std::string_view s) {
    if (s == "0" || s == "false" || s == "False") {
        return false;
    } else {
        return true;
    }
}

void admin_server::register_config_routes() {
    register_route_raw<superuser>(
      ss::httpd::config_json::get_config, [](ss::const_req, ss::reply& reply) {
          json::StringBuffer buf;
          json::Writer<json::StringBuffer> writer(buf);
          config::shard_local_cfg().to_json(writer);

          reply.set_status(ss::httpd::reply::status_type::ok, buf.GetString());
          return "";
      });

    register_route_raw<superuser>(
      ss::httpd::cluster_config_json::get_cluster_config,
      [](ss::const_req req, ss::reply& reply) {
          json::StringBuffer buf;
          json::Writer<json::StringBuffer> writer(buf);

          bool include_defaults = true;
          auto include_defaults_str = req.get_query_param("include_defaults");
          if (!include_defaults_str.empty()) {
              include_defaults = str_to_bool(include_defaults_str);
          }

          config::shard_local_cfg().to_json(
            writer, [include_defaults](config::base_property& p) {
                return include_defaults || !p.is_default();
            });

          reply.set_status(ss::httpd::reply::status_type::ok, buf.GetString());
          return "";
      });

    register_route_raw<superuser>(
      ss::httpd::config_json::get_node_config,
      [](ss::const_req, ss::reply& reply) {
          json::StringBuffer buf;
          json::Writer<json::StringBuffer> writer(buf);
          config::node().to_json(writer);

          reply.set_status(ss::httpd::reply::status_type::ok, buf.GetString());
          return "";
      });

    register_route<superuser>(
      ss::httpd::config_json::set_log_level,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          auto name = req->param["name"];

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

          co_return ss::json::json_return_type(ss::json::json_void());
      });
}

static json_validator make_cluster_config_validator() {
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
    return json_validator(schema);
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
        modified_keys.insert(i.first);
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
        std::vector<std::reference_wrapper<
          const config::property<std::optional<ss::sstring>>>>
          properties{
            std::ref(updated_config.cloud_storage_secret_key),
            std::ref(updated_config.cloud_storage_access_key),
            std::ref(updated_config.cloud_storage_region),
            std::ref(updated_config.cloud_storage_bucket)};
        for (auto& p : properties) {
            if (p() == std::nullopt) {
                errors[ss::sstring(p.get().name())]
                  = "Must be set when cloud storage enabled";
            }
        }
    }
}

void admin_server::register_cluster_config_routes() {
    static thread_local auto cluster_config_validator(
      make_cluster_config_validator());

    register_route<superuser>(
      ss::httpd::cluster_config_json::get_cluster_config_status,
      [this](std::unique_ptr<ss::httpd::request>)
        -> ss::future<ss::json::json_return_type> {
          std::vector<ss::httpd::cluster_config_json::cluster_config_status>
            res;

          auto& cfg = _controller->get_config_manager();
          auto statuses = co_await cfg.invoke_on(
            cluster::controller_stm_shard,
            [](cluster::config_manager& manager) {
                // Intentional copy, do not want to pass reference to mutable
                // status map back to originating core.
                return cluster::config_manager::status_map(
                  manager.get_status());
            });

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

          co_return ss::json::json_return_type(std::move(res));
      });

    register_route<publik>(
      ss::httpd::cluster_config_json::get_cluster_config_schema,
      [](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          using property_map = std::map<
            ss::sstring,
            ss::httpd::cluster_config_json::cluster_config_property_metadata>;

          property_map properties;

          config::shard_local_cfg().for_each(
            [&properties](const config::base_property& p) {
                if (p.get_visibility() == config::visibility::deprecated) {
                    // Do not mention deprecated settings in schema: they
                    // only exist internally to avoid making existing stored
                    // configs invalid.
                    return;
                }

                auto [pm_i, inserted] = properties.emplace(
                  ss::sstring(p.name()),
                  ss::httpd::cluster_config_json::
                    cluster_config_property_metadata());
                vassert(inserted, "Emplace failed, duplicate property name?");

                auto& pm = pm_i->second;
                pm.description = ss::sstring(p.desc());
                pm.needs_restart = p.needs_restart();
                pm.visibility = ss::sstring(
                  config::to_string_view(p.get_visibility()));
                pm.nullable = p.is_nullable();
                pm.is_secret = p.is_secret();

                if (p.is_array()) {
                    pm.type = "array";

                    auto items = ss::httpd::cluster_config_json::
                      cluster_config_property_metadata_items();
                    items.type = ss::sstring(p.type_name());
                    pm.items = items;
                } else {
                    pm.type = ss::sstring(p.type_name());
                }

                auto enum_values = p.enum_values();
                if (!enum_values.empty()) {
                    // In swagger, this field would just be called 'enum', but
                    // because we use C++ code generation for our structures,
                    // we cannot use that reserved word
                    pm.enum_values = enum_values;
                }

                const auto& example = p.example();
                if (example.has_value()) {
                    pm.example = ss::sstring(example.value());
                }

                const auto& units = p.units_name();
                if (units.has_value()) {
                    pm.units = ss::sstring(units.value());
                }
            });

          std::map<ss::sstring, property_map> response = {
            {ss::sstring("properties"), std::move(properties)}};

          co_return ss::json::json_return_type(std::move(response));
      });

    register_route<superuser, true>(
      ss::httpd::cluster_config_json::patch_cluster_config,
      [this](
        std::unique_ptr<ss::httpd::request> req,
        request_auth_result const& auth_state)
        -> ss::future<ss::json::json_return_type> {
          if (!_controller->get_feature_table().local().is_active(
                cluster::feature::central_config)) {
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
              for (const auto& i : update.upsert) {
                  // Decode to a YAML object because that's what the property
                  // interface expects.
                  // Don't both catching ParserException: this was encoded
                  // just a few lines above.
                  const auto& yaml_value = i.second;
                  auto val = YAML::Load(yaml_value);

                  if (!cfg.contains(i.first)) {
                      errors[i.first] = "Unknown property";
                      continue;
                  }
                  auto& property = cfg.get(i.first);

                  try {
                      auto validation_err = property.validate(val);
                      if (validation_err.has_value()) {
                          errors[i.first]
                            = validation_err.value().error_message();
                          vlog(
                            logger.warn,
                            "Invalid {}: '{}' ({})",
                            i.first,
                            yaml_value,
                            validation_err.value().error_message());
                      } else {
                          // In case any property subclass might throw
                          // from it's value setter even after a non-throwing
                          // call to validate (if this happens validate() was
                          // implemented wrongly, but let's be safe)
                          property.set_value(val);
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
                              message = fmt::format("out of range");
                          } catch (...) {
                              // This was not an out-of-bounds case, use
                              // the type error message
                          }
                      }

                      errors[i.first] = message;
                      vlog(
                        logger.warn,
                        "Invalid {}: '{}' ({})",
                        i.first,
                        yaml_value,
                        std::current_exception());
                  } catch (...) {
                      auto message = fmt::format(
                        "{}", std::current_exception());
                      errors[i.first] = message;
                      vlog(
                        logger.warn,
                        "Invalid {}: '{}' ({})",
                        i.first,
                        yaml_value,
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
                    ss::httpd::reply::status_type::bad_request,
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
              ss::httpd::cluster_config_json::cluster_config_write_result
                result;
              result.config_version = current_version;
              co_return ss::json::json_return_type(std::move(result));
          }

          vlog(
            logger.trace,
            "patch_cluster_config: {} upserts, {} removes",
            update.upsert.size(),
            update.remove.size());

          auto patch_result
            = co_await _controller->get_config_frontend().invoke_on(
              cluster::config_frontend::version_shard,
              [update = std::move(update)](cluster::config_frontend& fe) mutable
              -> ss::future<cluster::config_frontend::patch_result> {
                  return fe.patch(
                    std::move(update), model::timeout_clock::now() + 5s);
              });

          co_await throw_on_error(
            *req, patch_result.errc, model::controller_ntp);

          ss::httpd::cluster_config_json::cluster_config_write_result result;
          result.config_version = patch_result.version;
          co_return ss::json::json_return_type(std::move(result));
      });
}

void admin_server::register_raft_routes() {
    register_route<superuser>(
      ss::httpd::raft_json::raft_transfer_leadership,
      [this](std::unique_ptr<ss::httpd::request> req) {
          raft::group_id group_id;
          try {
              group_id = raft::group_id(std::stoll(req->param["group_id"]));
          } catch (...) {
              throw ss::httpd::bad_param_exception(fmt::format(
                "Raft group id must be an integer: {}",
                req->param["group_id"]));
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

          return _partition_manager.invoke_on(
            shard,
            [group_id, target, this, req = std::move(req)](
              cluster::partition_manager& pm) mutable {
                auto consensus = pm.consensus_for(group_id);
                if (!consensus) {
                    throw ss::httpd::not_found_exception();
                }
                const auto ntp = consensus->ntp();
                return consensus->do_transfer_leadership(target).then(
                  [this, req = std::move(req), ntp](std::error_code err)
                    -> ss::future<ss::json::json_return_type> {
                      co_await throw_on_error(*req, err, ntp);
                      co_return ss::json::json_return_type(
                        ss::json::json_void());
                  });
            });
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

    if (!doc.HasMember("password") || !doc["password"].IsString()) {
        throw ss::httpd::bad_request_exception(
          fmt::format("String password smissing"));
    }
    const auto password = doc["password"].GetString();

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

void admin_server::register_security_routes() {
    register_route<superuser>(
      ss::httpd::security_json::create_user,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          auto doc = parse_json_body(*req);

          auto credential = parse_scram_credential(doc);

          if (!doc.HasMember("username") || !doc["username"].IsString()) {
              throw ss::httpd::bad_request_exception(
                fmt::format("String username missing"));
          }

          auto username = security::credential_user(
            doc["username"].GetString());

          auto err
            = co_await _controller->get_security_frontend().local().create_user(
              username, credential, model::timeout_clock::now() + 5s);
          vlog(
            logger.debug,
            "Creating user '{}' {}:{}",
            username,
            err,
            err.message());

          if (err == cluster::errc::user_exists) {
              // Idempotency: if user is same as one that already exists,
              // suppress the user_exists error and return success.
              const auto& credentials_store
                = _controller->get_credential_store().local();
              std::optional<security::scram_credential> creds
                = credentials_store.get<security::scram_credential>(username);
              if (
                creds.has_value()
                && match_scram_credential(doc, creds.value())) {
                  co_return ss::json::json_return_type(ss::json::json_void());
              }
          }

          co_await throw_on_error(*req, err, model::controller_ntp);
          co_return ss::json::json_return_type(ss::json::json_void());
      });

    register_route<superuser>(
      ss::httpd::security_json::delete_user,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          auto user = security::credential_user(req->param["user"]);

          auto err
            = co_await _controller->get_security_frontend().local().delete_user(
              user, model::timeout_clock::now() + 5s);
          vlog(
            logger.debug, "Deleting user '{}' {}:{}", user, err, err.message());
          if (err == cluster::errc::user_does_not_exist) {
              // Idempotency: removing a non-existent user is successful.
              co_return ss::json::json_return_type(ss::json::json_void());
          }
          co_await throw_on_error(*req, err, model::controller_ntp);
          co_return ss::json::json_return_type(ss::json::json_void());
      });

    register_route<superuser>(
      ss::httpd::security_json::update_user,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          auto user = security::credential_user(req->param["user"]);

          auto doc = parse_json_body(*req);

          auto credential = parse_scram_credential(doc);

          auto err
            = co_await _controller->get_security_frontend().local().update_user(
              user, credential, model::timeout_clock::now() + 5s);
          vlog(logger.debug, "Updating user {}:{}", err, err.message());
          co_await throw_on_error(*req, err, model::controller_ntp);
          co_return ss::json::json_return_type(ss::json::json_void());
      });

    register_route<superuser>(
      ss::httpd::security_json::list_users,
      [this](std::unique_ptr<ss::httpd::request>) {
          std::vector<ss::sstring> users;
          for (const auto& [user, _] :
               _controller->get_credential_store().local()) {
              users.push_back(user());
          }
          return ss::make_ready_future<ss::json::json_return_type>(
            std::move(users));
      });
}

void admin_server::register_kafka_routes() {
    register_route<superuser>(
      ss::httpd::partition_json::kafka_transfer_leadership,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          auto ns = model::ns(req->param["namespace"]);

          auto topic = model::topic(req->param["topic"]);

          model::partition_id partition;
          try {
              partition = model::partition_id(
                std::stoll(req->param["partition"]));
          } catch (...) {
              throw ss::httpd::bad_param_exception(fmt::format(
                "Partition id must be an integer: {}",
                req->param["partition"]));
          }

          if (partition() < 0) {
              throw ss::httpd::bad_param_exception(
                fmt::format("Invalid partition id {}", partition));
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
            "Leadership transfer request for leader of topic-partition "
            "{}:{}:{} "
            "to node {}",
            ns,
            topic,
            partition,
            target);

          model::ntp ntp(ns, topic, partition);

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
                return partition->transfer_leadership(target).then(
                  [this, req = std::move(req), ntp](std::error_code err)
                    -> ss::future<ss::json::json_return_type> {
                      co_await throw_on_error(*req, err, ntp);
                      co_return ss::json::json_return_type(
                        ss::json::json_void());
                  });
            });
      });
}

void admin_server::register_status_routes() {
    register_route<publik>(
      ss::httpd::status_json::ready,
      [this](std::unique_ptr<ss::httpd::request>) {
          std::unordered_map<ss::sstring, ss::sstring> status_map{
            {"status", _ready ? "ready" : "booting"}};
          return ss::make_ready_future<ss::json::json_return_type>(status_map);
      });
}

static json_validator make_feature_put_validator() {
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
    return json_validator(schema);
}

void admin_server::register_features_routes() {
    static thread_local auto feature_put_validator(
      make_feature_put_validator());

    register_route<user>(
      ss::httpd::features_json::get_features,
      [this](std::unique_ptr<ss::httpd::request>)
        -> ss::future<ss::json::json_return_type> {
          ss::httpd::features_json::features_response res;

          const auto& ft = _controller->get_feature_table().local();
          auto version = ft.get_active_version();

          res.cluster_version = version;
          for (const auto& fs : ft.get_feature_state()) {
              ss::httpd::features_json::feature_state item;
              vlog(
                logger.trace,
                "feature_state: {} {}",
                fs.spec.name,
                fs.get_state());
              item.name = ss::sstring(fs.spec.name);

              switch (fs.get_state()) {
              case cluster::feature_state::state::active:
                  item.state = ss::httpd::features_json::feature_state::
                    feature_state_state::active;
                  break;
              case cluster::feature_state::state::unavailable:
                  item.state = ss::httpd::features_json::feature_state::
                    feature_state_state::unavailable;
                  break;
              case cluster::feature_state::state::available:
                  item.state = ss::httpd::features_json::feature_state::
                    feature_state_state::available;
                  break;
              case cluster::feature_state::state::preparing:
                  item.state = ss::httpd::features_json::feature_state::
                    feature_state_state::preparing;
                  break;
              case cluster::feature_state::state::disabled_clean:
              case cluster::feature_state::state::disabled_active:
              case cluster::feature_state::state::disabled_preparing:
                  item.state = ss::httpd::features_json::feature_state::
                    feature_state_state::disabled;
                  break;
              }

              switch (fs.get_state()) {
              case cluster::feature_state::state::active:
              case cluster::feature_state::state::preparing:
              case cluster::feature_state::state::disabled_active:
              case cluster::feature_state::state::disabled_preparing:
                  item.was_active = true;
                  break;
              default:
                  item.was_active = false;
              }

              res.features.push(item);
          }

          co_return std::move(res);
      });

    register_route<superuser>(
      ss::httpd::features_json::put_feature,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
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
              action.action
                = cluster::feature_update_action::action_t::activate;
          } else if (new_state_str == "disabled") {
              action.action
                = cluster::feature_update_action::action_t::deactivate;
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
      });
}

model::node_id parse_broker_id(const ss::httpd::request& req) {
    try {
        return model::node_id(
          boost::lexical_cast<model::node_id::type>(req.param["id"]));
    } catch (...) {
        throw ss::httpd::bad_param_exception(
          fmt::format("Broker id: {}, must be an integer", req.param["id"]));
    }
}

void admin_server::register_broker_routes() {
    register_route<user>(
      ss::httpd::broker_json::get_cluster_view,
      [this](std::unique_ptr<ss::httpd::request>) {
          cluster::node_report_filter filter;

          return _controller->get_health_monitor()
            .local()
            .get_cluster_health(
              cluster::cluster_report_filter{
                .node_report_filter = std::move(filter),
              },
              cluster::force_refresh::no,
              model::no_timeout)
            .then([this](result<cluster::cluster_health_report> h_report) {
                if (h_report.has_error()) {
                    throw ss::httpd::base_exception(
                      fmt::format(
                        "Unable to get cluster health: {}",
                        h_report.error().message()),
                      ss::httpd::reply::status_type::service_unavailable);
                }

                std::map<model::node_id, ss::httpd::broker_json::broker> result;

                auto& members_table = _controller->get_members_table().local();
                for (auto& broker : members_table.all_brokers()) {
                    ss::httpd::broker_json::broker b;
                    b.node_id = broker->id();
                    b.num_cores = broker->properties().cores;
                    b.membership_status = fmt::format(
                      "{}", broker->get_membership_state());
                    b.is_alive = true;
                    result[broker->id()] = b;
                }

                for (auto& ns : h_report.value().node_states) {
                    auto it = result.find(ns.id);
                    if (it == result.end()) {
                        continue;
                    }
                    it->second.is_alive = (bool)ns.is_alive;

                    auto r_it = std::find_if(
                      h_report.value().node_reports.begin(),
                      h_report.value().node_reports.end(),
                      [id = ns.id](const cluster::node_health_report& nhr) {
                          return nhr.id == id;
                      });
                    if (r_it != h_report.value().node_reports.end()) {
                        it->second.version = r_it->local_state.redpanda_version;
                        for (auto& ds : r_it->local_state.disks) {
                            ss::httpd::broker_json::disk_space_info dsi;
                            dsi.path = ds.path;
                            dsi.free = ds.free;
                            dsi.total = ds.total;
                            it->second.disk_space.push(dsi);
                        }
                    }
                }

                ss::httpd::broker_json::cluster_view ret;
                ret.version = members_table.version();
                for (auto& [_, b] : result) {
                    ret.brokers.push(b);
                }

                return ss::make_ready_future<ss::json::json_return_type>(
                  std::move(ret));
            });
      });

    register_route<user>(
      ss::httpd::broker_json::get_brokers,
      [this](std::unique_ptr<ss::httpd::request>) {
          cluster::node_report_filter filter;

          return _controller->get_health_monitor()
            .local()
            .get_cluster_health(
              cluster::cluster_report_filter{
                .node_report_filter = std::move(filter),
              },
              cluster::force_refresh::no,
              model::no_timeout)
            .then([this](result<cluster::cluster_health_report> h_report) {
                if (h_report.has_error()) {
                    throw ss::httpd::base_exception(
                      fmt::format(
                        "Unable to get cluster health: {}",
                        h_report.error().message()),
                      ss::httpd::reply::status_type::service_unavailable);
                }

                std::vector<ss::httpd::broker_json::broker> res;

                for (auto& ns : h_report.value().node_states) {
                    auto broker = _metadata_cache.local().get_broker(ns.id);
                    if (!broker) {
                        continue;
                    }
                    auto& b = res.emplace_back();
                    b.node_id = ns.id;
                    b.num_cores = (*broker)->properties().cores;
                    b.membership_status = fmt::format(
                      "{}", ns.membership_state);
                    b.is_alive = (bool)ns.is_alive;

                    auto r_it = std::find_if(
                      h_report.value().node_reports.begin(),
                      h_report.value().node_reports.end(),
                      [id = ns.id](const cluster::node_health_report& nhr) {
                          return nhr.id == id;
                      });
                    if (r_it != h_report.value().node_reports.end()) {
                        b.version = r_it->local_state.redpanda_version;
                        for (auto& ds : r_it->local_state.disks) {
                            ss::httpd::broker_json::disk_space_info dsi;
                            dsi.path = ds.path;
                            dsi.free = ds.free;
                            dsi.total = ds.total;
                            b.disk_space.push(dsi);
                        }
                    }
                }

                return ss::make_ready_future<ss::json::json_return_type>(
                  std::move(res));
            });
      });

    register_route<user>(
      ss::httpd::broker_json::get_broker,
      [this](std::unique_ptr<ss::httpd::request> req) {
          model::node_id id = parse_broker_id(*req);
          auto broker = _metadata_cache.local().get_broker(id);
          if (!broker) {
              throw ss::httpd::not_found_exception(
                fmt::format("broker with id: {} not found", id));
          }
          ss::httpd::broker_json::broker ret;
          ret.node_id = (*broker)->id();
          ret.num_cores = (*broker)->properties().cores;
          ret.membership_status = fmt::format(
            "{}", (*broker)->get_membership_state());
          return ss::make_ready_future<ss::json::json_return_type>(ret);
      });

    register_route<superuser>(
      ss::httpd::broker_json::decommission,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          model::node_id id = parse_broker_id(*req);

          auto ec = co_await _controller->get_members_frontend()
                      .local()
                      .decommission_node(id);

          co_await throw_on_error(*req, ec, model::controller_ntp, id);
          co_return ss::json::json_void();
      });

    register_route<superuser>(
      ss::httpd::broker_json::recommission,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          model::node_id id = parse_broker_id(*req);

          auto ec = co_await _controller->get_members_frontend()
                      .local()
                      .recommission_node(id);
          co_await throw_on_error(*req, ec, model::controller_ntp, id);
          co_return ss::json::json_void();
      });

    register_route<superuser>(
      ss::httpd::broker_json::start_broker_maintenance,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          if (!_controller->get_feature_table().local().is_active(
                cluster::feature::maintenance_mode)) {
              throw ss::httpd::bad_request_exception(
                "Maintenance mode feature not active (upgrade in progress?)");
          }
          model::node_id id = parse_broker_id(*req);
          auto ec = co_await _controller->get_members_frontend()
                      .local()
                      .set_maintenance_mode(id, true);
          co_await throw_on_error(*req, ec, model::controller_ntp, id);
          co_return ss::json::json_void();
      });

    register_route<superuser>(
      ss::httpd::broker_json::stop_broker_maintenance,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          if (!_controller->get_feature_table().local().is_active(
                cluster::feature::maintenance_mode)) {
              throw ss::httpd::bad_request_exception(
                "Maintenance mode feature not active (upgrade in progress?)");
          }
          model::node_id id = parse_broker_id(*req);
          auto ec = co_await _controller->get_members_frontend()
                      .local()
                      .set_maintenance_mode(id, false);
          co_await throw_on_error(*req, ec, model::controller_ntp, id);
          co_return ss::json::json_void();
      });

    /*
     * Unlike start|stop_broker_maintenace, the xxx_local_maintenance versions
     * below operate on local state only and could be used to force a node out
     * of maintenance mode if needed. they don't require the feature flag
     * because the feature is available locally.
     */
    register_route<superuser>(
      ss::httpd::broker_json::start_local_maintenance,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          co_await _controller->get_drain_manager().invoke_on_all(
            [](cluster::drain_manager& dm) { return dm.drain(); });
          co_return ss::json::json_void();
      });

    register_route<superuser>(
      ss::httpd::broker_json::stop_local_maintenance,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          co_await _controller->get_drain_manager().invoke_on_all(
            [](cluster::drain_manager& dm) { return dm.restore(); });
          co_return ss::json::json_void();
      });

    register_route<superuser>(
      ss::httpd::broker_json::get_local_maintenance,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          auto status
            = co_await _controller->get_drain_manager().local().status();
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
          co_return res;
      });
}

// Helpers for partition routes
namespace {
model::ntp parse_ntp_from_request(ss::httpd::parameters& param) {
    auto ns = model::ns(param["namespace"]);
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

    return model::ntp(std::move(ns), std::move(topic), partition);
}

} // namespace

void admin_server::register_partition_routes() {
    /*
     * Get a list of partition summaries.
     */
    register_route<user>(
      ss::httpd::partition_json::get_partitions,
      [this](std::unique_ptr<ss::httpd::request>) {
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
                return ss::make_ready_future<ss::json::json_return_type>(
                  std::move(partitions));
            });
      });

    /*
     * Get detailed information about a partition.
     */
    register_route<user>(
      ss::httpd::partition_json::get_partition,
      [this](std::unique_ptr<ss::httpd::request> req) {
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
              // Controller topic is on all nodes.  Report all nodes,
              // with the leader first.
              auto leader_opt
                = _metadata_cache.local().get_controller_leader_id();
              if (leader_opt.has_value()) {
                  ss::httpd::partition_json::assignment a;
                  a.node_id = leader_opt.value();
                  a.core = cluster::controller_stm_shard;
                  p.replicas.push(a);
                  p.leader_id = *leader_opt;
              }
              // special case, controller is raft group 0
              p.raft_group_id = 0;
              for (const auto& i : _metadata_cache.local().all_broker_ids()) {
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
              p.status = ssx::sformat(
                "{}", cluster::reconciliation_status::done);
              return ss::make_ready_future<ss::json::json_return_type>(
                std::move(p));

          } else {
              // Normal topic
              auto assignment = _controller->get_topics_state()
                                  .local()
                                  .get_partition_assignment(ntp);

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
                .then(
                  [p](const cluster::ntp_reconciliation_state& state) mutable {
                      p.status = ssx::sformat("{}", state.status());
                      return ss::make_ready_future<ss::json::json_return_type>(
                        std::move(p));
                  });
          }
      });

    /*
     * Get detailed information about transactions for partition.
     */
    register_route<user>(
      ss::httpd::partition_json::get_transactions,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
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
            [_ntp = std::move(ntp), _req = std::move(req), this](
              cluster::partition_manager& pm) mutable
            -> ss::future<ss::json::json_return_type> {
                auto ntp = std::move(_ntp);
                auto req = std::move(_req);
                auto partition = pm.get(ntp);
                if (!partition) {
                    throw ss::httpd::server_error_exception(fmt_with_ctx(
                      fmt::format, "Can not find partition {}", partition));
                }

                auto rm_stm_ptr = partition->rm_stm();

                if (!rm_stm_ptr) {
                    throw ss::httpd::server_error_exception(fmt_with_ctx(
                      fmt::format,
                      "Can not get rm_stm for partition {}",
                      partition));
                }

                auto transactions = co_await rm_stm_ptr->get_transactions();

                if (transactions.has_error()) {
                    co_await throw_on_error(*req, transactions.error(), ntp);
                }
                ss::httpd::partition_json::transactions ans;

                auto offset_translator
                  = partition->get_offset_translator_state();

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
                    new_tx.staleness_ms = staleness.has_value()
                                            ? staleness.value().count()
                                            : -1;
                    auto timeout = tx_info.get_timeout();
                    // -1 is returned for expired transaction, because
                    // timeout is useless for expired tx.
                    new_tx.timeout_ms = timeout.has_value()
                                          ? timeout.value().count()
                                          : -1;

                    if (tx_info.is_expired()) {
                        ans.expired_transactions.push(new_tx);
                    } else {
                        ans.active_transactions.push(new_tx);
                    }
                }

                co_return ss::json::json_return_type(ans);
            });
      });

    /*
     * Abort transaction for partition
     */
    register_route<superuser>(
      ss::httpd::partition_json::mark_transaction_expired,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
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
                  throw ss::httpd::bad_param_exception(fmt_with_ctx(
                    fmt::format, "Invalid transaction epoch {}", epoch));
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
                    throw ss::httpd::server_error_exception(fmt_with_ctx(
                      fmt::format, "Can not find partition {}", partition));
                }

                auto rm_stm_ptr = partition->rm_stm();

                if (!rm_stm_ptr) {
                    throw ss::httpd::server_error_exception(fmt_with_ctx(
                      fmt::format,
                      "Can not get rm_stm for partition {}",
                      partition));
                }

                auto res = co_await rm_stm_ptr->mark_expired(pid);
                co_await throw_on_error(*req, res, ntp);

                co_return ss::json::json_return_type(ss::json::json_void());
            });
      });

    // make sure to call reset() before each use
    static thread_local json_validator set_replicas_validator(
      make_set_replicas_validator());

    register_route<superuser>(
      ss::httpd::partition_json::set_partition_replicas,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          auto ns = model::ns(req->param["namespace"]);
          auto topic = model::topic(req->param["topic"]);

          model::partition_id partition;
          try {
              partition = model::partition_id(
                std::stoi(req->param["partition"]));
          } catch (...) {
              throw ss::httpd::bad_param_exception(fmt::format(
                "Partition id must be an integer: {}",
                req->param["partition"]));
          }

          if (partition() < 0) {
              throw ss::httpd::bad_param_exception(
                fmt::format("Invalid partition id {}", partition));
          }

          const model::ntp ntp(std::move(ns), std::move(topic), partition);

          if (ntp == model::controller_ntp) {
              throw ss::httpd::bad_request_exception(
                fmt::format("Can't reconfigure a controller"));
          }

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
              bool is_valid = co_await _controller->get_topics_frontend()
                                .local()
                                .validate_shard(node_id, shard);
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
                                        [node_id](
                                          const model::broker_shard& bs) {
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
            = _controller->get_topics_state().local().get_partition_assignment(
              ntp);

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
            logger.info,
            "Request to change ntp {} replica set to {}",
            ntp,
            replicas);

          auto err
            = co_await _controller->get_topics_frontend()
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
      });
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
          [](std::unique_ptr<ss::httpd::request>) {
              ss::httpd::hbadger_json::failure_injector_status status;
              status.enabled = false;
              return ss::make_ready_future<ss::json::json_return_type>(
                std::move(status));
          });
        return;
    }

    register_route<user>(
      ss::httpd::hbadger_json::get_failure_probes,
      [](std::unique_ptr<ss::httpd::request>) {
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
      [](std::unique_ptr<ss::httpd::request> req) {
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
      [](std::unique_ptr<ss::httpd::request> req) {
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

void admin_server::register_transaction_routes() {
    register_route<user>(
      ss::httpd::transaction_json::get_all_transactions,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          if (!config::shard_local_cfg().enable_transactions) {
              throw ss::httpd::bad_request_exception(
                "Transaction are disabled");
          }

          if (need_redirect_to_leader(model::tx_manager_ntp, _metadata_cache)) {
              throw co_await redirect_to_leader(*req, model::tx_manager_ntp);
          }

          auto& tx_frontend = _partition_manager.local().get_tx_frontend();
          if (!tx_frontend.local_is_initialized()) {
              throw ss::httpd::bad_request_exception("Can not get tx_frontend");
          }

          auto res = co_await tx_frontend.local().get_all_transactions();
          if (!res.has_value()) {
              co_await throw_on_error(*req, res.error(), model::tx_manager_ntp);
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
      });

    register_route<user>(
      ss::httpd::transaction_json::delete_partition,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          if (need_redirect_to_leader(model::tx_manager_ntp, _metadata_cache)) {
              throw co_await redirect_to_leader(*req, model::tx_manager_ntp);
          }

          auto transaction_id = req->param["transactional_id"];

          auto namespace_from_req = req->get_query_param("namespace");
          auto topic_from_req = req->get_query_param("topic");

          auto partition_str = req->get_query_param("partition_id");
          int64_t partition;
          try {
              partition = std::stoi(partition_str);
          } catch (...) {
              throw ss::httpd::bad_param_exception(
                fmt::format("Partition must be an integer: {}", partition_str));
          }

          if (partition < 0) {
              throw ss::httpd::bad_param_exception(
                fmt::format("Invalid partition {}", partition));
          }

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

          model::ntp ntp(namespace_from_req, topic_from_req, partition);
          cluster::tm_transaction::tx_partition partition_for_delete{
            .ntp = ntp, .etag = model::term_id(etag)};
          kafka::transactional_id tid(transaction_id);

          auto& tx_frontend = _partition_manager.local().get_tx_frontend();
          if (!tx_frontend.local_is_initialized()) {
              throw ss::httpd::bad_request_exception(
                "Transaction are disabled");
          }

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
      });
}

void admin_server::register_debug_routes() {
    register_route<user>(
      ss::httpd::debug_json::reset_leaders_info,
      [this](std::unique_ptr<ss::httpd::request>)
        -> ss::future<ss::json::json_return_type> {
          vlog(logger.info, "Request to reset leaders info");
          co_await _metadata_cache.invoke_on_all(
            [](auto& mc) { mc.reset_leaders(); });

          co_return ss::json::json_void();
      });

    register_route<user>(
      ss::httpd::debug_json::get_leaders_info,
      [this](std::unique_ptr<ss::httpd::request>)
        -> ss::future<ss::json::json_return_type> {
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

          co_return ss::json::json_return_type(ans);
      });
}

void admin_server::register_cluster_routes() {
    register_route<publik>(
      ss::httpd::cluster_json::get_cluster_health_overview,
      [this](std::unique_ptr<ss::httpd::request>)
        -> ss::future<ss::json::json_return_type> {
          vlog(logger.debug, "Requested cluster status");
          auto health_overview = co_await _controller->get_health_monitor()
                                   .local()
                                   .get_cluster_health_overview(
                                     model::time_from_now(
                                       std::chrono::seconds(5)));
          ss::httpd::cluster_json::cluster_health_overview ret;
          ret.is_healthy = health_overview.is_healthy;
          ret.all_nodes._set = true;
          ret.nodes_down._set = true;
          ret.leaderless_partitions._set = true;

          ret.all_nodes = health_overview.all_nodes;
          ret.nodes_down = health_overview.nodes_down;

          for (auto& ntp : health_overview.leaderless_partitions) {
              ret.leaderless_partitions.push(fmt::format(
                "{}/{}/{}", ntp.ns(), ntp.tp.topic(), ntp.tp.partition));
          }
          if (health_overview.controller_id) {
              ret.controller_id = health_overview.controller_id.value();
          } else {
              ret.controller_id = -1;
          }

          co_return ss::json::json_return_type(ret);
      });
}
