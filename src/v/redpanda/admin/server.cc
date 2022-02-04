/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "redpanda/admin/server.h"

#include "cluster/cluster_utils.h"
#include "cluster/controller.h"
#include "cluster/controller_api.h"
#include "cluster/errc.h"
#include "cluster/fwd.h"
#include "cluster/members_frontend.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_manager.h"
#include "cluster/security_frontend.h"
#include "cluster/shard_table.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "config/endpoint_tls_config.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record.h"
#include "net/dns.h"
#include "raft/types.h"
#include "redpanda/admin/api-doc/cluster_config.json.h"
#include "redpanda/admin/api-doc/config.json.h"
#include "redpanda/admin/api-doc/raft.json.h"
#include "redpanda/admin/api-doc/security.json.h"
#include "redpanda/admin/api-doc/status.json.h"
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
#include <seastar/http/json_path.hh>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/lexical_cast/bad_lexical_cast.hpp>
#include <fmt/core.h>
#include <rapidjson/document.h>
#include <rapidjson/schema.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <limits>
#include <stdexcept>
#include <system_error>
#include <unordered_map>

using namespace std::chrono_literals;

namespace admin {

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
  , _metadata_cache(metadata_cache) {}

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
    rb->register_api_file(_server._routes, "hbadger");
    rb->register_function(_server._routes, insert_comma);
    rb->register_api_file(_server._routes, "broker");

    register_config_routes();
    register_cluster_config_routes();
    register_raft_routes();
    register_security_routes();
    register_status_routes();
    register_broker_routes();
    register_partition_routes();
    register_hbadger_routes();
}

/**
 * A helper around rapidjson's Parse that checks for errors & raises
 * seastar HTTP exception.  Without that check, something as simple
 * as an empty request body causes a redpanda crash via a rapidjson
 * assertion when trying to GetObject on the resulting document.
 */
rapidjson::Document parse_json_body(ss::httpd::request const& req) {
    rapidjson::Document doc;
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
void apply_validator(
  json_validator& validator, rapidjson::Document const& doc) {
    validator.validator.Reset();
    validator.validator.ResetError();

    if (!doc.Accept(validator.validator)) {
        rapidjson::StringBuffer val_buf;
        rapidjson::Writer<rapidjson::StringBuffer> w{val_buf};
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
bool get_boolean_query_param(
  const ss::httpd::request& req, std::string_view name) {
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
    for (auto& ep : _cfg.endpoints) {
        // look for credentials matching current endpoint
        auto tls_it = std::find_if(
          _cfg.endpoints_tls.begin(),
          _cfg.endpoints_tls.end(),
          [&ep](const config::endpoint_tls_config& c) {
              return c.name == ep.name;
          });

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
        }
        auto resolved = co_await net::resolve_dns(ep.address);
        co_await ss::with_scheduling_group(_cfg.sg, [this, cred, resolved] {
            return _server.listen(resolved, cred);
        });
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
        if (colon == std::string::npos) {
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
        default:
            throw ss::httpd::server_error_exception(
              fmt::format("Unexpected tx_error error: {}", ec.message()));
        }
    } else {
        throw ss::httpd::server_error_exception(
          fmt::format("Unexpected error: {}", ec.message()));
    }
}

void admin_server::register_config_routes() {
    static ss::httpd::handle_function get_config_handler =
      []([[maybe_unused]] ss::const_req req, ss::reply& reply) {
          rapidjson::StringBuffer buf;
          rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
          config::shard_local_cfg().to_json(writer);

          reply.set_status(ss::httpd::reply::status_type::ok, buf.GetString());
          return "";
      };

    auto get_config_handler_f = new ss::httpd::function_handler{
      get_config_handler, "json"};

    ss::httpd::config_json::get_config.set(
      _server._routes, get_config_handler_f);

    static ss::httpd::handle_function get_node_config_handler =
      []([[maybe_unused]] ss::const_req req, ss::reply& reply) {
          rapidjson::StringBuffer buf;
          rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
          config::node().to_json(writer);

          reply.set_status(ss::httpd::reply::status_type::ok, buf.GetString());
          return "";
      };

    auto get_node_config_handler_f = new ss::httpd::function_handler{
      get_node_config_handler, "json"};

    ss::httpd::config_json::get_node_config.set(
      _server._routes, get_node_config_handler_f);

    ss::httpd::config_json::set_log_level.set(
      _server._routes, [this](ss::const_req req) {
          auto name = req.param["name"];

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
                req.get_query_param("level"));
          } catch (const boost::bad_lexical_cast& e) {
              throw ss::httpd::bad_param_exception(fmt::format(
                "Cannot set log level for {{{}}}: unknown level {{{}}}",
                name,
                req.get_query_param("level")));
          }

          // how long should the new log level be active
          std::optional<std::chrono::seconds> expires;
          if (auto e = req.get_query_param("expires"); !e.empty()) {
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

          return ss::json::json_return_type(ss::json::json_void());
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

void admin_server::register_cluster_config_routes() {
    static thread_local auto cluster_config_validator(
      make_cluster_config_validator());

    ss::httpd::cluster_config_json::get_cluster_config_status.set(
      _server._routes,
      [this](std::unique_ptr<ss::httpd::request> req)
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

    ss::httpd::cluster_config_json::get_cluster_config_schema.set(
      _server._routes,
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

    ss::httpd::cluster_config_json::patch_cluster_config.set(
      _server._routes,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          if (!config::node().enable_central_config) {
              throw ss::httpd::bad_request_exception(
                "Requires enable_central_config=True in node configuration");
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
              rapidjson::StringBuffer val_buf;
              rapidjson::Writer<rapidjson::StringBuffer> w{val_buf};
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

              if (!errors.empty()) {
                  // TODO structured response
                  throw ss::httpd::bad_request_exception(
                    fmt::format("Invalid properties"));
              }
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
    ss::httpd::raft_json::raft_transfer_leadership.set(
      _server._routes, [this](std::unique_ptr<ss::httpd::request> req) {
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
parse_scram_credential(const rapidjson::Document& doc) {
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

void admin_server::register_security_routes() {
    ss::httpd::security_json::create_user.set(
      _server._routes,
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
          vlog(logger.debug, "Creating user {}:{}", err, err.message());
          co_await throw_on_error(*req, err, model::controller_ntp);
          co_return ss::json::json_return_type(ss::json::json_void());
      });

    ss::httpd::security_json::delete_user.set(
      _server._routes,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          auto user = security::credential_user(req->param["user"]);

          auto err
            = co_await _controller->get_security_frontend().local().delete_user(
              user, model::timeout_clock::now() + 5s);
          vlog(logger.debug, "Deleting user {}:{}", err, err.message());
          co_await throw_on_error(*req, err, model::controller_ntp);
          co_return ss::json::json_return_type(ss::json::json_void());
      });

    ss::httpd::security_json::update_user.set(
      _server._routes,
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

    ss::httpd::security_json::list_users.set(
      _server._routes, [this](std::unique_ptr<ss::httpd::request>) {
          std::vector<ss::sstring> users;
          for (const auto& [user, _] :
               _controller->get_credential_store().local()) {
              users.push_back(user());
          }
          return ss::make_ready_future<ss::json::json_return_type>(
            std::move(users));
      });
}

void admin_server::register_status_routes() {
    ss::httpd::status_json::ready.set(
      _server._routes, [this](std::unique_ptr<ss::httpd::request>) {
          std::unordered_map<ss::sstring, ss::sstring> status_map{
            {"status", _ready ? "ready" : "booting"}};
          return ss::make_ready_future<ss::json::json_return_type>(status_map);
      });
}

} // namespace admin
