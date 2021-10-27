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

#include "redpanda/admin_server.h"

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
#include "finjector/hbadger.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "raft/types.h"
#include "redpanda/admin/api-doc/broker.json.h"
#include "redpanda/admin/api-doc/config.json.h"
#include "redpanda/admin/api-doc/hbadger.json.h"
#include "redpanda/admin/api-doc/partition.json.h"
#include "redpanda/admin/api-doc/raft.json.h"
#include "redpanda/admin/api-doc/security.json.h"
#include "redpanda/admin/api-doc/status.json.h"
#include "rpc/dns.h"
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

#include <stdexcept>
#include <system_error>
#include <unordered_map>

using namespace std::chrono_literals;

static ss::logger logger{"admin_api_server"};

admin_server::admin_server(
  admin_server_cfg cfg,
  ss::sharded<cluster::partition_manager>& pm,
  cluster::controller* controller,
  ss::sharded<cluster::shard_table>& st,
  ss::sharded<cluster::metadata_cache>& metadata_cache)
  : _log_level_timer([this] { log_level_timer_handler(); })
  , _server("admin")
  , _cfg(std::move(cfg))
  , _partition_manager(pm)
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
    register_raft_routes();
    register_kafka_routes();
    register_security_routes();
    register_status_routes();
    register_broker_routes();
    register_partition_routes();
    register_hbadger_routes();
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
        auto resolved = co_await rpc::resolve_dns(ep.address);
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

ss::future<ss::httpd::redirect_exception>
admin_server::redirect_to_leader(ss::httpd::request& req) const {
    auto leader_id_opt = _metadata_cache.local().get_leader_id(
      model::controller_ntp);

    if (!leader_id_opt.has_value()) {
        throw ss::httpd::base_exception(
          fmt::format("Controller leader not available"),
          ss::httpd::reply::status_type::service_unavailable);
    }
    auto leader_opt = _metadata_cache.local().get_broker(leader_id_opt.value());

    if (!leader_opt.has_value()) {
        throw ss::httpd::base_exception(
          fmt::format("Controller leader metadata not available"),
          ss::httpd::reply::status_type::service_unavailable);
    }

    // FIXME: We assume that our peers are listening on the same admin port as
    // we are.
    auto port = config::node_config().admin()[0].address.port();

    // FIXME: We assume that our peer is listening on the same network interface
    // as they use for RPCs.
    auto url = fmt::format(
      "{}://{}:{}{}",
      req.get_protocol_name(),
      leader_opt.value()->rpc_address().host(),
      port,
      req._url);

    vlog(logger.info, "Redirecting admin API call to leader at {}", url);

    co_return ss::httpd::redirect_exception(
      url, ss::httpd::reply::status_type::temporary_redirect);
}

/**
 * Throw an appropriate seastar HTTP exception if we saw
 * a redpanda error during a request.
 *
 * @param ec error code, may be from any subsystem
 * @param id optional node ID, for operations that acted on a particular
 *           node and would like it referenced in per-node cluster errors
 */
ss::future<> admin_server::throw_on_error(
  ss::httpd::request& req, std::error_code ec, model::node_id id) const {
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
            throw ss::httpd::base_exception(
              fmt::format("Not ready ({})", ec.message()),
              ss::httpd::reply::status_type::service_unavailable);
        case cluster::errc::not_leader:
        case cluster::errc::not_leader_controller:
            throw co_await redirect_to_leader(req);
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
        case raft::errc::not_leader:
            throw co_await redirect_to_leader(req);
        default:
            throw ss::httpd::server_error_exception(
              fmt::format("Unexpected raft error: {}", ec.message()));
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
                return consensus->do_transfer_leadership(target).then(
                  [this, req = std::move(req)](std::error_code err)
                    -> ss::future<ss::json::json_return_type> {
                      co_await throw_on_error(*req, err);
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
          rapidjson::Document doc;
          doc.Parse(req->content.data());

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
          co_await throw_on_error(*req, err);
          co_return ss::json::json_return_type(ss::json::json_void());
      });

    ss::httpd::security_json::delete_user.set(
      _server._routes, [this](std::unique_ptr<ss::httpd::request> req) {
          auto user = security::credential_user(req->param["user"]);

          return _controller->get_security_frontend()
            .local()
            .delete_user(user, model::timeout_clock::now() + 5s)
            .then([](std::error_code err) {
                vlog(logger.debug, "Deleting user {}:{}", err, err.message());
                if (err) {
                    throw ss::httpd::bad_request_exception(
                      fmt::format("Deleting user: {}", err.message()));
                }
                return ss::make_ready_future<ss::json::json_return_type>(
                  ss::json::json_return_type(ss::json::json_void()));
            });
      });

    ss::httpd::security_json::update_user.set(
      _server._routes, [this](std::unique_ptr<ss::httpd::request> req) {
          auto user = security::credential_user(req->param["user"]);

          rapidjson::Document doc;
          doc.Parse(req->content.data());

          auto credential = parse_scram_credential(doc);

          return _controller->get_security_frontend()
            .local()
            .update_user(user, credential, model::timeout_clock::now() + 5s)
            .then([](std::error_code err) {
                vlog(logger.debug, "Updating user {}:{}", err, err.message());
                if (err) {
                    throw ss::httpd::bad_request_exception(
                      fmt::format("Updating user: {}", err.message()));
                }
                return ss::make_ready_future<ss::json::json_return_type>(
                  ss::json::json_return_type(ss::json::json_void()));
            });
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

void admin_server::register_kafka_routes() {
    ss::httpd::partition_json::kafka_transfer_leadership.set(
      _server._routes, [this](std::unique_ptr<ss::httpd::request> req) {
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
              throw ss::httpd::not_found_exception(fmt::format(
                "Topic partition {}:{} not found", topic, partition));
          }

          return _partition_manager.invoke_on(
            *shard,
            [ntp = std::move(ntp), target, this, req = std::move(req)](
              cluster::partition_manager& pm) mutable {
                auto partition = pm.get(ntp);
                if (!partition) {
                    throw ss::httpd::not_found_exception();
                }
                return partition->transfer_leadership(target).then(
                  [this, req = std::move(req)](std::error_code err)
                    -> ss::future<ss::json::json_return_type> {
                      co_await throw_on_error(*req, err);
                      co_return ss::json::json_return_type(
                        ss::json::json_void());
                  });
            });
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
    ss::httpd::broker_json::get_brokers.set(
      _server._routes, [this](std::unique_ptr<ss::httpd::request>) {
          std::vector<ss::httpd::broker_json::broker> res;
          for (const auto& broker : _metadata_cache.local().all_brokers()) {
              auto& b = res.emplace_back();
              b.node_id = broker->id();
              b.num_cores = broker->properties().cores;
              b.membership_status = fmt::format(
                "{}", broker->get_membership_state());
          }
          return ss::make_ready_future<ss::json::json_return_type>(
            std::move(res));
      });

    ss::httpd::broker_json::get_broker.set(
      _server._routes, [this](std::unique_ptr<ss::httpd::request> req) {
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

    ss::httpd::broker_json::decommission.set(
      _server._routes,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          model::node_id id = parse_broker_id(*req);

          auto ec = co_await _controller->get_members_frontend()
                      .local()
                      .decommission_node(id);

          co_await throw_on_error(*req, ec, id);
          co_return ss::json::json_void();
      });
    ss::httpd::broker_json::recommission.set(
      _server._routes,
      [this](std::unique_ptr<ss::httpd::request> req)
        -> ss::future<ss::json::json_return_type> {
          model::node_id id = parse_broker_id(*req);

          auto ec = co_await _controller->get_members_frontend()
                      .local()
                      .recommission_node(id);
          co_await throw_on_error(*req, ec, id);
          co_return ss::json::json_void();
      });
}

struct json_validator {
    explicit json_validator(const std::string& schema_text)
      : schema(make_schema_document(schema_text))
      , validator(schema) {}

    static rapidjson::SchemaDocument
    make_schema_document(const std::string& schema) {
        rapidjson::Document doc;
        if (doc.Parse(schema).HasParseError()) {
            throw std::runtime_error(
              fmt::format("Invalid schema document: {}", schema));
        }
        return rapidjson::SchemaDocument(doc);
    }

    const rapidjson::SchemaDocument schema;
    rapidjson::SchemaValidator validator;
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

void admin_server::register_partition_routes() {
    /*
     * Get a list of partition summaries.
     */
    ss::httpd::partition_json::get_partitions.set(
      _server._routes, [this](std::unique_ptr<ss::httpd::request>) {
          using summary = ss::httpd::partition_json::partition_summary;
          return _partition_manager
            .map_reduce0(
              [](cluster::partition_manager& pm) {
                  std::vector<summary> partitions;
                  partitions.reserve(pm.partitions().size());
                  for (const auto& it : pm.partitions()) {
                      summary p;
                      p.ns = it.first.ns;
                      p.topic = it.first.tp.topic;
                      p.partition_id = it.first.tp.partition;
                      p.core = ss::this_shard_id();
                      partitions.push_back(std::move(p));
                  }
                  return partitions;
              },
              std::vector<summary>{},
              [](std::vector<summary> acc, std::vector<summary> update) {
                  acc.insert(acc.end(), update.begin(), update.end());
                  return acc;
              })
            .then([](std::vector<summary> partitions) {
                return ss::make_ready_future<ss::json::json_return_type>(
                  std::move(partitions));
            });
      });

    /*
     * Get detailed information about a partition.
     */
    ss::httpd::partition_json::get_partition.set(
      _server._routes, [this](std::unique_ptr<ss::httpd::request> req) {
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

    // make sure to call reset() before each use
    static thread_local json_validator set_replicas_validator(
      make_set_replicas_validator());

    ss::httpd::partition_json::set_partition_replicas.set(
      _server._routes,
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

          if (ns != model::kafka_namespace) {
              throw ss::httpd::bad_request_exception(
                fmt::format("Unsupported namespace: {}", ns));
          }

          rapidjson::Document doc;
          if (doc.Parse(req->content.data()).HasParseError()) {
              throw ss::httpd::bad_request_exception(
                "Could not replica set json");
          }

          set_replicas_validator.validator.Reset();
          if (!doc.Accept(set_replicas_validator.validator)) {
              throw ss::httpd::bad_request_exception(
                "Replica set json is invalid");
          }

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
                    "Replica set refers to non-existent node/shard (node {} "
                    "shard {})",
                    node_id,
                    shard));
              }

              replicas.push_back(
                model::broker_shard{.node_id = node_id, .shard = shard});
          }

          const model::ntp ntp(std::move(ns), std::move(topic), partition);

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

          if (err) {
              vlog(
                logger.error,
                "Error changing ntp {} replicas: {}:{}",
                ntp,
                err,
                err.message());
              throw ss::httpd::bad_request_exception(
                fmt::format("Error moving partition: {}", err.message()));
          }

          co_return ss::json::json_void();
      });
}

void admin_server::register_hbadger_routes() {
    /**
     * we always register `v1/failure-probes` route. It will ALWAYS return empty
     * list of probes in production mode, and flag indicating that honey badger
     * is disabled
     */

    if constexpr (!finjector::honey_badger::is_enabled()) {
        ss::httpd::hbadger_json::get_failure_probes.set(
          _server._routes, [](std::unique_ptr<ss::httpd::request>) {
              ss::httpd::hbadger_json::failure_injector_status status;
              status.enabled = false;
              return ss::make_ready_future<ss::json::json_return_type>(
                std::move(status));
          });
        return;
    }
    ss::httpd::hbadger_json::get_failure_probes.set(
      _server._routes, [](std::unique_ptr<ss::httpd::request>) {
          auto modules = finjector::shard_local_badger().points();
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

    ss::httpd::hbadger_json::set_failure_probe.set(
      _server._routes, [](std::unique_ptr<ss::httpd::request> req) {
          auto m = req->param["module"];
          auto p = req->param["point"];
          auto type = req->param["type"];
          vlog(
            logger.info,
            "Request to set failure probe of type '{}' in  '{}' at point '{}'",
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
    ss::httpd::hbadger_json::delete_failure_probe.set(
      _server._routes, [](std::unique_ptr<ss::httpd::request> req) {
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
