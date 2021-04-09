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
#include "cluster/fwd.h"
#include "cluster/partition_manager.h"
#include "cluster/security_frontend.h"
#include "cluster/shard_table.h"
#include "cluster/topics_frontend.h"
#include "config/configuration.h"
#include "config/endpoint_tls_config.h"
#include "raft/types.h"
#include "redpanda/admin/api-doc/config.json.h"
#include "redpanda/admin/api-doc/kafka.json.h"
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
#include <seastar/http/exception.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/json_path.hh>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <unordered_map>

using namespace std::chrono_literals;

static ss::logger logger{"admin_api_server"};

admin_server::admin_server(
  admin_server_cfg cfg,
  ss::sharded<cluster::partition_manager>& pm,
  cluster::controller* controller,
  ss::sharded<cluster::shard_table>& st)
  : _server("admin")
  , _cfg(std::move(cfg))
  , _partition_manager(pm)
  , _controller(controller)
  , _shard_table(st) {}

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
    ss::httpd::config_json::get_config.set(
      _server._routes, []([[maybe_unused]] ss::const_req req) {
          rapidjson::StringBuffer buf;
          rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
          config::shard_local_cfg().to_json(writer);
          return ss::json::json_return_type(buf.GetString());
      });
    register_raft_routes();
    register_kafka_routes();
    register_security_routes();
    register_status_routes();
}

void admin_server::configure_dashboard() {
    if (_cfg.dashboard_dir) {
        _dashboard_handler = std::make_unique<dashboard_handler>(
          *_cfg.dashboard_dir);
        _server._routes.add(
          ss::httpd::operation_type::GET,
          ss::httpd::url("/dashboard").remainder("path"),
          _dashboard_handler.get());
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
            shard, [group_id, target](cluster::partition_manager& pm) mutable {
                auto consensus = pm.consensus_for(group_id);
                if (!consensus) {
                    throw ss::httpd::not_found_exception();
                }
                return consensus->transfer_leadership(target).then(
                  [](std::error_code err) {
                      if (err) {
                          throw ss::httpd::server_error_exception(fmt::format(
                            "Leadership transfer failed: {}", err.message()));
                      }
                      return ss::json::json_return_type(ss::json::json_void());
                  });
            });
      });
}

/*
 * Parse integer pairs from: ?target={\d,\d}* where each pair represent a
 * node-id and a shard-id, repsectively.
 */
static std::vector<model::broker_shard>
parse_target_broker_shards(const ss::sstring& param) {
    std::vector<ss::sstring> parts;
    boost::split(parts, param, boost::is_any_of(","));

    if (parts.size() % 2 != 0) {
        throw ss::httpd::bad_param_exception(
          fmt::format("Invalid target parameter format: {}", param));
    }

    std::vector<model::broker_shard> replicas;

    for (auto i = 0u; i < parts.size(); i += 2) {
        auto node = std::stoi(parts[i]);
        auto shard = std::stoi(parts[i + 1]);

        if (node < 0 || shard < 0) {
            throw ss::httpd::bad_param_exception(
              fmt::format("Invalid target {}:{}", node, shard));
        }

        replicas.push_back(model::broker_shard{
          .node_id = model::node_id(node),
          .shard = static_cast<uint32_t>(shard),
        });
    }

    return replicas;
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
      _server._routes, [this](std::unique_ptr<ss::httpd::request> req) {
          rapidjson::Document doc;
          doc.Parse(req->content.data());

          auto credential = parse_scram_credential(doc);

          if (!doc.HasMember("username") || !doc["username"].IsString()) {
              throw ss::httpd::bad_request_exception(
                fmt::format("String username missing"));
          }

          auto username = security::credential_user(
            doc["username"].GetString());

          return _controller->get_security_frontend()
            .local()
            .create_user(username, credential, model::timeout_clock::now() + 5s)
            .then([](std::error_code err) {
                vlog(logger.debug, "Creating user {}:{}", err, err.message());
                if (err) {
                    throw ss::httpd::bad_request_exception(
                      fmt::format("Creating user: {}", err.message()));
                }
                return ss::make_ready_future<ss::json::json_return_type>(
                  ss::json::json_return_type(ss::json::json_void()));
            });
      });

    ss::httpd::security_json::delete_user.set(
      _server._routes, [this](std::unique_ptr<ss::httpd::request> req) {
          auto user = security::credential_user(
            model::topic(req->param["user"]));

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
          auto user = security::credential_user(
            model::topic(req->param["user"]));

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
    ss::httpd::kafka_json::kafka_transfer_leadership.set(
      _server._routes, [this](std::unique_ptr<ss::httpd::request> req) {
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
            "Leadership transfer request for leader of topic-partition {}:{} "
            "to node {}",
            topic,
            partition,
            target);

          model::ntp ntp(model::kafka_namespace, topic, partition);

          auto shard = _shard_table.local().shard_for(ntp);
          if (!shard) {
              throw ss::httpd::not_found_exception(fmt::format(
                "Topic partition {}:{} not found", topic, partition));
          }

          return _partition_manager.invoke_on(
            *shard,
            [ntp = std::move(ntp),
             target](cluster::partition_manager& pm) mutable {
                auto partition = pm.get(ntp);
                if (!partition) {
                    throw ss::httpd::not_found_exception();
                }
                return partition->transfer_leadership(target).then(
                  [](std::error_code err) {
                      if (err) {
                          throw ss::httpd::server_error_exception(fmt::format(
                            "Leadership transfer failed: {}", err.message()));
                      }
                      return ss::json::json_return_type(ss::json::json_void());
                  });
            });
      });

    ss::httpd::partition_json::kafka_move_partition.set(
      _server._routes, [this](std::unique_ptr<ss::httpd::request> req) {
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

          std::optional<std::vector<model::broker_shard>> replicas;
          if (auto node = req->get_query_param("target"); !node.empty()) {
              try {
                  replicas = parse_target_broker_shards(node);
              } catch (...) {
                  throw ss::httpd::bad_param_exception(fmt::format(
                    "Invalid target format {}: {}",
                    node,
                    std::current_exception()));
              }
          }

          // this can be removed when we have more sophisticated machinary in
          // redpanda itself for automatically selecting target node/shard.
          if (!replicas || replicas->empty()) {
              throw ss::httpd::bad_request_exception(
                "Partition movement requires target replica set");
          }

          model::ntp ntp(model::kafka_namespace, topic, partition);

          vlog(
            logger.debug,
            "Request to change ntp {} replica set to {}",
            ntp,
            replicas);

          return _controller->get_topics_frontend()
            .local()
            .move_partition_replicas(
              ntp, *replicas, model::timeout_clock::now() + 5s)
            .then([ntp, replicas](std::error_code err) {
                vlog(
                  logger.debug,
                  "Result changing ntp {} replica set to {}: {}:{}",
                  ntp,
                  replicas,
                  err,
                  err.message());
                if (err) {
                    throw ss::httpd::bad_request_exception(
                      fmt::format("Error moving partition: {}", err.message()));
                }
                return ss::make_ready_future<ss::json::json_return_type>(
                  ss::json::json_return_type(ss::json::json_void()));
            });
      });
}

void admin_server::register_status_routes() {
    ss::httpd::status_json::ready.set(
      _server._routes, [](std::unique_ptr<ss::httpd::request>) {
          const static std::unordered_map<ss::sstring, ss::sstring> status_map{
            {"status", "ready"}};
          return ss::make_ready_future<ss::json::json_return_type>(status_map);
      });
}
