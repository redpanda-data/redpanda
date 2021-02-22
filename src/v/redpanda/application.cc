// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "redpanda/application.h"

#include "cluster/cluster_utils.h"
#include "cluster/id_allocator.h"
#include "cluster/id_allocator_frontend.h"
#include "cluster/metadata_dissemination_handler.h"
#include "cluster/metadata_dissemination_service.h"
#include "cluster/partition_manager.h"
#include "cluster/service.h"
#include "config/configuration.h"
#include "config/seed_server.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "kafka/server/group_manager.h"
#include "kafka/server/group_router.h"
#include "kafka/server/protocol.h"
#include "kafka/server/quota_manager.h"
#include "model/metadata.h"
#include "platform/stop_signal.h"
#include "raft/service.h"
#include "redpanda/admin/api-doc/config.json.h"
#include "redpanda/admin/api-doc/kafka.json.h"
#include "redpanda/admin/api-doc/raft.json.h"
#include "rpc/simple_protocol.h"
#include "storage/chunk_cache.h"
#include "storage/directories.h"
#include "syschecks/syschecks.h"
#include "test_utils/logs.h"
#include "utils/file_io.h"
#include "version.h"
#include "vlog.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/prometheus.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/api_docs.hh>
#include <seastar/http/exception.hh>
#include <seastar/http/file_handler.hh>
#include <seastar/json/json_elements.hh>
#include <seastar/util/defer.hh>

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <sys/utsname.h>

#include <chrono>
#include <exception>
#include <vector>

application::application(ss::sstring logger_name)
  : _log(std::move(logger_name)){

  };

int application::run(int ac, char** av) {
    init_env();
    vlog(_log.info, "Redpanda {}", redpanda_version());
    struct ::utsname buf;
    ::uname(&buf);
    vlog(
      _log.info,
      "kernel={}, nodename={}, machine={}",
      buf.release,
      buf.nodename,
      buf.machine);
    ss::app_template app = setup_app_template();
    return app.run(ac, av, [this, &app] {
        auto& cfg = app.configuration();
        validate_arguments(cfg);
        return ss::async([this, &cfg] {
            try {
                ::stop_signal app_signal;
                auto deferred = ss::defer([this] {
                    auto deferred = std::move(_deferred);
                    // stop services in reverse order
                    while (!deferred.empty()) {
                        deferred.pop_back();
                    }
                });
                // must initialize configuration before services
                hydrate_config(cfg);
                initialize();
                check_environment();
                setup_metrics();
                configure_admin_server();
                wire_up_services();
                start();
                app_signal.wait().get();
                vlog(_log.info, "Stopping...");
            } catch (...) {
                vlog(
                  _log.info,
                  "Failure during startup: {}",
                  std::current_exception());
                return 1;
            }
            return 0;
        });
    });
}

void application::initialize(std::optional<scheduling_groups> groups) {
    if (config::shard_local_cfg().enable_pid_file()) {
        syschecks::pidfile_create(config::shard_local_cfg().pidfile_path());
    }

    smp_service_groups.create_groups().get();
    _deferred.emplace_back(
      [this] { smp_service_groups.destroy_groups().get(); });

    if (groups) {
        _scheduling_groups = *groups;
        return;
    }

    _scheduling_groups.create_groups().get();
    _deferred.emplace_back(
      [this] { _scheduling_groups.destroy_groups().get(); });
}

void application::setup_metrics() {
    if (!config::shard_local_cfg().disable_metrics()) {
        _metrics.add_group(
          "application",
          {ss::metrics::make_gauge(
            "uptime",
            [] {
                return std::chrono::duration_cast<std::chrono::milliseconds>(
                         ss::engine().uptime())
                  .count();
            },
            ss::metrics::description("Redpanda uptime in milliseconds"))});
    }
}

void application::validate_arguments(const po::variables_map& cfg) {
    if (!cfg.count("redpanda-cfg")) {
        throw std::invalid_argument("Missing redpanda-cfg flag");
    }
}

void application::init_env() { std::setvbuf(stdout, nullptr, _IOLBF, 1024); }

ss::app_template application::setup_app_template() {
    ss::app_template::config app_cfg;
    app_cfg.name = "Redpanda";
    using namespace std::literals::chrono_literals; // NOLINT
    app_cfg.default_task_quota = 500us;
    app_cfg.auto_handle_sigint_sigterm = false;
    auto app = ss::app_template(app_cfg);
    app.add_options()(
      "redpanda-cfg",
      po::value<std::string>(),
      ".yaml file config for redpanda");
    return app;
}

void application::hydrate_config(const po::variables_map& cfg) {
    std::filesystem::path cfg_path(cfg["redpanda-cfg"].as<std::string>());
    auto buf = read_fully(cfg_path).get0();
    // see https://github.com/jbeder/yaml-cpp/issues/765
    auto workaround = ss::uninitialized_string(buf.size_bytes());
    auto in = iobuf::iterator_consumer(buf.cbegin(), buf.cend());
    in.consume_to(buf.size_bytes(), workaround.begin());
    const YAML::Node config = YAML::Load(workaround);
    vlog(_log.info, "Configuration:\n\n{}\n\n", config);
    ss::smp::invoke_on_all([&config] {
        config::shard_local_cfg().read_yaml(config);
    }).get0();
    vlog(
      _log.info,
      "Use `rpk config set redpanda.<cfg> <value>` to change values "
      "below:");
    config::shard_local_cfg().for_each(
      [this](const config::base_property& item) {
          std::stringstream val;
          item.print(val);
          vlog(_log.debug, "{}\t- {}", val.str(), item.desc());
      });
}

void application::check_environment() {
    syschecks::systemd_message("checking environment (CPU, Mem)").get();
    syschecks::cpu();
    syschecks::memory(config::shard_local_cfg().developer_mode());
    storage::directories::initialize(
      config::shard_local_cfg().data_directory().as_sstring())
      .get();
}

/**
 * Prepend a / to the path component. This handles the case where path is an
 * empty string (e.g. url/) or when the path omits the root file path directory
 * (e.g. url/index.html vs url//index.html). The directory handler in seastar is
 * opininated and not very forgiving here so we help it a bit.
 */
class dashboard_handler final : public ss::httpd::directory_handler {
public:
    dashboard_handler()
      : directory_handler(*config::shard_local_cfg().dashboard_dir()) {}

    ss::future<std::unique_ptr<ss::httpd::reply>> handle(
      const ss::sstring& path,
      std::unique_ptr<ss::httpd::request> req,
      std::unique_ptr<ss::httpd::reply> rep) override {
        req->param.set("path", "/" + req->param.at("path"));
        return directory_handler::handle(path, std::move(req), std::move(rep));
    }
};

void application::configure_admin_server() {
    auto& conf = config::shard_local_cfg();
    if (!conf.enable_admin_api()) {
        return;
    }
    syschecks::systemd_message("constructing http server").get();
    construct_service(_admin, ss::sstring("admin")).get();
    // configure admin API TLS
    if (conf.admin_api_tls().is_enabled()) {
        _admin
          .invoke_on_all([this](ss::http_server& server) {
              return config::shard_local_cfg()
                .admin_api_tls()
                .get_credentials_builder()
                .then([this, &server](
                        std::optional<ss::tls::credentials_builder> builder) {
                    if (!builder) {
                        return ss::now();
                    }

                    return builder
                      ->build_reloadable_server_credentials(
                        [this](
                          const std::unordered_set<ss::sstring>& updated,
                          const std::exception_ptr& eptr) {
                            cluster::log_certificate_reload_event(
                              _log, "API TLS", updated, eptr);
                        })
                      .then([&server](auto cred) {
                          server.set_tls_credentials(std::move(cred));
                      });
                });
          })
          .get0();
    }
    if (conf.dashboard_dir()) {
        _admin
          .invoke_on_all([](ss::http_server& server) {
              server._routes.add(
                ss::httpd::operation_type::GET,
                ss::httpd::url("/dashboard").remainder("path"),
                new dashboard_handler());
          })
          .get0();
    }
    ss::prometheus::config metrics_conf;
    metrics_conf.metric_help = "redpanda metrics";
    metrics_conf.prefix = "vectorized";
    ss::prometheus::add_prometheus_routes(_admin, metrics_conf).get();
    if (conf.enable_admin_api()) {
        syschecks::systemd_message(
          "enabling admin HTTP api: {}", config::shard_local_cfg().admin())
          .get();
        auto rb = ss::make_shared<ss::api_registry_builder20>(
          conf.admin_api_doc_dir(), "/v1");
        _admin
          .invoke_on_all([this, rb](ss::http_server& server) {
              auto insert_comma = [](ss::output_stream<char>& os) {
                  return os.write(",\n");
              };
              rb->set_api_doc(server._routes);
              rb->register_api_file(server._routes, "header");
              rb->register_api_file(server._routes, "config");
              rb->register_function(server._routes, insert_comma);
              rb->register_api_file(server._routes, "raft");
              rb->register_function(server._routes, insert_comma);
              rb->register_api_file(server._routes, "kafka");
              ss::httpd::config_json::get_config.set(
                server._routes, []([[maybe_unused]] ss::const_req req) {
                    rapidjson::StringBuffer buf;
                    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
                    config::shard_local_cfg().to_json(writer);
                    return ss::json::json_return_type(buf.GetString());
                });
              admin_register_raft_routes(server);
              admin_register_kafka_routes(server);
          })
          .get();
    }

    with_scheduling_group(_scheduling_groups.admin_sg(), [this] {
        return rpc::resolve_dns(config::shard_local_cfg().admin())
          .then([this](ss::socket_address addr) mutable {
              return _admin
                .invoke_on_all<ss::future<> (ss::http_server::*)(
                  ss::socket_address)>(&ss::http_server::listen, addr)
                .handle_exception([this](auto ep) {
                    _log.error("Exception on http admin server: {}", ep);
                    return ss::make_exception_future<>(ep);
                });
          });
    }).get();

    vlog(_log.info, "Started HTTP admin service listening at {}", conf.admin());
}

static storage::kvstore_config kvstore_config_from_global_config() {
    /*
     * The key-value store is rooted at the configured data directory, and
     * the internal kvstore topic-namespace results in a storage layout of:
     *
     *    /var/lib/redpanda/data/
     *       - redpanda/kvstore/
     *           - 0
     *           - 1
     *           - ... #cores
     */
    return storage::kvstore_config(
      config::shard_local_cfg().kvstore_max_segment_size(),
      config::shard_local_cfg().kvstore_flush_interval(),
      config::shard_local_cfg().data_directory().as_sstring(),
      storage::debug_sanitize_files::no);
}

static storage::log_config manager_config_from_global_config() {
    return storage::log_config(
      storage::log_config::storage_type::disk,
      config::shard_local_cfg().data_directory().as_sstring(),
      config::shard_local_cfg().log_segment_size(),
      config::shard_local_cfg().compacted_log_segment_size(),
      config::shard_local_cfg().max_compacted_log_segment_size(),
      storage::debug_sanitize_files::no,
      config::shard_local_cfg().retention_bytes(),
      config::shard_local_cfg().log_compaction_interval_ms(),
      config::shard_local_cfg().delete_retention_ms(),
      storage::with_cache(!config::shard_local_cfg().disable_batch_cache()),
      storage::batch_cache::reclaim_options{
        .growth_window = config::shard_local_cfg().reclaim_growth_window(),
        .stable_window = config::shard_local_cfg().reclaim_stable_window(),
        .min_size = config::shard_local_cfg().reclaim_min_size(),
        .max_size = config::shard_local_cfg().reclaim_max_size(),
      });
}

// add additional services in here
void application::wire_up_services() {
    ss::smp::invoke_on_all([] {
        return storage::internal::chunks().start();
    }).get();

    // cluster
    syschecks::systemd_message("Adding raft client cache").get();
    construct_service(_raft_connection_cache).get();
    syschecks::systemd_message("Building shard-lookup tables").get();
    construct_service(shard_table).get();

    syschecks::systemd_message("Intializing storage services").get();
    construct_service(
      storage,
      kvstore_config_from_global_config(),
      manager_config_from_global_config())
      .get();

    if (coproc_enabled()) {
        auto coproc_supervisor_server_addr
          = rpc::resolve_dns(
              config::shard_local_cfg().coproc_supervisor_server())
              .get0();
        syschecks::systemd_message("Building coproc pacemaker").get();
        construct_service(
          pacemaker, coproc_supervisor_server_addr, std::ref(storage))
          .get();
    }

    syschecks::systemd_message("Intializing raft group manager").get();
    construct_service(
      raft_group_manager,
      model::node_id(config::shard_local_cfg().node_id()),
      config::shard_local_cfg().raft_io_timeout_ms(),
      config::shard_local_cfg().raft_heartbeat_interval_ms(),
      std::ref(_raft_connection_cache),
      std::ref(storage))
      .get();

    syschecks::systemd_message("Adding partition manager").get();
    construct_service(
      partition_manager, std::ref(storage), std::ref(raft_group_manager))
      .get();
    vlog(_log.info, "Partition manager started");

    // controller

    syschecks::systemd_message("Creating cluster::controller").get();

    construct_single_service(
      controller,
      _raft_connection_cache,
      partition_manager,
      shard_table,
      storage);

    controller->wire_up().get0();
    syschecks::systemd_message("Creating kafka metadata cache").get();
    construct_service(
      metadata_cache,
      std::ref(controller->get_topics_state()),
      std::ref(controller->get_members_table()),
      std::ref(controller->get_partition_leaders()))
      .get();

    syschecks::systemd_message("Creating metadata dissemination service").get();
    construct_service(
      md_dissemination_service,
      std::ref(raft_group_manager),
      std::ref(partition_manager),
      std::ref(controller->get_partition_leaders()),
      std::ref(controller->get_members_table()),
      std::ref(controller->get_topics_state()),
      std::ref(_raft_connection_cache))
      .get();

    // group membership
    syschecks::systemd_message("Creating partition manager").get();
    construct_service(
      _group_manager,
      std::ref(raft_group_manager),
      std::ref(partition_manager),
      std::ref(controller->get_topics_state()),
      std::ref(config::shard_local_cfg()))
      .get();
    syschecks::systemd_message("Creating kafka group shard mapper").get();
    construct_service(coordinator_ntp_mapper, std::ref(metadata_cache)).get();
    syschecks::systemd_message("Creating kafka group router").get();
    construct_service(
      group_router,
      _scheduling_groups.kafka_sg(),
      smp_service_groups.kafka_smp_sg(),
      std::ref(_group_manager),
      std::ref(shard_table),
      std::ref(coordinator_ntp_mapper))
      .get();

    // metrics and quota management
    syschecks::systemd_message("Adding kafka quota manager").get();
    construct_service(quota_mgr).get();
    // rpc
    rpc::server_configuration rpc_cfg("internal_rpc");
    /**
     * Use port based load_balancing_algorithm to make connection shard
     * assignment deterministic.
     **/
    rpc_cfg.load_balancing_algo
      = ss::server_socket::load_balancing_algorithm::port;
    rpc_cfg.max_service_memory_per_core = memory_groups::rpc_total_memory();
    rpc_cfg.disable_metrics = rpc::metrics_disabled(
      config::shard_local_cfg().disable_metrics());
    auto rpc_server_addr
      = rpc::resolve_dns(config::shard_local_cfg().rpc_server()).get0();
    rpc_cfg.addrs.emplace_back(rpc_server_addr);
    auto rpc_builder = config::shard_local_cfg()
                         .rpc_server_tls()
                         .get_credentials_builder()
                         .get0();
    rpc_cfg.credentials
      = rpc_builder ? rpc_builder
                        ->build_reloadable_server_credentials(
                          [this](
                            const std::unordered_set<ss::sstring>& updated,
                            const std::exception_ptr& eptr) {
                              cluster::log_certificate_reload_event(
                                _log, "Internal RPC TLS", updated, eptr);
                          })
                        .get0()
                    : nullptr;
    syschecks::systemd_message("Starting internal RPC {}", rpc_cfg).get();
    construct_service(_rpc, rpc_cfg).get();
    // coproc rpc
    if (coproc_enabled()) {
        auto coproc_script_manager_server_addr
          = rpc::resolve_dns(
              config::shard_local_cfg().coproc_script_manager_server())
              .get0();
        rpc::server_configuration cp_rpc_cfg("coproc_rpc");
        cp_rpc_cfg.max_service_memory_per_core
          = memory_groups::rpc_total_memory();
        cp_rpc_cfg.addrs.emplace_back(coproc_script_manager_server_addr);
        cp_rpc_cfg.disable_metrics = rpc::metrics_disabled(
          config::shard_local_cfg().disable_metrics());
        syschecks::systemd_message(
          "Starting coprocessor internal RPC {}", cp_rpc_cfg)
          .get();
        construct_service(_coproc_rpc, cp_rpc_cfg).get();
    }

    syschecks::systemd_message("Creating id allocator frontend").get();
    construct_service(
      id_allocator_frontend,
      smp_service_groups.raft_smp_sg(),
      std::ref(partition_manager),
      std::ref(shard_table),
      std::ref(metadata_cache),
      std::ref(_raft_connection_cache),
      std::ref(controller->get_partition_leaders()),
      std::ref(controller))
      .get();

    rpc::server_configuration kafka_cfg("kafka_rpc");
    kafka_cfg.max_service_memory_per_core = memory_groups::kafka_total_memory();
    for (const auto& ep : config::shard_local_cfg().kafka_api()) {
        kafka_cfg.addrs.emplace_back(
          ep.name, rpc::resolve_dns(ep.address).get0());
    }
    syschecks::systemd_message("Building TLS credentials for kafka").get();
    auto kafka_builder = config::shard_local_cfg()
                           .kafka_api_tls()
                           .get_credentials_builder()
                           .get0();
    kafka_cfg.credentials
      = kafka_builder ? kafka_builder
                          ->build_reloadable_server_credentials(
                            [this](
                              const std::unordered_set<ss::sstring>& updated,
                              const std::exception_ptr& eptr) {
                                cluster::log_certificate_reload_event(
                                  _log, "Kafka RPC TLS", updated, eptr);
                            })
                          .get0()
                      : nullptr;
    kafka_cfg.disable_metrics = rpc::metrics_disabled(
      config::shard_local_cfg().disable_metrics());
    syschecks::systemd_message("Starting kafka RPC {}", kafka_cfg).get();
    construct_service(_kafka_server, kafka_cfg).get();
    construct_service(
      fetch_session_cache,
      config::shard_local_cfg().fetch_session_eviction_timeout_ms())
      .get();
}

void application::start() {
    syschecks::systemd_message("Staring storage services").get();
    storage.invoke_on_all(&storage::api::start).get();

    syschecks::systemd_message("Starting the partition manager").get();
    partition_manager.invoke_on_all(&cluster::partition_manager::start).get();

    syschecks::systemd_message("Starting Raft group manager").get();
    raft_group_manager.invoke_on_all(&raft::group_manager::start).get();

    syschecks::systemd_message("Starting Kafka group manager").get();
    _group_manager.invoke_on_all(&kafka::group_manager::start).get();

    syschecks::systemd_message("Starting controller").get();
    controller->start().get0();
    /**
     * We schedule shutting down controller input and aborting its operation
     * as a first shutdown step. (other services are stopeed in
     * an order reverse to the startup sequence.) This way we terminate all long
     * running opertions before shutting down the RPC server, preventing it to
     * wait on background dispatch gate `close` call.
     *
     * NOTE controller has to be stopped only after it was started
     */
    _deferred.emplace_back([this] { controller->shutdown_input().get(); });
    // FIXME: in first patch explain why this is started after the
    // controller so the broker set will be available. Then next patch fix.
    syschecks::systemd_message("Starting metadata dissination service").get();
    md_dissemination_service
      .invoke_on_all(&cluster::metadata_dissemination_service::start)
      .get();

    syschecks::systemd_message("Starting RPC").get();
    _rpc
      .invoke_on_all([this](rpc::server& s) {
          auto proto = std::make_unique<rpc::simple_protocol>();
          proto->register_service<cluster::id_allocator>(
            _scheduling_groups.raft_sg(),
            smp_service_groups.raft_smp_sg(),
            std::ref(id_allocator_frontend));
          proto->register_service<
            raft::service<cluster::partition_manager, cluster::shard_table>>(
            _scheduling_groups.raft_sg(),
            smp_service_groups.raft_smp_sg(),
            partition_manager,
            shard_table.local());
          proto->register_service<cluster::service>(
            _scheduling_groups.cluster_sg(),
            smp_service_groups.cluster_smp_sg(),
            std::ref(controller->get_topics_frontend()),
            std::ref(controller->get_members_manager()),
            std::ref(metadata_cache));
          proto->register_service<cluster::metadata_dissemination_handler>(
            _scheduling_groups.cluster_sg(),
            smp_service_groups.cluster_smp_sg(),
            std::ref(controller->get_partition_leaders()));
          s.set_protocol(std::move(proto));
      })
      .get();
    auto& conf = config::shard_local_cfg();
    _rpc.invoke_on_all(&rpc::server::start).get();
    vlog(_log.info, "Started RPC server listening at {}", conf.rpc_server());

    if (coproc_enabled()) {
        syschecks::systemd_message("Starting coproc RPC").get();
        _coproc_rpc
          .invoke_on_all([this](rpc::server& s) {
              auto proto = std::make_unique<rpc::simple_protocol>();
              proto->register_service<coproc::service>(
                _scheduling_groups.coproc_sg(),
                smp_service_groups.coproc_smp_sg(),
                std::ref(pacemaker));
              s.set_protocol(std::move(proto));
          })
          .get();
        _coproc_rpc.invoke_on_all(&rpc::server::start).get();
        vlog(
          _log.info,
          "Started coproc RPC server listening at {}",
          conf.coproc_script_manager_server());
    }

    quota_mgr.invoke_on_all(&kafka::quota_manager::start).get();

    // Kafka API
    _kafka_server
      .invoke_on_all([this](rpc::server& s) {
          auto proto = std::make_unique<kafka::protocol>(
            smp_service_groups.kafka_smp_sg(),
            metadata_cache,
            controller->get_topics_frontend(),
            quota_mgr,
            group_router,
            shard_table,
            partition_manager,
            coordinator_ntp_mapper,
            fetch_session_cache,
            std::ref(id_allocator_frontend));
          s.set_protocol(std::move(proto));
      })
      .get();
    _kafka_server.invoke_on_all(&rpc::server::start).get();
    vlog(
      _log.info, "Started Kafka API server listening at {}", conf.kafka_api());

    if (coproc_enabled()) {
        /// Temporarily disable retries for the new client until we create a
        /// more granular way to configure this per client or per request.
        kafka::client::shard_local_cfg().retries.set_value(size_t(0));
        construct_single_service(
          _wasm_event_listener,
          config::shard_local_cfg().data_directory.value().path);
        _wasm_event_listener->start().get();
        /// Start the pacemakers offset keeper
        pacemaker.invoke_on_all(&coproc::pacemaker::start).get();
    }

    vlog(_log.info, "Successfully started Redpanda!");
    syschecks::systemd_notify_ready().get();
}

void application::admin_register_raft_routes(ss::http_server& server) {
    ss::httpd::raft_json::raft_transfer_leadership.set(
      server._routes, [this](std::unique_ptr<ss::httpd::request> req) {
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

          if (!shard_table.local().contains(group_id)) {
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
            _log.info,
            "Leadership transfer request for raft group {} to node {}",
            group_id,
            target);

          auto shard = shard_table.local().shard_for(group_id);

          return partition_manager.invoke_on(
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

void application::admin_register_kafka_routes(ss::http_server& server) {
    ss::httpd::kafka_json::kafka_transfer_leadership.set(
      server._routes, [this](std::unique_ptr<ss::httpd::request> req) {
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
            _log.info,
            "Leadership transfer request for leader of topic-partition {}:{} "
            "to node {}",
            topic,
            partition,
            target);

          model::ntp ntp(model::kafka_namespace, topic, partition);

          auto shard = shard_table.local().shard_for(ntp);
          if (!shard) {
              throw ss::httpd::not_found_exception(fmt::format(
                "Topic partition {}:{} not found", topic, partition));
          }

          return partition_manager.invoke_on(
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
}
