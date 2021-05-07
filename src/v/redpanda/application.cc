// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "redpanda/application.h"

#include "archival/ntp_archiver_service.h"
#include "archival/service.h"
#include "cluster/cluster_utils.h"
#include "cluster/id_allocator.h"
#include "cluster/id_allocator_frontend.h"
#include "cluster/metadata_dissemination_handler.h"
#include "cluster/metadata_dissemination_service.h"
#include "cluster/partition_manager.h"
#include "cluster/security_frontend.h"
#include "cluster/service.h"
#include "cluster/topics_frontend.h"
#include "config/configuration.h"
#include "config/endpoint_tls_config.h"
#include "config/seed_server.h"
#include "kafka/client/configuration.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "kafka/server/group_manager.h"
#include "kafka/server/group_router.h"
#include "kafka/server/protocol.h"
#include "kafka/server/queue_depth_monitor.h"
#include "kafka/server/quota_manager.h"
#include "model/metadata.h"
#include "pandaproxy/configuration.h"
#include "pandaproxy/proxy.h"
#include "platform/stop_signal.h"
#include "raft/service.h"
#include "redpanda/admin_server.h"
#include "resource_mgmt/io_priority.h"
#include "rpc/simple_protocol.h"
#include "storage/chunk_cache.h"
#include "storage/directories.h"
#include "syschecks/syschecks.h"
#include "test_utils/logs.h"
#include "utils/file_io.h"
#include "utils/human.h"
#include "version.h"
#include "vlog.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/prometheus.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>
#include <seastar/json/json_elements.hh>
#include <seastar/net/tls.hh>
#include <seastar/util/conversions.hh>
#include <seastar/util/defer.hh>

#include <sys/utsname.h>

#include <chrono>
#include <exception>
#include <vector>

application::application(ss::sstring logger_name)
  : _log(std::move(logger_name)){

  };

static void log_system_resources(
  ss::logger& log, const boost::program_options::variables_map& cfg) {
    const auto shard_mem = ss::memory::stats();
    auto total_mem = shard_mem.total_memory() * ss::smp::count;
    /**
     * IMPORTANT: copied out of seastar `resources.cc`, if logic in seastar will
     * change we have to change our logic in here.
     */
    const size_t default_reserve_memory = std::max<size_t>(
      1536_MiB, 0.07 * total_mem);
    auto reserve = cfg.contains("reserve-memory") ? ss::parse_memory_size(
                     cfg["reserve-memory"].as<std::string>())
                                                  : default_reserve_memory;
    vlog(
      log.info,
      "System resources: {{ cpus: {}, available memory: {}, reserved memory: "
      "{}}}",
      ss::smp::count,
      human::bytes(total_mem),
      human::bytes(reserve));
}

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
        log_system_resources(_log, cfg);
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
                wire_up_services();
                configure_admin_server();
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

void application::initialize(
  std::optional<YAML::Node> proxy_cfg,
  std::optional<YAML::Node> proxy_client_cfg,
  std::optional<scheduling_groups> groups) {
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

    if (proxy_cfg) {
        _proxy_config.emplace(*proxy_cfg);
    }

    if (proxy_client_cfg) {
        _proxy_client_config.emplace(*proxy_client_cfg);
    }
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
    vlog(
      _log.info,
      "Use `rpk config set <cfg> <value>` to change values "
      "below:");
    auto config_printer = [this](std::string_view service) {
        return [this, service](const config::base_property& item) {
            std::stringstream val;
            item.print(val);
            vlog(_log.info, "{}.{}\t- {}", service, val.str(), item.desc());
        };
    };
    _redpanda_enabled = config["redpanda"];
    if (_redpanda_enabled) {
        ss::smp::invoke_on_all([&config] {
            config::shard_local_cfg().read_yaml(config);
        }).get0();
        config::shard_local_cfg().for_each(config_printer("redpanda"));
    }
    if (config["pandaproxy"]) {
        _proxy_config.emplace(config["pandaproxy"]);
        if (config["pandaproxy_client"]) {
            _proxy_client_config.emplace(config["pandaproxy_client"]);
        } else {
            _proxy_client_config.emplace();
            const auto& kafka_api = config::shard_local_cfg().kafka_api.value();
            vassert(!kafka_api.empty(), "There are no kafka_api listeners");
            _proxy_client_config->brokers.set_value(
              std::vector<unresolved_address>{kafka_api[0].address});
            const auto& kafka_api_tls
              = config::shard_local_cfg().kafka_api_tls.value();
            auto tls_it = std::find_if(
              kafka_api_tls.begin(),
              kafka_api_tls.end(),
              [&kafka_api](const config::endpoint_tls_config& tls) {
                  return tls.name == kafka_api[0].name;
              });
            if (tls_it != kafka_api_tls.end()) {
                _proxy_client_config->broker_tls.set_value(tls_it->config);
            }
        }
        _proxy_config->for_each(config_printer("pandaproxy"));
        _proxy_client_config->for_each(config_printer("pandaproxy_client"));
    }
}

void application::check_environment() {
    syschecks::systemd_message("checking environment (CPU, Mem)").get();
    syschecks::cpu();
    syschecks::memory(config::shard_local_cfg().developer_mode());
    if (_redpanda_enabled) {
        storage::directories::initialize(
          config::shard_local_cfg().data_directory().as_sstring())
          .get();
    }
}

static admin_server_cfg
admin_server_cfg_from_global_cfg(scheduling_groups& sgs) {
    return admin_server_cfg{
      .endpoints = config::shard_local_cfg().admin(),
      .endpoints_tls = config::shard_local_cfg().admin_api_tls(),
      .dashboard_dir = config::shard_local_cfg().dashboard_dir(),
      .admin_api_docs_dir = config::shard_local_cfg().admin_api_doc_dir(),
      .enable_admin_api = config::shard_local_cfg().enable_admin_api(),
      .sg = sgs.admin_sg(),
    };
}

void application::configure_admin_server() {
    auto& conf = config::shard_local_cfg();
    if (!conf.enable_admin_api()) {
        return;
    }
    syschecks::systemd_message("constructing http server").get();
    construct_service(
      _admin,
      admin_server_cfg_from_global_cfg(_scheduling_groups),
      std::ref(partition_manager),
      controller.get(),
      std::ref(shard_table))
      .get();
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
      priority_manager::local().compaction_priority(),
      config::shard_local_cfg().retention_bytes(),
      config::shard_local_cfg().log_compaction_interval_ms(),
      config::shard_local_cfg().delete_retention_ms(),
      storage::with_cache(!config::shard_local_cfg().disable_batch_cache()),
      storage::batch_cache::reclaim_options{
        .growth_window = config::shard_local_cfg().reclaim_growth_window(),
        .stable_window = config::shard_local_cfg().reclaim_stable_window(),
        .min_size = config::shard_local_cfg().reclaim_min_size(),
        .max_size = config::shard_local_cfg().reclaim_max_size(),
      },
      config::shard_local_cfg().readers_cache_eviction_timeout_ms());
}

// add additional services in here
void application::wire_up_services() {
    if (_redpanda_enabled) {
        wire_up_redpanda_services();
    }
    if (_proxy_config) {
        construct_service(_proxy_client, to_yaml(*_proxy_client_config)).get();
        construct_service(
          _proxy,
          to_yaml(*_proxy_config),
          smp_service_groups.proxy_smp_sg(),
          std::reference_wrapper(_proxy_client))
          .get();
    }
}

void application::wire_up_redpanda_services() {
    ss::smp::invoke_on_all([] {
        return storage::internal::chunks().start();
    }).get();

    // cluster
    syschecks::systemd_message("Adding raft client cache").get();
    construct_service(_raft_connection_cache).get();
    syschecks::systemd_message("Building shard-lookup tables").get();
    construct_service(shard_table).get();

    syschecks::systemd_message("Intializing storage services").get();
    auto log_cfg = manager_config_from_global_config();
    log_cfg.reclaim_opts.background_reclaimer_sg
      = _scheduling_groups.cache_background_reclaim_sg();
    construct_service(storage, kvstore_config_from_global_config(), log_cfg)
      .get();

    if (coproc_enabled()) {
        syschecks::systemd_message("Building coproc pacemaker").get();
        construct_service(
          pacemaker,
          config::shard_local_cfg().coproc_supervisor_server(),
          std::ref(storage))
          .get();
    }

    syschecks::systemd_message("Intializing raft group manager").get();
    construct_service(
      raft_group_manager,
      model::node_id(config::shard_local_cfg().node_id()),
      config::shard_local_cfg().raft_io_timeout_ms(),
      config::shard_local_cfg().raft_heartbeat_interval_ms(),
      config::shard_local_cfg().raft_heartbeat_timeout_ms(),
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

    if (archival_storage_enabled()) {
        syschecks::systemd_message("Starting archival scheduler").get();
        ss::sharded<archival::configuration> configs;
        configs.start().get();
        configs
          .invoke_on_all([](archival::configuration& c) {
              return archival::scheduler_service::get_archival_service_config()
                .then(
                  [&c](archival::configuration cfg) { c = std::move(cfg); });
          })
          .get();
        construct_service(
          archival_scheduler,
          std::ref(storage),
          std::ref(partition_manager),
          std::ref(controller->get_topics_state()),
          std::ref(configs))
          .get();
        configs.stop().get();
    }
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
    ss::sharded<rpc::server_configuration> rpc_cfg;
    rpc_cfg.start(ss::sstring("internal_rpc")).get();
    rpc_cfg
      .invoke_on_all([this](rpc::server_configuration& c) {
          return ss::async([this, &c] {
              auto rpc_server_addr = rpc::resolve_dns(
                                       config::shard_local_cfg().rpc_server())
                                       .get0();
              c.load_balancing_algo
                = ss::server_socket::load_balancing_algorithm::port;
              c.max_service_memory_per_core = memory_groups::rpc_total_memory();
              c.disable_metrics = rpc::metrics_disabled(
                config::shard_local_cfg().disable_metrics());
              auto rpc_builder = config::shard_local_cfg()
                                   .rpc_server_tls()
                                   .get_credentials_builder()
                                   .get0();
              auto credentials
                = rpc_builder
                    ? rpc_builder
                        ->build_reloadable_server_credentials(
                          [this](
                            const std::unordered_set<ss::sstring>& updated,
                            const std::exception_ptr& eptr) {
                              cluster::log_certificate_reload_event(
                                _log, "Internal RPC TLS", updated, eptr);
                          })
                        .get0()
                    : nullptr;
              c.addrs.emplace_back(rpc_server_addr, credentials);
          });
      })
      .get();
    /**
     * Use port based load_balancing_algorithm to make connection shard
     * assignment deterministic.
     **/
    syschecks::systemd_message("Starting internal RPC {}", rpc_cfg.local())
      .get();
    construct_service(_rpc, &rpc_cfg).get();
    rpc_cfg.stop().get();

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

    ss::sharded<rpc::server_configuration> kafka_cfg;
    kafka_cfg.start(ss::sstring("kafka_rpc")).get();
    kafka_cfg
      .invoke_on_all([this](rpc::server_configuration& c) {
          return ss::async([this, &c] {
              c.max_service_memory_per_core
                = memory_groups::kafka_total_memory();
              auto& tls_config
                = config::shard_local_cfg().kafka_api_tls.value();
              for (const auto& ep : config::shard_local_cfg().kafka_api()) {
                  ss::shared_ptr<ss::tls::server_credentials> credentails;
                  // find credentials for this endpoint
                  auto it = find_if(
                    tls_config.begin(),
                    tls_config.end(),
                    [&ep](const config::endpoint_tls_config& cfg) {
                        return cfg.name == ep.name;
                    });
                  // if tls is configured for this endpoint build reloadable
                  // credentails
                  if (it != tls_config.end()) {
                      syschecks::systemd_message(
                        "Building TLS credentials for kafka")
                        .get();
                      auto kafka_builder
                        = it->config.get_credentials_builder().get0();
                      credentails
                        = kafka_builder
                            ? kafka_builder
                                ->build_reloadable_server_credentials(
                                  [this, name = it->name](
                                    const std::unordered_set<ss::sstring>&
                                      updated,
                                    const std::exception_ptr& eptr) {
                                      cluster::log_certificate_reload_event(
                                        _log, "Kafka RPC TLS", updated, eptr);
                                  })
                                .get0()
                            : nullptr;
                  }

                  c.addrs.emplace_back(
                    ep.name, rpc::resolve_dns(ep.address).get0(), credentails);
              }

              c.disable_metrics = rpc::metrics_disabled(
                config::shard_local_cfg().disable_metrics());
          });
      })
      .get();
    syschecks::systemd_message("Starting kafka RPC {}", kafka_cfg.local())
      .get();
    construct_service(_kafka_server, &kafka_cfg).get();
    kafka_cfg.stop().get();
    construct_service(
      fetch_session_cache,
      config::shard_local_cfg().fetch_session_eviction_timeout_ms())
      .get();
}

ss::future<> application::set_proxy_config(ss::sstring name, std::any val) {
    return _proxy.invoke_on_all(
      [name{std::move(name)}, val{std::move(val)}](pandaproxy::proxy& p) {
          p.config().get(name).set_value(val);
      });
}

bool application::archival_storage_enabled() {
    const auto& cfg = config::shard_local_cfg();
    return cfg.cloud_storage_enabled();
}

ss::future<>
application::set_proxy_client_config(ss::sstring name, std::any val) {
    return _proxy.invoke_on_all(
      [name{std::move(name)}, val{std::move(val)}](pandaproxy::proxy& p) {
          p.client_config().get(name).set_value(val);
      });
}

void application::start() {
    if (_redpanda_enabled) {
        start_redpanda();
    }

    if (_proxy_config) {
        _proxy.invoke_on_all(&pandaproxy::proxy::start).get();
        vlog(
          _log.info,
          "Started Pandaproxy listening at {}",
          _proxy_config->pandaproxy_api());
    }

    vlog(_log.info, "Successfully started Redpanda!");
    syschecks::systemd_notify_ready().get();
}

void application::start_redpanda() {
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
            shard_table.local(),
            config::shard_local_cfg().raft_heartbeat_interval_ms());
          proto->register_service<cluster::service>(
            _scheduling_groups.cluster_sg(),
            smp_service_groups.cluster_smp_sg(),
            std::ref(controller->get_topics_frontend()),
            std::ref(controller->get_members_manager()),
            std::ref(metadata_cache),
            std::ref(controller->get_security_frontend()),
            std::ref(controller->get_api()));
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

    if (archival_storage_enabled()) {
        syschecks::systemd_message("Starting archival storage").get();
        archival_scheduler
          .invoke_on_all(
            [](archival::scheduler_service& svc) { return svc.start(); })
          .get();
    }

    quota_mgr.invoke_on_all(&kafka::quota_manager::start).get();

    std::optional<kafka::qdc_monitor::config> qdc_config;
    if (config::shard_local_cfg().kafka_qdc_enable()) {
        qdc_config = kafka::qdc_monitor::config{
          .latency_alpha = config::shard_local_cfg().kafka_qdc_latency_alpha(),
          .max_latency = config::shard_local_cfg().kafka_qdc_max_latency_ms(),
          .window_count = config::shard_local_cfg().kafka_qdc_window_count(),
          .window_size = config::shard_local_cfg().kafka_qdc_window_size_ms(),
          .depth_alpha = config::shard_local_cfg().kafka_qdc_depth_alpha(),
          .idle_depth = config::shard_local_cfg().kafka_qdc_idle_depth(),
          .min_depth = config::shard_local_cfg().kafka_qdc_min_depth(),
          .max_depth = config::shard_local_cfg().kafka_qdc_max_depth(),
          .depth_update_freq
          = config::shard_local_cfg().kafka_qdc_depth_update_ms(),
        };
    }

    // Kafka API
    _kafka_server
      .invoke_on_all([this, qdc_config](rpc::server& s) {
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
            std::ref(id_allocator_frontend),
            controller->get_credential_store(),
            controller->get_authorizer(),
            controller->get_security_frontend(),
            qdc_config,
            controller->get_api());

          s.set_protocol(std::move(proto));
      })
      .get();
    _kafka_server.invoke_on_all(&rpc::server::start).get();
    vlog(
      _log.info, "Started Kafka API server listening at {}", conf.kafka_api());

    if (coproc_enabled()) {
        construct_single_service(_wasm_event_listener, std::ref(pacemaker));
        _wasm_event_listener->start().get();
        pacemaker.invoke_on_all(&coproc::pacemaker::start).get();
    }
    if (config::shard_local_cfg().enable_admin_api()) {
        _admin.invoke_on_all(&admin_server::start).get0();
    }
}
