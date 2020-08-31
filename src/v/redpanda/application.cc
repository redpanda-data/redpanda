#include "redpanda/application.h"

#include "cluster/metadata_dissemination_handler.h"
#include "cluster/metadata_dissemination_service.h"
#include "cluster/service.h"
#include "config/configuration.h"
#include "config/seed_server.h"
#include "kafka/protocol.h"
#include "model/metadata.h"
#include "platform/stop_signal.h"
#include "raft/service.h"
#include "rpc/simple_protocol.h"
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
#include <seastar/util/defer.hh>

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <sys/utsname.h>

#include <chrono>
#include <vector>

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

void application::initialize() {
    if (config::shard_local_cfg().enable_pid_file()) {
        syschecks::pidfile_create(config::shard_local_cfg().pidfile_path());
    }
    _scheduling_groups.create_groups().get();
    _deferred.emplace_back(
      [this] { _scheduling_groups.destroy_groups().get(); });
    _smp_groups.create_groups().get();
    _deferred.emplace_back([this] { _smp_groups.destroy_groups().get(); });
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
    auto buf = read_fully(cfg["redpanda-cfg"].as<std::string>()).get0();
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
          vlog(_log.info, "{}\t- {}", val.str(), item.desc());
      });
}

void application::check_environment() {
    syschecks::systemd_message("checking environment (CPU, Mem)");
    syschecks::cpu();
    syschecks::memory(config::shard_local_cfg().developer_mode());
    storage::directories::initialize(
      config::shard_local_cfg().data_directory().as_sstring())
      .get();
}

void application::configure_admin_server() {
    auto& conf = config::shard_local_cfg();
    if (!conf.enable_admin_api()) {
        return;
    }
    syschecks::systemd_message("constructing http server");
    construct_service(_admin, ss::sstring("admin")).get();
    // configure admin API TLS
    if (conf.admin_api_tls().is_enabled()) {
        _admin
          .invoke_on_all([](ss::http_server& server) {
              return config::shard_local_cfg()
                .admin_api_tls()
                .get_credentials_builder()
                .then([&server](
                        std::optional<ss::tls::credentials_builder> builder) {
                    if (!builder) {
                        return;
                    }
                    server.set_tls_credentials(
                      builder->build_server_credentials());
                });
          })
          .get0();
    }
    ss::prometheus::config metrics_conf;
    metrics_conf.metric_help = "redpanda metrics";
    metrics_conf.prefix = "vectorized";
    ss::prometheus::add_prometheus_routes(_admin, metrics_conf).get();
    if (conf.enable_admin_api()) {
        syschecks::systemd_message(
          "enabling admin HTTP api: {}", config::shard_local_cfg().admin());
        auto rb = ss::make_shared<ss::api_registry_builder20>(
          conf.admin_api_doc_dir(), "/v1");
        _admin
          .invoke_on_all([rb](ss::http_server& server) {
              rb->set_api_doc(server._routes);
              rb->register_api_file(server._routes, "header");
              rb->register_api_file(server._routes, "config");
              ss::httpd::config_json::get_config.set(
                server._routes, []([[maybe_unused]] ss::const_req req) {
                    rapidjson::StringBuffer buf;
                    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
                    config::shard_local_cfg().to_json(writer);
                    return ss::json::json_return_type(buf.GetString());
                });
          })
          .get();
    }

    with_scheduling_group(_scheduling_groups.admin_sg(), [this] {
        return config::shard_local_cfg().admin().resolve().then(
          [this](ss::socket_address addr) mutable {
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
      storage::debug_sanitize_files::no,
      config::shard_local_cfg().retention_bytes(),
      config::shard_local_cfg().log_compaction_interval_ms(),
      config::shard_local_cfg().delete_retention_ms(),
      storage::log_config::with_cache(
        config::shard_local_cfg().disable_batch_cache()),
      storage::batch_cache::reclaim_options{
        .growth_window = config::shard_local_cfg().reclaim_growth_window(),
        .stable_window = config::shard_local_cfg().reclaim_stable_window(),
        .min_size = config::shard_local_cfg().reclaim_min_size(),
        .max_size = config::shard_local_cfg().reclaim_max_size(),
      });
}

// add additional services in here
void application::wire_up_services() {
    // cluster
    syschecks::systemd_message("Adding raft client cache");
    construct_service(_raft_connection_cache).get();
    syschecks::systemd_message("Building shard-lookup tables");
    construct_service(shard_table).get();

    if (coproc_enabled()) {
        syschecks::systemd_message("Building coproc mapping tables");
        construct_service(mappings).get();
    }

    syschecks::systemd_message("Intializing storage services");
    construct_service(
      storage,
      kvstore_config_from_global_config(),
      manager_config_from_global_config())
      .get();

    syschecks::systemd_message("Intializing raft group manager");
    construct_service(
      raft_group_manager,
      model::node_id(config::shard_local_cfg().node_id()),
      config::shard_local_cfg().raft_io_timeout_ms(),
      config::shard_local_cfg().raft_heartbeat_interval_ms(),
      std::ref(_raft_connection_cache),
      std::ref(storage))
      .get();

    syschecks::systemd_message("Adding partition manager");
    construct_service(
      partition_manager, std::ref(storage), std::ref(raft_group_manager))
      .get();
    vlog(_log.info, "Partition manager started");

    // controller

    syschecks::systemd_message("Creating cluster::controller");

    construct_single_service(
      controller,
      _raft_connection_cache,
      partition_manager,
      shard_table,
      storage);

    controller->wire_up().get0();
    syschecks::systemd_message("Creating kafka metadata cache");
    construct_service(
      metadata_cache,
      std::ref(controller->get_topics_state()),
      std::ref(controller->get_members_table()),
      std::ref(controller->get_partition_leaders()))
      .get();

    syschecks::systemd_message("Creating metadata dissemination service");
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
    syschecks::systemd_message("Creating partition manager");
    construct_service(
      _group_manager,
      std::ref(raft_group_manager),
      std::ref(partition_manager),
      std::ref(config::shard_local_cfg()))
      .get();
    syschecks::systemd_message("Creating kafka group shard mapper");
    construct_service(coordinator_ntp_mapper, std::ref(metadata_cache)).get();
    syschecks::systemd_message("Creating kafka group router");
    construct_service(
      group_router,
      _scheduling_groups.kafka_sg(),
      _smp_groups.kafka_smp_sg(),
      std::ref(_group_manager),
      std::ref(shard_table),
      std::ref(coordinator_ntp_mapper))
      .get();

    // metrics and quota management
    syschecks::systemd_message("Adding kafka quota manager");
    construct_service(_quota_mgr).get();
    // rpc
    rpc::server_configuration rpc_cfg;
    rpc_cfg.max_service_memory_per_core = memory_groups::rpc_total_memory();
    auto rpc_server_addr
      = config::shard_local_cfg().rpc_server().resolve().get0();
    rpc_cfg.addrs.push_back(rpc_server_addr);
    rpc_cfg.credentials = config::shard_local_cfg()
                            .rpc_server_tls()
                            .get_credentials_builder()
                            .get0();
    syschecks::systemd_message("Starting internal RPC {}", rpc_cfg);
    construct_service(_rpc, rpc_cfg).get();

    // coproc rpc
    if (coproc_enabled()) {
        rpc::server_configuration cp_rpc_cfg;
        cp_rpc_cfg.max_service_memory_per_core
          = memory_groups::rpc_total_memory();
        cp_rpc_cfg.addrs.emplace_back(
          ss::socket_address(ss::ipv4_addr("127.0.0.1", 43188)));
        syschecks::systemd_message(
          "Starting coprocessor internal RPC {}", cp_rpc_cfg);
        construct_service(_coproc_rpc, cp_rpc_cfg).get();
    }

    rpc::server_configuration kafka_cfg;
    kafka_cfg.max_service_memory_per_core = memory_groups::kafka_total_memory();
    auto kafka_addr = config::shard_local_cfg().kafka_api().resolve().get0();
    kafka_cfg.addrs.push_back(kafka_addr);
    syschecks::systemd_message("Building TLS credentials for kafka");
    kafka_cfg.credentials = config::shard_local_cfg()
                              .kafka_api_tls()
                              .get_credentials_builder()
                              .get0();
    syschecks::systemd_message("Starting kafka RPC {}", kafka_cfg);
    construct_service(_kafka_server, kafka_cfg).get();
}

void application::start() {
    syschecks::systemd_message("Staring storage services");
    storage.invoke_on_all(&storage::api::start).get();

    syschecks::systemd_message("Starting the partition manager");
    partition_manager.invoke_on_all(&cluster::partition_manager::start).get();

    syschecks::systemd_message("Starting Raft group manager");
    raft_group_manager.invoke_on_all(&raft::group_manager::start).get();

    syschecks::systemd_message("Starting Kafka group manager");
    _group_manager.invoke_on_all(&kafka::group_manager::start).get();

    syschecks::systemd_message("Starting controller");
    controller->start().get0();

    // FIXME: in first patch explain why this is started after the
    // controller so the broker set will be available. Then next patch fix.
    syschecks::systemd_message("Starting metadata dissination service");
    md_dissemination_service
      .invoke_on_all(&cluster::metadata_dissemination_service::start)
      .get();

    syschecks::systemd_message("Starting RPC");
    _rpc
      .invoke_on_all([this](rpc::server& s) {
          auto proto = std::make_unique<rpc::simple_protocol>();
          proto->register_service<
            raft::service<cluster::partition_manager, cluster::shard_table>>(
            _scheduling_groups.raft_sg(),
            _smp_groups.raft_smp_sg(),
            partition_manager,
            shard_table.local());
          proto->register_service<cluster::service>(
            _scheduling_groups.cluster_sg(),
            _smp_groups.cluster_smp_sg(),
            std::ref(controller->get_topics_frontend()),
            std::ref(controller->get_members_manager()),
            std::ref(metadata_cache));
          proto->register_service<cluster::metadata_dissemination_handler>(
            _scheduling_groups.cluster_sg(),
            _smp_groups.cluster_smp_sg(),
            std::ref(controller->get_partition_leaders()));
          s.set_protocol(std::move(proto));
      })
      .get();
    auto& conf = config::shard_local_cfg();
    _rpc.invoke_on_all(&rpc::server::start).get();
    vlog(_log.info, "Started RPC server listening at {}", conf.rpc_server());

    if (coproc_enabled()) {
        syschecks::systemd_message("Starting coproc RPC");
        _coproc_rpc
          .invoke_on_all([this](rpc::server& s) {
              auto proto = std::make_unique<rpc::simple_protocol>();
              proto->register_service<coproc::service>(
                _scheduling_groups.coproc_sg(),
                _smp_groups.coproc_smp_sg(),
                std::ref(mappings),
                std::ref(storage));
              s.set_protocol(std::move(proto));
          })
          .get();
        _coproc_rpc.invoke_on_all(&rpc::server::start).get();
        vlog(
          _log.info, "Started coproc RPC server listening at 127.0.0.1:43188");
    }

    _quota_mgr.invoke_on_all(&kafka::quota_manager::start).get();

    // Kafka API
    _kafka_server
      .invoke_on_all([this](rpc::server& s) {
          auto proto = std::make_unique<kafka::protocol>(
            _smp_groups.kafka_smp_sg(),
            metadata_cache,
            controller->get_topics_frontend(),
            _quota_mgr,
            group_router,
            shard_table,
            partition_manager,
            coordinator_ntp_mapper);
          s.set_protocol(std::move(proto));
      })
      .get();
    _kafka_server.invoke_on_all(&rpc::server::start).get();
    vlog(
      _log.info, "Started Kafka API server listening at {}", conf.kafka_api());

    vlog(_log.info, "Successfully started Redpanda!");
    syschecks::systemd_notify_ready();
}
