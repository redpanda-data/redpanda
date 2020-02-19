#include "redpanda/application.h"

#include "cluster/service.h"
#include "kafka/protocol.h"
#include "platform/stop_signal.h"
#include "raft/service.h"
#include "rpc/simple_protocol.h"
#include "storage/directories.h"
#include "syschecks/syschecks.h"
#include "test_utils/logs.h"
#include "utils/file_io.h"

#include <seastar/core/prometheus.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/api_docs.hh>

#include <nlohmann/json.hpp>
#include <sys/utsname.h>

#include <chrono>

int application::run(int ac, char** av) {
    init_env();
    struct ::utsname buf;
    ::uname(&buf);
    _log.info(
      "kernel={}, nodename={}, machine={}",
      buf.release,
      buf.nodename,
      buf.machine);
    ss::app_template app = setup_app_template();
    return app.run(ac, av, [this, &app] {
        auto& cfg = app.configuration();
        validate_arguments(cfg);
        return ss::async([this, &cfg] {
            ::stop_signal app_signal;
            auto deferred = ss::defer([this] {
                auto deferred = std::move(_deferred);
                // stop services in reverse order
                while (!deferred.empty()) {
                    deferred.pop_back();
                }
            });
            initialize();
            hydrate_config(cfg);
            check_environment();
            configure_admin_server();
            wire_up_services();
            start();
            app_signal.wait().get();
            _log.info("Stopping...");
        });
    });
}

void application::initialize() {
    _scheduling_groups.create_groups().get();
    _deferred.emplace_back(
      [this] { _scheduling_groups.destroy_groups().get(); });
    _smp_groups.create_groups().get();
    _deferred.emplace_back([this] { _smp_groups.destroy_groups().get(); });
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
    ss::sstring workaround(ss::sstring::initialized_later(), buf.size_bytes());
    auto in = iobuf::iterator_consumer(buf.cbegin(), buf.cend());
    in.consume_to(buf.size_bytes(), workaround.begin());
    YAML::Node config = YAML::Load(workaround);
    _log.info("Configuration:\n\n{}\n\n", config);
    ss::smp::invoke_on_all([config] {
        config::shard_local_cfg().read_yaml(config);
    }).get0();
}

void application::check_environment() {
    auto& cfg = config::shard_local_cfg();
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
          .invoke_on_all([rb, this](ss::http_server& server) {
              rb->set_api_doc(server._routes);
              rb->register_api_file(server._routes, "header");
              rb->register_api_file(server._routes, "config");
              ss::httpd::config_json::get_config.set(
                server._routes, [this](ss::const_req req) {
                    nlohmann::json jconf;
                    config::shard_local_cfg().to_json(jconf);
                    return ss::json::json_return_type(jconf.dump());
                });
          })
          .get();
    }

    with_scheduling_group(_scheduling_groups.admin_sg(), [this] {
        return config::shard_local_cfg().admin().resolve().then(
          [this](ss::socket_address addr) mutable {
              return _admin.invoke_on_all(&ss::http_server::listen, addr)
                .handle_exception([this](auto ep) {
                    _log.error("Exception on http admin server: {}", ep);
                    return ss::make_exception_future<>(ep);
                });
          });
    }).get();

    _log.info("Started HTTP admin service listening at {}", conf.admin());
}

// add additional services in here
void application::wire_up_services() {
    // cluster
    syschecks::systemd_message("Adding raft client cache");
    construct_service(_raft_connection_cache).get();
    syschecks::systemd_message("Building shard-lookup tables");
    construct_service(shard_table).get();
    syschecks::systemd_message("Adding partition manager");
    construct_service(
      partition_manager,
      storage::log_append_config::fsync::yes,
      std::chrono::seconds(10), // disk timeout
      std::ref(shard_table),
      std::ref(_raft_connection_cache))
      .get();
    _log.info("Partition manager started");

    // controller
    syschecks::systemd_message("Creating kafka metadata cache");
    construct_service(metadata_cache).get();

    syschecks::systemd_message("Creating cluster::controller");
    construct_service(
      controller,
      std::ref(partition_manager),
      std::ref(shard_table),
      std::ref(metadata_cache),
      std::ref(_raft_connection_cache))
      .get();

    // group membership
    syschecks::systemd_message("Creating partition manager");
    construct_service(
      _group_manager,
      std::ref(partition_manager),
      std::ref(config::shard_local_cfg()))
      .get();
    syschecks::systemd_message("Creating kafka group shard mapper");
    construct_service(_group_shard_mapper, std::ref(shard_table)).get();
    syschecks::systemd_message("Creating kafka group router");
    construct_service(
      group_router,
      _scheduling_groups.kafka_sg(),
      _smp_groups.kafka_smp_sg(),
      std::ref(_group_manager),
      std::ref(_group_shard_mapper))
      .get();

    // rpc
    rpc::server_configuration rpc_cfg;
    rpc_cfg.max_service_memory_per_core = memory_groups::rpc_total_memory();
    auto rpc_server_addr
      = config::shard_local_cfg().rpc_server().resolve().get0();
    rpc_cfg.addrs.push_back(rpc_server_addr);
    syschecks::systemd_message("Starting internal RPC {}", rpc_cfg);
    construct_service(_rpc, rpc_cfg).get();

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

    // metrics and quota management
    syschecks::systemd_message("Adding kafka quota manager");
    construct_service(_quota_mgr).get();

    syschecks::systemd_message("Building kafka controller dispatcher");
    construct_service(
      cntrl_dispatcher,
      std::ref(controller),
      _smp_groups.kafka_smp_sg(),
      _scheduling_groups.kafka_sg())
      .get();
}

void application::start() {
    syschecks::systemd_message("Starting controller");
    controller.invoke_on_all(&cluster::controller::start).get();

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
            std::ref(controller));
          s.set_protocol(std::move(proto));
      })
      .get();
    auto& conf = config::shard_local_cfg();
    _rpc.invoke_on_all(&rpc::server::start).get();
    _log.info("Started RPC server listening at {}", conf.rpc_server());

    _quota_mgr.invoke_on_all(&kafka::quota_manager::start).get();

    // Kafka API
    _kafka_server
      .invoke_on_all([this](rpc::server& s) {
          _log.info("adding kafka protocol to kafka server");
          auto proto = std::make_unique<kafka::protocol>(
            _smp_groups.kafka_smp_sg(),
            metadata_cache,
            cntrl_dispatcher,
            _quota_mgr,
            group_router,
            shard_table,
            partition_manager);
          s.set_protocol(std::move(proto));
      })
      .get();
    _kafka_server.invoke_on_all(&rpc::server::start).get();
    _log.info("Started Kafka API server listening at {}", conf.kafka_api());
    syschecks::systemd_message("redpanda ready!");
    syschecks::systemd_notify_ready();
}
