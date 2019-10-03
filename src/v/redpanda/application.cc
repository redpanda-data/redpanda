#include "redpanda/application.h"

#include "raft/service.h"
#include "storage/directories.h"
#include "syschecks/syschecks.h"

#include <seastar/core/prometheus.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/api_docs.hh>

#include <nlohmann/json.hpp>

#include <chrono>

int application::run(int ac, char** av) {
    init_env();
    app_template app = setup_app_template();
    return app.run(ac, av, [this, &app] {
        auto cfg = app.configuration();
        validate_arguments(cfg);
        return async([this, &cfg] {
            ::stop_signal stop_signal;
            // add another steps if needed here
            hydrate_config(cfg);
            check_environment();
            create_groups();
            configure_admin_server();
            wire_up_services();
            auto deferred = std::move(_deferred);
            stop_signal.wait().get();
            _log.info("Stopping...");
        });
    });
}

void application::validate_arguments(const po::variables_map& cfg) {
    if (!cfg.count("redpanda-cfg")) {
        throw std::invalid_argument("Missing redpanda-cfg flag");
    }
}

void application::init_env() {
    syschecks::initialize_intrinsics();
    std::setvbuf(stdout, nullptr, _IOLBF, 1024);
}

app_template application::setup_app_template() {
    app_template::config app_cfg;
    app_cfg.name = "Redpanda";
    using namespace std::literals::chrono_literals; // NOLINT
    app_cfg.default_task_quota = 500us;
    app_cfg.auto_handle_sigint_sigterm = false;
    auto app = app_template(app_cfg);
    app.add_options()(
      "redpanda-cfg",
      po::value<std::string>(),
      ".yaml file config for redpanda");
    return app;
}

template<typename Service, typename... Args>
future<> application::start_service(sharded<Service>& s, Args&&... args) {
    auto f = s.start(std::forward<Args>(args)...);
    _deferred.emplace_back([&s] { s.stop().get(); });
    return f;
}

void application::hydrate_config(const po::variables_map& cfg) {
    YAML::Node config = YAML::LoadFile(cfg["redpanda-cfg"].as<std::string>());
    _log.info("Read file:\n\n{}\n\n", config);
    start_service(_conf).get();
    auto& local_cfg = _conf.local();
    local_cfg.read_yaml(config);
    _conf
      .invoke_on_others([&local_cfg, this](auto& cfg) {
          // propagate to other shards
          local_cfg.visit_all([&local_cfg, &cfg](auto& property) {
              cfg.get(property.name()) = property;
          });
      })
      .get();
}

void application::check_environment() {
    syschecks::cpu();
    syschecks::memory(_conf.local().developer_mode());
    storage::directories::initialize(_conf.local().data_directory()).get();
}

void application::create_groups() {
    _scheduling_groups.create_groups().get();
    _smp_groups.create_groups().get();
}

void application::configure_admin_server() {
    start_service(_admin, sstring("admin")).get();
    prometheus::config metrics_conf;
    metrics_conf.metric_help = "redpanda metrics";
    metrics_conf.prefix = "vectorized";
    prometheus::add_prometheus_routes(_admin, metrics_conf).get();

    if (_conf.local().enable_admin_api()) {
        auto rb = make_shared<api_registry_builder20>(
          _conf.local().admin_api_doc_dir(), "/v1");
        _admin
          .invoke_on_all([rb, this](http_server& server) {
              rb->set_api_doc(server._routes);
              rb->register_api_file(server._routes, "header");
              rb->register_api_file(server._routes, "config");
              config_json::get_config.set(
                server._routes, [this](const_req req) {
                    nlohmann::json jconf;
                    _conf.local().to_json(jconf);
                    return json::json_return_type(jconf.dump());
                });
          })
          .get();
    }

    with_scheduling_group(_scheduling_groups.admin_sg(), [this] {
        return _admin.invoke_on_all(&http_server::listen, _conf.local().admin())
          .handle_exception([this](auto ep) {
              _log.error("Exception on http admin server: {}", ep);
              return make_exception_future<>(ep);
          });
    }).get();

    _log.info(
      "Started HTTP admin service listening at {}", _conf.local().admin());
}

// add additional services in here
void application::wire_up_services() {
    // cluster
    start_service(_raft_client_cache).get();
    start_service(_shard_table).get();
    start_service(
      _partition_manager,
      _conf.local().node_id(),
      std::ref(_shard_table),
      std::ref(_raft_client_cache))
      .get();
    _log.info("Partition manager started");

    // controller
    _controller = std::make_unique<cluster::controller>(
      _conf.local().node_id(),
      _conf.local().data_directory(),
      _conf.local().log_segment_size(),
      _partition_manager,
      _shard_table);

    _deferred.emplace_back([this] { _controller->stop().get(); });
    // rpc
    rpc::server_configuration rpc_cfg;
    rpc_cfg.max_service_memory_per_core = memory_groups::rpc_total_memory();
    rpc_cfg.addrs.push_back(_conf.local().rpc_server);
    start_service(_rpc, rpc_cfg).get();

    _rpc
      .invoke_on_all([this](rpc::server& s) {
          s.register_service<
            raft::service<cluster::partition_manager, cluster::shard_table>>(
            _scheduling_groups.raft_sg(),
            _smp_groups.raft_smp_sg(),
            _partition_manager,
            _shard_table.local());
      })
      .get();
    _rpc.invoke_on_all(&rpc::server::start).get();
    _log.info("Started RPC server listening at {}", _conf.local().rpc_server());
    start_service(_metadata_cache).get();

    // metrics and quota management
    start_service(
      _quota_mgr,
      _conf.local().default_num_windows(),
      _conf.local().default_window_sec(),
      _conf.local().target_quota_byte_rate(),
      _conf.local().quota_manager_gc_sec())
      .get();
    _quota_mgr.invoke_on_all([](auto& qm) mutable { return qm.start(); }).get();

    // Kafka API
    auto kafka_creds
      = _conf.local().kafka_api_tls().get_credentials_builder().get0();
    kafka::transport::kafka_server_config server_config = {
      // FIXME: Add memory manager
      memory::stats().total_memory() / 10,
      _smp_groups.kafka_smp_sg(),
      kafka_creds};
    start_service(
      _kafka_server,
      kafka::transport::probe(),
      std::ref(_metadata_cache),
      std::move(server_config),
      std::ref(_quota_mgr))
      .get();

    _kafka_server
      .invoke_on_all([this](kafka::transport::kafka_server& server) mutable {
          // FIXME: Configure keepalive.
          return server.listen(_conf.local().kafka_api(), false);
      })
      .get();
    _log.info(
      "Started Kafka API server listening at {}", _conf.local().kafka_api());
}
