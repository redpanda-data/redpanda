#include "redpanda/application.h"

#include "raft/service.h"
#include "storage/directories.h"
#include "syschecks/syschecks.h"

#include <seastar/core/prometheus.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/api_docs.hh>

#include <nlohmann/json.hpp>

#include <chrono>

static inline future<temporary_buffer<char>> read_fully(sstring name) {
    return open_file_dma(name, open_flags::ro).then([](file f) {
        return f.size()
          .then([f](uint64_t size) mutable {
              return f.dma_read_bulk<char>(0, size);
          })
          .then([f](temporary_buffer<char> buf) mutable {
              return f.close().then(
                [f, buf = std::move(buf)]() mutable { return std::move(buf); });
          });
    });
}

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

void application::hydrate_config(const po::variables_map& cfg) {
    auto buf = read_fully(cfg["redpanda-cfg"].as<std::string>()).get0();
    // see https://github.com/jbeder/yaml-cpp/issues/765
    std::string workaround(buf.get(), buf.size());
    YAML::Node config = YAML::Load(workaround);
    _log.info("Read file:\n\n{}\n\n", config);
    construct_service(_conf).get();
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
    storage::directories::initialize(
      _conf.local().data_directory().as_sstring())
      .get();
}

void application::create_groups() {
    _scheduling_groups.create_groups().get();
    _smp_groups.create_groups().get();
}

void application::configure_admin_server() {
    construct_service(_admin, sstring("admin")).get();
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
    construct_service(_raft_client_cache).get();
    construct_service(_shard_table).get();
    construct_service(
      _partition_manager,
      _conf.local().node_id(),
      std::chrono::milliseconds(_conf.local().raft_timeout()),
      _conf.local().data_directory().as_sstring(),
      _conf.local().log_segment_size(),
      storage::log_append_config::fsync::yes,
      std::chrono::seconds(10), // disk timeout
      std::ref(_shard_table),
      std::ref(_raft_client_cache))
      .get();
    _log.info("Partition manager started");

    // controller
    _controller = std::make_unique<cluster::controller>(
      model::node_id(_conf.local().node_id()),
      _conf.local().data_directory().as_sstring(),
      _conf.local().log_segment_size(),
      _partition_manager,
      _shard_table);
    _controller->start().get();
    _deferred.emplace_back([this] { _controller->stop().get(); });

    // group membership
    construct_service(_group_manager, std::ref(_partition_manager)).get();
    construct_service(_group_shard_mapper, std::ref(_shard_table)).get();
    construct_service(
      _group_router,
      _scheduling_groups.kafka_sg(),
      _smp_groups.kafka_smp_sg(),
      std::ref(_group_manager),
      std::ref(_group_shard_mapper))
      .get();

    // rpc
    rpc::server_configuration rpc_cfg;
    rpc_cfg.max_service_memory_per_core = memory_groups::rpc_total_memory();
    rpc_cfg.addrs.push_back(_conf.local().rpc_server);
    construct_service(_rpc, rpc_cfg).get();

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
    construct_service(_metadata_cache).get();

    // metrics and quota management
    construct_service(
      _quota_mgr,
      _conf.local().default_num_windows(),
      _conf.local().default_window_sec(),
      _conf.local().target_quota_byte_rate(),
      _conf.local().quota_manager_gc_sec())
      .get();
    _quota_mgr.invoke_on_all(&kafka::transport::quota_manager::start).get();

    // Kafka API
    auto kafka_creds
      = _conf.local().kafka_api_tls().get_credentials_builder().get0();
    kafka::transport::kafka_server_config server_config = {
      // FIXME: Add memory manager
      memory::stats().total_memory() / 10,
      _smp_groups.kafka_smp_sg(),
      kafka_creds};
    construct_service(
      _kafka_server,
      kafka::transport::probe(),
      std::ref(_metadata_cache),
      std::move(server_config),
      std::ref(_quota_mgr),
      std::ref(_group_router))
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
