#include "cluster/metadata_cache.h"
#include "cluster/partition_manager.h"
#include "raft/group_shard_table.h"
#include "raft/service.h"
#include "redpanda/admin/api-doc/config.json.h"
#include "redpanda/kafka/transport/probe.h"
#include "redpanda/kafka/transport/server.h"
#include "resource_mgmt/cpu_scheduling.h"
#include "resource_mgmt/memory_groups.h"
#include "resource_mgmt/smp_groups.h"
#include "rpc/server.h"
#include "storage/directories.h"
#include "syschecks/syschecks.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/prometheus.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/api_docs.hh>
#include <seastar/http/httpd.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/defer.hh>

#include <fmt/format.h>
#include <fmt/ostream.h>
#include <nlohmann/json.hpp>

#include <chrono>
#include <iostream>

logger lgr{"redpanda::main"};

// in transition
#include "redpanda/config/configuration.h"

class stop_signal {
    void signaled() {
        _caught = true;
        _cond.broadcast();
    }

public:
    stop_signal() {
        engine().handle_signal(SIGINT, [this] { signaled(); });
        engine().handle_signal(SIGTERM, [this] { signaled(); });
    }

    ~stop_signal() {
        // There's no way to unregister a handler yet, so register a no-op
        // handler instead.
        engine().handle_signal(SIGINT, [] {});
        engine().handle_signal(SIGTERM, [] {});
    }

    future<> wait() {
        return _cond.wait([this] { return _caught; });
    }

    bool stopping() const {
        return _caught;
    }

private:
    bool _caught = false;
    condition_variable _cond;
};

future<> check_environment(const config::configuration& c);
bool hydrate_cfg(sharded<config::configuration>& c, std::string filename);

int main(int argc, char** argv, char** env) {
    // This is needed for detecting sse4.2 instructions
    syschecks::initialize_intrinsics();
    namespace po = boost::program_options; // NOLINT
    std::setvbuf(stdout, nullptr, _IOLBF, 1024);
    app_template::config app_cfg;
    app_cfg.name = "Redpanda";
    app_cfg.default_task_quota = 500us;
    app_cfg.auto_handle_sigint_sigterm = false;
    app_template app(std::move(app_cfg));
    app.add_options()(
      "redpanda-cfg",
      po::value<std::string>(),
      ".yaml file config for redpanda");
    return app.run(argc, argv, [&] {
    // Just being safe
#ifndef NDEBUG
        std::cout.setf(std::ios::unitbuf);
#endif
        return async([&] {
            ::stop_signal stop_signal;
            auto&& config = app.configuration();
            if (!config.count("redpanda-cfg")) {
                throw std::runtime_error("Missing redpanda-cfg flag");
            }
            static sharded<config::configuration> rp_config;
            rp_config.start().get();
            auto destroy_config = defer([]() { rp_config.stop().get(); });
            if (!hydrate_cfg(
                  rp_config, config["redpanda-cfg"].as<std::string>())) {
                throw std::runtime_error(fmt::format(
                  "Could not find `redpanda` section in: {}",
                  config["redpanda-cfg"].as<std::string>()));
            }
            lgr.info("Configuration: {}", rp_config.local());
            check_environment(rp_config.local()).get();

            scheduling_groups sched_groups;
            sched_groups.create_groups().get();
            smp_groups smpgs;
            smpgs.create_groups().get();

            // cluster
            static sharded<raft::client_cache> raft_client_cache;
            raft_client_cache.start().get();
            auto stop_client_cache = defer(
              [] { raft_client_cache.stop().get(); });

            static sharded<raft::group_shard_table> group_shard_table;
            group_shard_table.start().get();
            auto stop_group_shard_table = defer(
              [] { group_shard_table.stop().get(); });

            static sharded<cluster::partition_manager> partition_manager;
            partition_manager
              .start(
                rp_config.local().node_id(),
                std::ref(group_shard_table),
                std::ref(raft_client_cache))
              .get();
            auto stop_partition_manager = defer(
              [] { partition_manager.stop().get(); });

            // rpc
            rpc::server_configuration rpc_cfg;
            rpc_cfg.max_service_memory_per_core
              = memory_groups::rpc_total_memory();
            rpc_cfg.addrs.push_back(rp_config.local().rpc_server);
            static sharded<rpc::server> rpc;
            rpc.start(rpc_cfg).get();
            auto stop_rpc = defer([] { rpc.stop().get(); });
            rpc
              .invoke_on_all([&](rpc::server& s) {
                  s.register_service<raft::service<cluster::partition_manager>>(
                    sched_groups.raft_sg(),
                    smpgs.raft_smp_sg(),
                    partition_manager,
                    group_shard_table);
              })
              .get();
            rpc.invoke_on_all(&rpc::server::start).get();
            // prometheus admin
            static sharded<http_server> admin;
            admin.start(sstring("admin")).get();
            auto stop_admin = defer([] { admin.stop().get(); });

            prometheus::config metrics_conf;
            metrics_conf.metric_help = "red panda metrics";
            metrics_conf.prefix = "vectorized";
            prometheus::add_prometheus_routes(admin, metrics_conf).get();

            if (rp_config.local().enable_admin_api()) {
                auto rb = make_shared<api_registry_builder20>(
                  rp_config.local().admin_api_doc_dir(), "/v1");
                admin
                  .invoke_on_all([rb](http_server& server) {
                      rb->set_api_doc(server._routes);
                      rb->register_api_file(server._routes, "header");
                      rb->register_api_file(server._routes, "config");
                      config_json::get_config.set(
                        server._routes, [](const_req req) {
                            nlohmann::json jconf;
                            rp_config.local().to_json(jconf);
                            return json::json_return_type(jconf.dump());
                        });
                  })
                  .get();
            }

            with_scheduling_group(sched_groups.admin_sg(), [&] {
                return admin
                  .invoke_on_all(
                    &http_server::listen, rp_config.local().admin())
                  .handle_exception([](auto ep) {
                      lgr.error("Exception on http admin server: {}", ep);
                      return make_exception_future<>(ep);
                  });
            }).get();

            static sharded<cluster::metadata_cache> metadata_cache;
            lgr.info("Starting Metadata Cache");
            metadata_cache.start().get();
            auto stop_metadata_cache = defer(
              [] { metadata_cache.stop().get(); });

            // metrics and quota management
            static sharded<kafka::transport::quota_manager> quota_mgr;
            quota_mgr
              .start(
                rp_config.local().default_num_windows(),
                rp_config.local().default_window_sec(),
                rp_config.local().target_quota_byte_rate(),
                rp_config.local().quota_manager_gc_sec())
              .get();
            quota_mgr.invoke_on_all([](auto& qm) mutable { return qm.start(); })
              .get();
            auto stop_quota_mgr = defer([] { quota_mgr.stop().get(); });
            static sharded<kafka::transport::kafka_server> kafka_server;
            kafka::transport::kafka_server_config server_config = {
              // FIXME: Add memory manager
              memory::stats().total_memory() / 10,
              smpgs.kafka_smp_sg()};
            lgr.info("Starting Kafka API server");
            kafka_server
              .start(
                kafka::transport::probe(),
                std::ref(metadata_cache),
                std::move(server_config),
                std::ref(quota_mgr))
              .get();
            kafka_server
              .invoke_on_all(
                [](kafka::transport::kafka_server& server) mutable {
                    // FIXME: Configure keepalive.
                    return server.listen(rp_config.local().kafka_api(), false);
                })
              .get();
            auto stop_kafka_server = defer([] { kafka_server.stop().get(); });

            stop_signal.wait().get();
            lgr.info("Stopping...");
        });
    });
    return 0;
}

bool hydrate_cfg(sharded<config::configuration>& c, std::string filename) {
    YAML::Node config = YAML::LoadFile(filename);
    lgr.info("Read file:\n\n{}\n\n", config);
    auto& local_cfg = c.local();
    local_cfg.read_yaml(config);
    c.invoke_on_others([&local_cfg](auto& cfg) {
         // propagate to other shards
         local_cfg.visit_all([&local_cfg, &cfg](auto& property) {
             cfg.get(property.name()) = property;
         });
     })
      .get();

    return true;
}

future<> check_environment(const config::configuration& c) {
    syschecks::cpu();
    syschecks::memory(c.developer_mode());
    return storage::directories::initialize(c.data_directory());
}
