#include "cluster/metadata_cache.h"
#include "filesystem/write_ahead_log.h"
#include "ioutil/dir_utils.h"
#include "redpanda/kafka/transport/probe.h"
#include "redpanda/kafka/transport/server.h"
#include "redpanda/rpc_config.h"
#include "resource_mgmt/cpu_scheduling.h"
#include "syschecks/syschecks.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/prometheus.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/httpd.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/defer.hh>

#include <fmt/format.h>

#include <chrono>
#include <iostream>

logger lgr{"redpanda::main"};

// in transition
#include "raft/raft_log_service.h"
#include "redpanda/config/configuration.h"

using namespace std::chrono_literals;

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
            sched_groups.start(rp_config.local()).get();

            // prometheus admin
            static sharded<http_server> admin;
            admin.start(sstring("admin")).get();
            auto stop_admin = defer([] { admin.stop().get(); });

            prometheus::config metrics_conf;
            metrics_conf.metric_help = "red panda metrics";
            metrics_conf.prefix = "vectorized";
            prometheus::add_prometheus_routes(admin, metrics_conf).get();

            with_scheduling_group(sched_groups.admin(), [&] {
                return admin
                  .invoke_on_all(
                    &http_server::listen, rp_config.local().admin())
                  .handle_exception([](auto ep) {
                      lgr.error("Exception on http admin server: {}", ep);
                      return make_exception_future<>(ep);
                  });
            }).get();

            static sharded<write_ahead_log> log;
            log.start(wal_opts(rp_config.local())).get();
            auto stop_log = defer([] { log.stop().get(); });
            log.invoke_on_all(&write_ahead_log::open).get();
            log.invoke_on_all(&write_ahead_log::index).get();
            static sharded<cluster::metadata_cache> metadata_cache;
            lgr.info("Starting Metadata Cache");
            metadata_cache.start(std::ref(log)).get();
            auto stop_metadata_cache = defer(
              [] { metadata_cache.stop().get(); });

            static sharded<kafka::transport::kafka_server> kafka_server;
            smp_service_group_config storage_proxy_smp_service_group_config;
            // Assuming less than 1kB per queued request, this limits server
            // submit_to() queues to 5MB or less.
            auto kafka_server_smp_group
              = create_smp_service_group({5000}).get0();
            kafka::transport::kafka_server_config server_config = {
              // FIXME: Add memory manager
              memory::stats().total_memory() / 10,
              std::move(kafka_server_smp_group)};
            lgr.info("Starting Kafka API server");
            kafka_server
              .start(
                kafka::transport::probe(),
                std::ref(metadata_cache),
                std::move(server_config))
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
    LOG_TRACE("Read file:\n\n{}\n\n", config);
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
    return dir_utils::create_dir_tree(c.data_directory());
}
