#include "cluster/metadata_cache.h"
#include "filesystem/write_ahead_log.h"
#include "ioutil/dir_utils.h"
#include "redpanda/kafka/transport/probe.h"
#include "redpanda/kafka/transport/server.h"
#include "syschecks/syschecks.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/prometheus.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/thread.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/defer.hh>

#include <smf/histogram_seastar_utils.h>
#include <smf/log.h>
#include <smf/lz4_filter.h>
#include <smf/rpc_server.h>
#include <smf/unique_histogram_adder.h>
#include <smf/zstd_filter.h>

#include <fmt/format.h>

#include <chrono>
#include <iostream>

// in transition
#include "raft/raft_log_service.h"
#include "redpanda/redpanda_cfg.h"
#include "redpanda/redpanda_service.h"

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

future<> check_environment(const redpanda_cfg& c);

void register_service(
  smf::rpc_server& s,
  sharded<write_ahead_log>* log,
  const redpanda_cfg* c);

void stop_rpc(sharded<smf::rpc_server>& rpc, sstring);
bool hydrate_cfg(redpanda_cfg& c, std::string filename);

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
    redpanda_cfg global_cfg;
    app.add_options()(
      "redpanda-cfg",
      po::value<std::string>(),
      ".yaml file config for redpanda");
    return app.run(argc, argv, [&] {
    // Just being safe
#ifndef NDEBUG
        std::cout.setf(std::ios::unitbuf);
        smf::app_run_log_level(log_level::trace);
#endif
        return async([&] {
            ::stop_signal stop_signal;
            auto&& config = app.configuration();
            LOG_THROW_IF(
              !config.count("redpanda-cfg"), "Missing redpanda-cfg flag");
            LOG_THROW_IF(
              !hydrate_cfg(
                global_cfg, config["redpanda-cfg"].as<std::string>()),
              "Could not find `redpanda` section in: {}",
              config["redpanda-cfg"].as<std::string>());
            LOG_INFO("Configuration: {}", global_cfg);
            check_environment(global_cfg).get();
            static sharded<write_ahead_log> log;
            log.start(global_cfg.wal_cfg()).get();
            auto stop_log = defer([] { log.stop().get(); });
            log.invoke_on_all(&write_ahead_log::open).get();
            log.invoke_on_all(&write_ahead_log::index).get();
            static sharded<smf::rpc_server> rpc;
            rpc.start(global_cfg.rpc_cfg()).get();
            auto stop_rcp = defer([&] {
              stop_rpc(rpc, global_cfg.directory);
            });
            rpc.invoke_on_all([&](smf::rpc_server& s) {
                register_service(s, &log, &global_cfg);
            }).get();
            rpc.invoke_on_all(&smf::rpc_server::start).get();
            static sharded<cluster::metadata_cache> metadata_cache;
            LOG_INFO("Starting Metadata Cache");
            metadata_cache.start(std::ref(log)).get();
            auto stop_metadata_cache = defer(
              [] { metadata_cache.stop().get(); });

            static sharded<kafka::transport::kafka_server>
              kafka_server;
            smp_service_group_config
              storage_proxy_smp_service_group_config;
            // Assuming less than 1kB per queued request, this limits server
            // submit_to() queues to 5MB or less.
            auto kafka_server_smp_group
              = create_smp_service_group({5000}).get0();
            kafka::transport::kafka_server_config server_config = {
              // FIXME: Add memory manager
              memory::stats().total_memory() / 10,
              std::move(kafka_server_smp_group)};
            LOG_INFO("Starting Kafka API server");
            kafka_server
              .start(
                kafka::transport::probe(),
                std::ref(metadata_cache),
                std::move(server_config))
              .get();
            auto addr = socket_address(ipv4_addr(
              global_cfg.ip, global_cfg.kafka_transport_port));
            kafka_server
              .invoke_on_all([addr = std::move(addr)](
                               kafka::transport::kafka_server& server) mutable {
                  // FIXME: Configure keepalive.
                  return server.listen(std::move(addr), false);
              })
              .get();
            auto stop_kafka_server = defer(
              [] { kafka_server.stop().get(); });

            stop_signal.wait().get();
            LOG_INFO("Stopping...");
        });
    });
    return 0;
}

// Called in the context of a thread.
void stop_rpc(
  sharded<smf::rpc_server>& rpc, sstring directory) {
    auto h = rpc
               .map_reduce(
                 smf::unique_histogram_adder(),
                 &smf::rpc_server::copy_histogram)
               .get0();
    const sstring histogram_dir = fmt::format(
      "{}/redpanda.latencies.hgrm", directory);
    smf::histogram_seastar_utils::write(histogram_dir, std::move(h)).get();
    rpc.stop().get();
}

bool hydrate_cfg(redpanda_cfg& c, std::string filename) {
    YAML::Node config = YAML::LoadFile(filename);
    LOG_TRACE("Read file:\n\n{}\n\n", c);
    if (!config["redpanda"]) {
        return false;
    }
    c = config["redpanda"].as<redpanda_cfg>();
    return true;
}

future<> check_environment(const redpanda_cfg& c) {
    syschecks::cpu();
    syschecks::memory(c.developer_mode);
    return dir_utils::create_dir_tree(c.directory);
}

void register_service(
  smf::rpc_server& s,
  sharded<write_ahead_log>* log,
  const redpanda_cfg* c) {
    using lz4_c_t = smf::lz4_compression_filter;
    using lz4_d_t = smf::lz4_decompression_filter;
    using zstd_d_t = smf::zstd_decompression_filter;
    s.register_outgoing_filter<lz4_c_t>(1 << 21 /*2MB*/);
    s.register_incoming_filter<lz4_d_t>();
    s.register_incoming_filter<zstd_d_t>();
    s.register_service<redpanda_service>(c, log);
    s.register_service<raft_log_service>(c->raft(), log);
}
