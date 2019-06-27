#include "filesystem/write_ahead_log.h"
#include "ioutil/dir_utils.h"
#include "syschecks/syschecks.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/prometheus.hh>
#include <seastar/core/thread.hh>

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

seastar::future<> check_environment(const redpanda_cfg& c);

void register_service(
  smf::rpc_server& s,
  seastar::distributed<write_ahead_log>* log,
  const redpanda_cfg* c);

void rpc_at_exit(seastar::distributed<smf::rpc_server>& rpc, seastar::sstring);
bool hydrate_cfg(redpanda_cfg& c, std::string filename);

int main(int argc, char** argv, char** env) {
    // This is needed for detecting sse4.2 instructions
    syschecks::initialize_intrinsics();
    namespace po = boost::program_options; // NOLINT
    std::setvbuf(stdout, nullptr, _IOLBF, 1024);
    seastar::distributed<smf::rpc_server> rpc;
    seastar::distributed<write_ahead_log> log;
    seastar::app_template::config app_cfg;
    app_cfg.name = "Redpanda";
    app_cfg.default_task_quota = 500us;
    app_cfg.auto_handle_sigint_sigterm = false;
    seastar::app_template app(std::move(app_cfg));
    redpanda_cfg global_cfg;
    app.add_options()(
      "redpanda-cfg",
      po::value<std::string>(),
      ".yaml file config for redpanda");
    return app.run(argc, argv, [&] {
    // Just being safe
#ifndef NDEBUG
        std::cout.setf(std::ios::unitbuf);
        smf::app_run_log_level(seastar::log_level::trace);
#endif
        return seastar::async([&] {
            seastar::engine().at_exit([&log] { return log.stop(); });
            auto&& config = app.configuration();
            LOG_THROW_IF(
              !config.count("redpanda-cfg"), "Missing redpanda-cfg flag");
            LOG_THROW_IF(
              !hydrate_cfg(
                global_cfg, config["redpanda-cfg"].as<std::string>()),
              "Could not find `redpanda` section in: {}",
              config["redpanda-cfg"].as<std::string>());
            LOG_INFO("Configuration: {}", global_cfg);
            rpc_at_exit(rpc, global_cfg.directory);
            check_environment(global_cfg).get();
            log.start(global_cfg.wal_cfg()).get();
            log.invoke_on_all(&write_ahead_log::open).get();
            log.invoke_on_all(&write_ahead_log::index).get();
            rpc.start(global_cfg.rpc_cfg()).get();
            rpc.invoke_on_all([&](smf::rpc_server& s) {
                register_service(s, &log, &global_cfg);
            }).get();
            rpc.invoke_on_all(&smf::rpc_server::start).get();
        });
    });
    return 0;
}

void rpc_at_exit(
  seastar::distributed<smf::rpc_server>& rpc, seastar::sstring directory) {
    seastar::engine().at_exit([&rpc] { return rpc.stop(); });
    seastar::engine().at_exit([&rpc, directory] {
        return rpc
          .map_reduce(
            smf::unique_histogram_adder(), &smf::rpc_server::copy_histogram)
          .then([directory](auto h) {
              const seastar::sstring histogram_dir = fmt::format(
                "{}/redpanda.latencies.hgrm", directory);
              return smf::histogram_seastar_utils::write(
                histogram_dir, std::move(h));
          });
    });
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

seastar::future<> check_environment(const redpanda_cfg& c) {
    syschecks::cpu();
    syschecks::memory(c.developer_mode);
    return dir_utils::create_dir_tree(c.directory);
}

void register_service(
  smf::rpc_server& s,
  seastar::distributed<write_ahead_log>* log,
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
