#include <iostream>

#include <seastar/core/app-template.hh>
#include <seastar/core/prometheus.hh>
#include <smf/histogram_seastar_utils.h>
#include <smf/log.h>
#include <smf/lz4_filter.h>
#include <smf/rpc_server.h>
#include <smf/unique_histogram_adder.h>
#include <smf/zstd_filter.h>

#include "filesystem/write_ahead_log.h"
#include "ioutil/dir_utils.h"
#include "syschecks/syschecks.h"

// redpanda
#include "redpanda_cfg.h"
#include "redpanda_service.h"

seastar::future<> check_environment(const v::redpanda_cfg &c);

void register_service(smf::rpc_server &s,
                      seastar::distributed<v::write_ahead_log> *log,
                      const v::redpanda_cfg *c);

void rpc_at_exit(seastar::distributed<smf::rpc_server> &rpc);
bool hydrate_cfg(v::redpanda_cfg &c, std::string filename);

int
main(int argc, char **argv, char **env) {
  // This is needed for detecting sse4.2 instructions
  v::syschecks::initialize_intrinsics();
  namespace po = boost::program_options;  // NOLINT
  std::setvbuf(stdout, nullptr, _IOLBF, 1024);
  seastar::distributed<smf::rpc_server> rpc;
  seastar::distributed<v::write_ahead_log> log;
  seastar::app_template app;
  v::redpanda_cfg global_cfg;
  app.add_options()("redpanda-cfg", po::value<std::string>(),
                    ".yaml file config for redpanda");
  return app.run_deprecated(argc, argv, [&] {
  // Just being safe
#ifndef NDEBUG
    std::cout.setf(std::ios::unitbuf);
    smf::app_run_log_level(seastar::log_level::trace);
#endif
    rpc_at_exit(rpc);
    seastar::engine().at_exit([&log] { return log.stop(); });
    auto &&config = app.configuration();
    LOG_THROW_IF(!config.count("redpanda-cfg"), "Missing redpanda-cfg flag");
    LOG_THROW_IF(
      !hydrate_cfg(global_cfg, config["redpanda-cfg"].as<std::string>()),
      "Could not find `redpanda` section in: {}",
      config["redpanda-cfg"].as<std::string>());
    LOG_INFO("Configuration: {}", global_cfg);
    return check_environment(global_cfg)
      .then([&log, &global_cfg] { return log.start(global_cfg.wal_cfg()); })
      .then([&log] { return log.invoke_on_all(&v::write_ahead_log::open); })
      .then([&log] { return log.invoke_on_all(&v::write_ahead_log::index); })
      .then([&rpc, &global_cfg] { return rpc.start(global_cfg.rpc_cfg()); })
      .then([&rpc, &global_cfg, &log] {
        return rpc.invoke_on_all(
          [&](smf::rpc_server &s) { register_service(s, &log, &global_cfg); });
      })
      .then([&rpc] { return rpc.invoke_on_all(&smf::rpc_server::start); });
  });
  return 0;
}

void
rpc_at_exit(seastar::distributed<smf::rpc_server> &rpc) {
  seastar::engine().at_exit([&rpc] { return rpc.stop(); });
  seastar::engine().at_exit([&rpc] {
    return rpc
      .map_reduce(smf::unique_histogram_adder(),
                  &smf::rpc_server::copy_histogram)
      .then([](auto h) {
        return smf::histogram_seastar_utils::write("redpanda.latencies.hgrm",
                                                   std::move(h));
      });
  });
}

bool
hydrate_cfg(v::redpanda_cfg &c, std::string filename) {
  YAML::Node config = YAML::LoadFile(filename);
  LOG_TRACE("Read file:\n\n{}\n\n", c);
  if (!config["redpanda"]) { return false; }
  c = config["redpanda"].as<v::redpanda_cfg>();
  return true;
}

seastar::future<>
check_environment(const v::redpanda_cfg &c) {
  v::syschecks::cpu();
  v::syschecks::memory(c.developer_mode);
  return v::dir_utils::create_dir_tree(c.directory);
}

void
register_service(smf::rpc_server &s,
                 seastar::distributed<v::write_ahead_log> *log,
                 const v::redpanda_cfg *c) {
  using srvc = v::redpanda_service;
  using lz4_c_t = smf::lz4_compression_filter;
  using lz4_d_t = smf::lz4_decompression_filter;
  using zstd_d_t = smf::zstd_decompression_filter;
  s.register_outgoing_filter<lz4_c_t>(1 << 21 /*2MB*/);
  s.register_incoming_filter<lz4_d_t>();
  s.register_incoming_filter<zstd_d_t>();
  s.register_service<srvc>(c, log);
}
