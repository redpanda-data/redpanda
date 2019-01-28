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
#include "syschecks/syschecks.h"

// bmtl
#include "bmtl_cfg.h"
#include "bmtl_service.h"

void
rpc_at_exit(seastar::distributed<smf::rpc_server> &rpc) {
  seastar::engine().at_exit([&rpc] { return rpc.stop(); });
  seastar::engine().at_exit([&rpc] {
    return rpc
      .map_reduce(smf::unique_histogram_adder(),
                  &smf::rpc_server::copy_histogram)
      .then([](auto h) {
        return smf::histogram_seastar_utils::write("bmtl.latencies.hgrm",
                                                   std::move(h));
      });
  });
}

bool
hydrate_cfg(rp::bmtl_cfg &c, std::string filename) {
  YAML::Node config = YAML::LoadFile(filename);
  LOG_INFO("Read yaml configuration:\n**********\n{}\n**********", config);
  if (!config["bmtl"]) { return false; }
  c = config["bmtl"].as<rp::bmtl_cfg>();
  return true;
}

// TODO(agallego) -
// 1) reduce memory size to account for defaults in memory used by the WAL
// 2) compute some reasonable bloat for the requests for RPC

int
main(int argc, char **argv, char **env) {
  namespace po = boost::program_options;  // NOLINT
  std::setvbuf(stdout, nullptr, _IOLBF, 1024);
  seastar::distributed<smf::rpc_server> rpc;
  seastar::distributed<rp::write_ahead_log> log;
  seastar::app_template app;
  rp::bmtl_cfg global_cfg;
  app.add_options()("bmtl-cfg", po::value<std::string>(),
                    ".yaml file config for bmtl");
  return app.run_deprecated(argc, argv, [&] {
  // Just begin safe
#ifndef NDEBUG
    std::cout.setf(std::ios::unitbuf);
    smf::app_run_log_level(seastar::log_level::trace);
#endif
    rpc_at_exit(rpc);
    seastar::engine().at_exit([&log] { return log.stop(); });
    auto &&config = app.configuration();
    LOG_THROW_IF(!config.count("bmtl-cfg"), "Missing bmtl-cfg flag");
    LOG_THROW_IF(!hydrate_cfg(global_cfg, config["bmtl-cfg"].as<std::string>()),
                 "Could not find `bmtl` section in: {}",
                 config["bmtl-cfg"].as<std::string>());
    rp::syschecks::cpu();
    rp::syschecks::memory(global_cfg.developer_mode);
    LOG_INFO("Hydrated config: {}", global_cfg);
    return log.start(global_cfg.wal_cfg())
      .then([&log] { return log.invoke_on_all(&rp::write_ahead_log::open); })
      .then([&log] { return log.invoke_on_all(&rp::write_ahead_log::index); })
      .then([&rpc, &global_cfg] { return rpc.start(global_cfg.rpc_cfg()); })
      .then([&rpc, &global_cfg, &log] {
        return rpc.invoke_on_all([&](smf::rpc_server &s) {
          using srvc = rp::bmtl_service;
          using lz4_c_t = smf::lz4_compression_filter;
          using lz4_d_t = smf::lz4_decompression_filter;
          using zstd_d_t = smf::zstd_decompression_filter;
          s.register_outgoing_filter<lz4_c_t>(1 << 21 /*2MB*/);
          s.register_incoming_filter<lz4_d_t>();
          s.register_incoming_filter<zstd_d_t>();
          s.register_service<srvc>(&global_cfg, &log);
        });
      })
      .then([&rpc] { return rpc.invoke_on_all(&smf::rpc_server::start); });
  });
  return 0;
}
