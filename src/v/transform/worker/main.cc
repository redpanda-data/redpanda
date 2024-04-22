/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/seastarx.h"
#include "base/vlog.h"
#include "net/server.h"
#include "net/types.h"
#include "syschecks/syschecks.h"
#include "transform/worker/worker.h"
#include "utils/stop_signal.h"
#include "version.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/reactor.hh>
#include <seastar/util/file.hh>

#include <boost/program_options.hpp>
#include <sys/utsname.h>
#include <yaml-cpp/node/parse.h>

#include <chrono>
#include <exception>

namespace po = boost::program_options;
using namespace std::literals::chrono_literals;

namespace transform::worker {
namespace {
static ss::logger log{"transform/worker"}; // NOLINT

struct config {
    struct listener {
        ss::sstring host;
        uint16_t port = 0;
        static listener parse(const YAML::Node& root) {
            return {
              .host = root["host"].as<ss::sstring>(),
              .port = root["port"].as<uint16_t>(),
            };
        }
    };
    listener listener;
    static config parse(const YAML::Node& root) {
        return {
          .listener = listener::parse(root["listener"]),
        };
    }
};

net::server_configuration create_server_config(const config& cfg) {
    net::server_configuration server_cfg("redpanda-worker");
    server_cfg.addrs.emplace_back(ss::socket_address(
      ss::net::inet_address(cfg.listener.host), cfg.listener.port));
    server_cfg.disable_public_metrics = net::public_metrics_disabled::yes;
    server_cfg.max_service_memory_per_core = int64_t(
      ss::memory::stats().total_memory());
    return server_cfg;
}

ss::future<int> run(std::filesystem::path config_file) {
    vlog(log.info, "Redpanda worker {}", redpanda_version());
    struct ::utsname buf; // NOLINT
    ::uname(&buf);
    vlog(
      log.info,
      "kernel={}, nodename={}, machine={}",
      buf.release,
      buf.nodename,
      buf.machine);
    // Set budgets
    ss::engine().update_blocked_reactor_notify_ms(
      std::chrono::duration_cast<std::chrono::milliseconds>(500us));
    ss::memory::set_large_allocation_warning_threshold(128_KiB); // NOLINT
    ss::memory::set_abort_on_allocation_failure(true);
    // End budgets
    stop_signal signal; // TODO: Pass down abort source from signal
    worker_service service;
    config cfg;
    try {
        ss::sstring file = co_await ss::util::read_entire_file_contiguous(
          config_file);
        cfg = config::parse(YAML::Load(file.c_str()));
        co_await service.start(worker_service::config{
          .server = create_server_config(cfg),
        });
    } catch (...) {
        vlog(
          log.warn,
          "Redpanda worker startup failed: {}",
          std::current_exception());
        co_return 1;
    }
    co_await signal.wait();
    co_await service.stop();
    co_return 0;
}
} // namespace
} // namespace transform::worker

/**
 * This binary hosts an off-broker worker for Data Transforms.
 */
int main(int ac, char** av) {
    // must be the first thing called
    syschecks::initialize_intrinsics();
    ss::app_template::config app_cfg;
    app_cfg.name = "Redpanda Transform Worker";
    app_cfg.default_task_quota = 500us;
    app_cfg.auto_handle_sigint_sigterm = false;
    ss::app_template app(app_cfg);
    app.add_options()(
      "redpanda-worker-cfg",
      po::value<std::string>(),
      ".yaml file config for redpanda worker nodes");
    return app.run(ac, av, [&app]() noexcept {
        return transform::worker::run(
          app.configuration()["redpanda-worker-cfg"].as<std::string>());
    });
}
