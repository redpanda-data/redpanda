// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/application.h"

#include "kafka/client/configuration.h"
#include "pandaproxy/configuration.h"
#include "platform/stop_signal.h"
#include "rpc/dns.h"
#include "ssx/future-util.h"
#include "syschecks/syschecks.h"
#include "utils/file_io.h"
#include "utils/unresolved_address.h"
#include "version.h"

#include <seastar/core/prometheus.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/api_docs.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/util/defer.hh>

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <sys/utsname.h>

#include <chrono>
#include <vector>

namespace pandaproxy {

void pandaproxy_notify_ready() {
    ss::sstring msg = fmt::format(
      "READY=1\nSTATUS=pandaproxy is ready! - {}", redpanda_version());
    syschecks::systemd_raw_message(msg).get();
}

int application::run(int ac, char** av) {
    init_env();
    vlog(_log.info, "Pandaproxy {}", redpanda_version());
    struct ::utsname buf;
    ::uname(&buf);
    vlog(
      _log.info,
      "kernel={}, nodename={}, machine={}",
      buf.release,
      buf.nodename,
      buf.machine);
    ss::app_template app = setup_app_template();
    return app.run(ac, av, [this, &app] {
        auto& cfg = app.configuration();
        validate_arguments(cfg);
        return ss::async([this, &cfg] {
            try {
                ::stop_signal app_signal;
                auto deferred = ss::defer([this] {
                    auto deferred = std::move(_deferred);
                    // stop services in reverse order
                    while (!deferred.empty()) {
                        deferred.pop_back();
                    }
                });
                // must initialize configuration before services
                hydrate_config(cfg);
                initialize();
                check_environment();
                configure_admin_server();
                wire_up_services();
                start();
                app_signal.wait().get();
                vlog(_log.info, "Stopping...");
            } catch (...) {
                vlog(
                  _log.info,
                  "Failure during startup: {}",
                  std::current_exception());
                return 1;
            }
            return 0;
        });
    });
}

void application::initialize() {
    if (kafka::client::shard_local_cfg().brokers.value().empty()) {
        throw std::invalid_argument(
          "Pandaproxy requires at least 1 seed broker");
    }
}

void application::validate_arguments(
  const boost::program_options::variables_map& cfg) {
    if (!cfg.count("pandaproxy-cfg")) {
        throw std::invalid_argument("Missing pandaproxy-cfg flag");
    }
}

void application::init_env() { std::setvbuf(stdout, nullptr, _IOLBF, 1024); }

ss::app_template application::setup_app_template() {
    ss::app_template::config app_cfg;
    app_cfg.name = "Pandaproxy";
    using namespace std::literals::chrono_literals; // NOLINT
    app_cfg.default_task_quota = 500us;
    app_cfg.auto_handle_sigint_sigterm = false;
    auto app = ss::app_template(app_cfg);
    app.add_options()(
      "pandaproxy-cfg",
      boost::program_options::value<std::string>(),
      ".yaml file config for Pandaproxy");
    return app;
}

void application::hydrate_config(
  const boost::program_options::variables_map& cfg) {
    auto buf = read_fully(cfg["pandaproxy-cfg"].as<std::string>()).get0();
    // see https://github.com/jbeder/yaml-cpp/issues/765
    auto workaround = ss::uninitialized_string(buf.size_bytes());
    auto in = iobuf::iterator_consumer(buf.cbegin(), buf.cend());
    in.consume_to(buf.size_bytes(), workaround.begin());
    const YAML::Node config = YAML::Load(workaround);
    vlog(_log.info, "Configuration:\n\n{}\n\n", config);
    ss::smp::invoke_on_all([&config] {
        shard_local_cfg().read_yaml(config);
        kafka::client::shard_local_cfg().read_yaml(config);
    }).get0();
    vlog(
      _log.info,
      "Use `rpk config set pandaproxy-cfg <value>` to change values below:");
    auto config_printer = [this](const config::base_property& item) {
        std::stringstream val;
        item.print(val);
        vlog(_log.info, "{}\t- {}", val.str(), item.desc());
    };
    shard_local_cfg().for_each(config_printer);
    kafka::client::shard_local_cfg().for_each(config_printer);
}

void application::check_environment() {
    syschecks::systemd_message("checking environment (CPU, Mem)").get();
    syschecks::cpu();
    syschecks::memory(shard_local_cfg().developer_mode());
}

void application::configure_admin_server() {
    auto& conf = shard_local_cfg();
    if (!conf.enable_admin_api()) {
        return;
    }
    syschecks::systemd_message("constructing http server").get();
    construct_service(_admin, ss::sstring("admin")).get();
    ss::prometheus::config metrics_conf;
    metrics_conf.metric_help = "pandaproxy metrics";
    metrics_conf.prefix = "vectorized";
    ss::prometheus::add_prometheus_routes(_admin, metrics_conf).get();
    if (conf.enable_admin_api()) {
        syschecks::systemd_message(
          "enabling admin HTTP api: {}", shard_local_cfg().admin_api())
          .get();
        auto rb = ss::make_shared<ss::api_registry_builder20>(
          conf.admin_api_doc_dir(), "/v1");
        _admin
          .invoke_on_all([rb](ss::http_server& server) {
              rb->set_api_doc(server._routes);
              rb->register_api_file(server._routes, "header");
              rb->register_api_file(server._routes, "config");
              ss::httpd::config_json::get_config.set(
                server._routes, [](ss::const_req) {
                    rapidjson::StringBuffer buf;
                    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
                    shard_local_cfg().to_json(writer);
                    return ss::json::json_return_type(buf.GetString());
                });
          })
          .get();
    }

    rpc::resolve_dns(shard_local_cfg().admin_api())
      .then([this](ss::socket_address addr) mutable {
          return _admin
            .invoke_on_all<ss::future<> (ss::http_server::*)(
              ss::socket_address)>(&ss::http_server::listen, addr)
            .handle_exception([this](auto ep) {
                _log.error("Exception on http admin server: {}", ep);
                return ss::make_exception_future<>(ep);
            });
      })
      .get();

    vlog(
      _log.info,
      "Started HTTP admin service listening at {}",
      conf.admin_api());
}

// add additional services in here
void application::wire_up_services() {
    syschecks::systemd_message("Starting Pandaproxy").get();

    construct_service(
      _proxy,
      rpc::resolve_dns(shard_local_cfg().pandaproxy_api()).get(),
      kafka::client::shard_local_cfg().brokers())
      .get();
}

void application::start() {
    auto& conf = shard_local_cfg();
    _proxy.invoke_on_all(&proxy::start).get();
    vlog(_log.info, "Started Pandaproxy listening at {}", conf.pandaproxy_api);

    syschecks::systemd_message("Pandaproxy ready!").get();
    pandaproxy_notify_ready();
}

} // namespace pandaproxy
