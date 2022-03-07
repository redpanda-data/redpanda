// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "net/server.h"

#include "config/configuration.h"
#include "net/unresolved_address.h"
#include "platform/stop_signal.h"
#include "raft/consensus.h"
#include "raft/consensus_client_protocol.h"
#include "raft/heartbeat_manager.h"
#include "raft/logger.h"
#include "raft/rpc_client_protocol.h"
#include "raft/service.h"
#include "raft/tron/logger.h"
#include "raft/tron/service.h"
#include "raft/types.h"
#include "rpc/connection_cache.h"
#include "rpc/simple_protocol.h"
#include "storage/api.h"
#include "storage/logger.h"
#include "syschecks/syschecks.h"
#include "utils/hdr_hist.h"
#include "vlog.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <fmt/format.h>

#include <cctype>
#include <cstdint>
#include <string>

using namespace std::chrono_literals; // NOLINT

auto& tronlog = raft::tron::tronlog;
namespace po = boost::program_options; // NOLINT

void cli_opts(po::options_description_easy_init o) {
    o("ip",
      po::value<ss::sstring>()->default_value("127.0.0.1"),
      "ip to listen to");
    o("workdir",
      po::value<ss::sstring>()->default_value("."),
      "work directory");
    o("peers",
      po::value<std::vector<ss::sstring>>()->multitoken(),
      "--peers 1,127.0.0.1:11215 \n --peers 2,127.0.0.0.1:11216");
    o("port", po::value<uint16_t>()->default_value(20776), "port for service");
    o("heartbeat-timeout-ms",
      po::value<int32_t>()->default_value(100),
      "raft heartbeat timeout in milliseconds");
    o("node-id", po::value<int32_t>(), "node-id required");
    o("key",
      po::value<ss::sstring>()->default_value(""),
      "key for TLS seccured connection");
    o("cert",
      po::value<ss::sstring>()->default_value(""),
      "cert for TLS seccured connection");
}

struct simple_shard_lookup {
    ss::shard_id shard_for(raft::group_id g) { return g() % ss::smp::count; }
    bool contains(raft::group_id g) { return true; }
};

class simple_group_manager {
public:
    simple_group_manager(
      model::node_id self,
      ss::sstring directory,
      std::chrono::milliseconds raft_heartbeat_interval,
      ss::sharded<rpc::connection_cache>& clients)
      : _self(self)
      , _consensus_client_protocol(
          raft::make_rpc_client_protocol(self, clients))
      , _storage(
          [directory]() {
              return storage::kvstore_config(
                1_MiB,
                config::mock_binding(10ms),
                directory,
                storage::debug_sanitize_files::yes);
          },
          [directory]() {
              return storage::log_config(
                storage::log_config::storage_type::disk,
                std::move(directory),
                1_GiB,
                storage::debug_sanitize_files::yes);
          })
      , _hbeats(
          raft_heartbeat_interval,
          _consensus_client_protocol,
          self,
          raft_heartbeat_interval * 20)
      , _recovery_memory_quota([] {
          return raft::recovery_memory_quota::configuration{
            .max_recovery_memory = config::mock_binding<std::optional<size_t>>(
              std::nullopt),
            .default_read_buffer_size = config::mock_binding(512_KiB),
          };
      }) {}

    ss::lw_shared_ptr<raft::consensus> consensus_for(raft::group_id) {
        return _consensus;
    }

    ss::future<> start(raft::group_configuration init_cfg) {
        return _storage.start()
          .then([this, init_cfg = std::move(init_cfg)]() mutable {
              return _storage.log_mgr()
                .manage(storage::ntp_config(
                  _ntp, _storage.log_mgr().config().base_dir))
                .then(
                  [this, cfg = std::move(init_cfg)](storage::log log) mutable {
                      _consensus = ss::make_lw_shared<raft::consensus>(
                        _self,
                        raft::group_id(66),
                        std::move(cfg),
                        raft::timeout_jitter(
                          config::shard_local_cfg().raft_election_timeout_ms()),
                        log,
                        raft::scheduling_config(
                          seastar::default_scheduling_group(),
                          seastar::default_priority_class()),
                        std::chrono::seconds(1),
                        _consensus_client_protocol,
                        [this](raft::leadership_status st) {
                            if (!st.current_leader) {
                                vlog(
                                  tronlog.info,
                                  "No leader in group {}",
                                  st.group);
                                return;
                            }
                            vlog(
                              tronlog.info,
                              "New leader {} elected in group {}",
                              st.current_leader.value(),
                              st.group);
                        },
                        _storage,
                        std::nullopt,
                        _recovery_memory_quota);
                      return _consensus->start().then(
                        [this] { return _hbeats.register_group(_consensus); });
                  });
          })
          .then([this] { return _hbeats.start(); });
    }
    ss::future<> stop() {
        return _consensus->stop()
          .then([this] { return _hbeats.stop(); })
          .then([this] { return _storage.stop(); });
    }

private:
    model::node_id _self;
    raft::consensus_client_protocol _consensus_client_protocol;
    storage::api _storage;
    raft::heartbeat_manager _hbeats;
    raft::recovery_memory_quota _recovery_memory_quota;
    model::ntp _ntp{
      model::ns("master_control_program"),
      model::topic("tron"),
      model::partition_id(ss::this_shard_id())};
    ss::lw_shared_ptr<raft::consensus> _consensus;
};

static std::pair<model::node_id, rpc::transport_configuration>
extract_peer(ss::sstring peer) {
    std::vector<ss::sstring> parts;
    parts.reserve(2);
    boost::split(parts, peer, boost::is_any_of(","));
    if (parts.size() != 2) {
        throw std::runtime_error(fmt::format("Could not parse peer:{}", peer));
    }
    int32_t n = boost::lexical_cast<int32_t>(parts[0]);
    rpc::transport_configuration cfg;
    std::vector<ss::sstring> address_parts;
    boost::split(parts, parts[1], boost::is_any_of(":"));
    cfg.server_addr = net::unresolved_address(
      address_parts[0], boost::lexical_cast<int16_t>(address_parts[1]));
    return {model::node_id(n), cfg};
}

static void initialize_connection_cache_in_thread(
  model::node_id self,
  ss::sharded<rpc::connection_cache>& cache,
  std::vector<ss::sstring> opts) {
    for (auto& i : opts) {
        auto [node, cfg] = extract_peer(i);
        for (ss::shard_id i = 0; i < ss::smp::count; ++i) {
            auto shard = rpc::connection_cache::shard_for(self, i, node);
            ss::smp::submit_to(shard, [&cache, shard, n = node, config = cfg] {
                return cache.local().emplace(
                  n,
                  config,
                  rpc::make_exponential_backoff_policy<rpc::clock_type>(
                    std::chrono::seconds(1), std::chrono::seconds(60)));
            }).get();
        }
    }
}

static model::broker broker_from_arg(ss::sstring peer) {
    std::vector<ss::sstring> parts;
    parts.reserve(2);
    boost::split(parts, peer, boost::is_any_of(","));
    if (parts.size() != 2) {
        throw std::runtime_error(fmt::format("Could not parse peer:{}", peer));
    }
    int32_t id = boost::lexical_cast<int32_t>(parts[0]);
    std::vector<ss::sstring> host_port;
    host_port.reserve(2);
    boost::split(host_port, parts[1], boost::is_any_of(":"));
    if (host_port.size() != 2) {
        throw std::runtime_error(fmt::format("Could not host:{}", parts[1]));
    }
    auto port = boost::lexical_cast<int32_t>(parts[0]);
    return model::broker(
      model::node_id(id),
      net::unresolved_address(host_port[0], port),
      net::unresolved_address(host_port[0], port),
      std::nullopt,
      model::broker_properties{.cores = ss::smp::count});
}

static raft::group_configuration
group_cfg_from_args(const po::variables_map& opts) {
    std::vector<model::broker> brokers;
    if (opts.find("peers") != opts.end()) {
        auto peers = opts["peers"].as<std::vector<ss::sstring>>();
        for (auto& arg : peers) {
            brokers.push_back(broker_from_arg(arg));
        }
    }
    // add self
    brokers.push_back(model::broker(
      model::node_id(opts["node-id"].as<int32_t>()),
      net::unresolved_address(
        opts["ip"].as<ss::sstring>(), opts["port"].as<uint16_t>()),
      net::unresolved_address(
        opts["ip"].as<ss::sstring>(), opts["port"].as<uint16_t>()),
      std::optional<ss::sstring>(),
      model::broker_properties{
        .cores = ss::smp::count,
      }));
    return raft::group_configuration(std::move(brokers), model::revision_id(0));
}

int main(int args, char** argv, char** env) {
    syschecks::initialize_intrinsics();
    std::setvbuf(stdout, nullptr, _IOLBF, 1024);
    ss::sharded<net::server> serv;
    ss::sharded<rpc::connection_cache> connection_cache;
    ss::sharded<simple_group_manager> group_manager;
    ss::app_template app;
    cli_opts(app.add_options());
    return app.run(args, argv, [&] {
        return ss::async([&] {
#ifndef NDEBUG
            std::cout.setf(std::ios::unitbuf);
#endif
            raft::raftlog.trace("ack");
            storage::stlog.trace("ack");
            stop_signal app_signal;
            auto& cfg = app.configuration();
            connection_cache.start().get();
            auto ccd = ss::defer(
              [&connection_cache] { connection_cache.stop().get(); });
            net::server_configuration scfg("tron_rpc");

            scfg.max_service_memory_per_core
              = ss::memory::stats().total_memory() * .7;
            auto key = cfg["key"].as<ss::sstring>();
            auto cert = cfg["cert"].as<ss::sstring>();
            ss::shared_ptr<ss::tls::server_credentials> credentials;
            if (key != "" && cert != "") {
                auto builder = ss::tls::credentials_builder();
                builder.set_dh_level(ss::tls::dh_params::level::MEDIUM);
                builder
                  .set_x509_key_file(cert, key, ss::tls::x509_crt_format::PEM)
                  .get();
                credentials
                  = builder.build_reloadable_server_credentials().get0();
            }
            scfg.addrs.emplace_back(
              ss::socket_address(
                ss::net::inet_address(cfg["ip"].as<ss::sstring>()),
                cfg["port"].as<uint16_t>()),
              credentials);
            auto self_id = cfg["node-id"].as<int32_t>();
            if (cfg.find("peers") != cfg.end()) {
                initialize_connection_cache_in_thread(
                  model::node_id(self_id),
                  connection_cache,
                  cfg["peers"].as<std::vector<ss::sstring>>());
            }
            const ss::sstring workdir = ssx::sformat(
              "{}/greetings-{}",
              cfg["workdir"].as<ss::sstring>(),
              cfg["node-id"].as<int32_t>());
            vlog(tronlog.info, "Work directory:{}", workdir);

            auto hbeat_interval = std::chrono::milliseconds(
              cfg["heartbeat-timeout-ms"].as<int32_t>());
            // initialize group_manager
            group_manager
              .start(
                model::node_id(cfg["node-id"].as<int32_t>()),
                workdir,
                hbeat_interval,
                std::ref(connection_cache))
              .get();
            serv.start(scfg).get();
            auto dserv = ss::defer([&serv] { serv.stop().get(); });
            vlog(tronlog.info, "registering service on all cores");
            simple_shard_lookup shard_table;
            serv
              .invoke_on_all(
                [&shard_table, &group_manager, hbeat_interval](net::server& s) {
                    auto proto = std::make_unique<rpc::simple_protocol>();
                    proto->register_service<raft::tron::service<
                      simple_group_manager,
                      simple_shard_lookup>>(
                      ss::default_scheduling_group(),
                      ss::default_smp_service_group(),
                      group_manager,
                      shard_table);
                    proto->register_service<
                      raft::service<simple_group_manager, simple_shard_lookup>>(
                      ss::default_scheduling_group(),
                      ss::default_smp_service_group(),
                      group_manager,
                      shard_table,
                      hbeat_interval);
                    s.set_protocol(std::move(proto));
                })
              .get();
            vlog(tronlog.info, "Invoking rpc start on all cores");
            serv.invoke_on_all(&net::server::start).get();
            vlog(tronlog.info, "Starting group manager");
            group_manager
              .invoke_on_all([&cfg](simple_group_manager& m) {
                  return m.start(group_cfg_from_args(cfg));
              })
              .get();
            app_signal.wait().get();
        });
    });
}
