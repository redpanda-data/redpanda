#include "rpc/server.h"

#include "platform/stop_signal.h"
#include "raft/client_cache.h"
#include "raft/consensus.h"
#include "raft/heartbeat_manager.h"
#include "raft/logger.h"
#include "raft/service.h"
#include "raft/tron/logger.h"
#include "raft/tron/service.h"
#include "raft/types.h"
#include "seastarx.h"
#include "storage/log_manager.h"
#include "storage/logger.h"
#include "syschecks/syschecks.h"
#include "utils/hdr_hist.h"

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

auto& tronlog = raft::tron::tronlog;
namespace po = boost::program_options; // NOLINT

void cli_opts(po::options_description_easy_init o) {
    o("ip",
      po::value<sstring>()->default_value("127.0.0.1"),
      "ip to listen to");
    o("workdir", po::value<sstring>()->default_value("."), "work directory");
    o("peers",
      po::value<std::vector<sstring>>()->multitoken(),
      "--peers 1,127.0.0.1:11215 \n --peers 2,127.0.0.0.1:11216");
    o("port", po::value<uint16_t>()->default_value(20776), "port for service");
    o("heartbeat-timeout-ms",
      po::value<int32_t>()->default_value(100),
      "raft heartbeat timeout in milliseconds");
    o("node-id", po::value<int32_t>(), "node-id required");
    o("key",
      po::value<sstring>()->default_value(""),
      "key for TLS seccured connection");
    o("cert",
      po::value<sstring>()->default_value(""),
      "cert for TLS seccured connection");
}

struct simple_shard_lookup {
    seastar::shard_id shard_for(raft::group_id g) {
        return g() % seastar::smp::count;
    }
};

class simple_group_manager {
public:
    simple_group_manager(
      model::node_id self,
      sstring directory,
      std::chrono::milliseconds raft_timeout,
      sharded<raft::client_cache>& clients)
      : _self(self)
      , _mngr(storage::log_config{
          .base_dir = directory,
          .max_segment_size = 1 << 30,
          .should_sanitize = storage::log_config::sanitize_files::yes})
      , _hbeats(raft_timeout, clients)
      , _clients(clients) {}

    raft::consensus& consensus_for(raft::group_id g) { return *_consensus; }

    future<> start(raft::group_configuration init_cfg) {
        return _mngr.manage(_ntp)
          .then(
            [this, cfg = std::move(init_cfg)](storage::log_ptr log) mutable {
                _consensus = make_lw_shared<raft::consensus>(
                  _self,
                  raft::group_id(66),
                  std::move(cfg),
                  raft::timeout_jitter(_hbeats.election_duration()),
                  *log,
                  storage::log_append_config::fsync::yes,
                  default_priority_class(),
                  std::chrono::seconds(1),
                  _clients,
                  [this](raft::group_id g) {
                      tronlog.info("Took leadership of: {}", g);
                  });
                _hbeats.register_group(_consensus);
                return _consensus->start();
            })
          .then([this] { return _hbeats.start(); });
    }
    future<> stop() {
        return _consensus->stop()
          .then([this] { return _hbeats.stop(); })
          .then([this] { return _mngr.stop(); });
    }

private:
    model::node_id _self;
    storage::log_manager _mngr;
    raft::heartbeat_manager _hbeats;
    sharded<raft::client_cache>& _clients;
    model::ntp _ntp{
      model::ns("master_control_program"),
      model::topic_partition{model::topic("tron"),
                             model::partition_id(engine().cpu_id())}};
    lw_shared_ptr<raft::consensus> _consensus;
};

static std::pair<model::node_id, rpc::client_configuration>
extract_peer(sstring peer) {
    std::vector<sstring> parts;
    parts.reserve(2);
    boost::split(parts, peer, boost::is_any_of(","));
    if (parts.size() != 2) {
        throw std::runtime_error(fmt::format("Could not parse peer:{}", peer));
    }
    int32_t n = boost::lexical_cast<int32_t>(parts[0]);
    rpc::client_configuration cfg;
    cfg.server_addr = ipv4_addr(parts[1]);
    return {model::node_id(n), cfg};
}

static void initialize_client_cache_in_thread(
  sharded<raft::client_cache>& cache, std::vector<sstring> opts) {
    for (auto& i : opts) {
        auto [node, cfg] = extract_peer(i);
        auto shard = raft::client_cache::shard_for(node);
        smp::submit_to(shard, [&cache, shard, n = node, config = cfg] {
            tronlog.info("shard: {} owns {}->{}", shard, n, config.server_addr);
            return cache.local().emplace(n, config);
        }).get();
    }
}

static model::broker broker_from_arg(sstring peer) {
    std::vector<sstring> parts;
    parts.reserve(2);
    boost::split(parts, peer, boost::is_any_of(","));
    if (parts.size() != 2) {
        throw std::runtime_error(fmt::format("Could not parse peer:{}", peer));
    }
    int32_t id = boost::lexical_cast<int32_t>(parts[0]);
    std::vector<sstring> host_port;
    host_port.reserve(2);
    boost::split(host_port, parts[1], boost::is_any_of(":"));
    if (host_port.size() != 2) {
        throw std::runtime_error(fmt::format("Could not host:{}", parts[1]));
    }
    auto port = boost::lexical_cast<int32_t>(parts[0]);
    return model::broker(
      model::node_id(id),
      socket_address(net::inet_address(host_port[0]), port),
      socket_address(net::inet_address(host_port[0]), port),
      std::nullopt,
      model::broker_properties{.cores = smp::count});
}

static raft::group_configuration
group_cfg_from_args(const po::variables_map& opts) {
    raft::group_configuration cfg;
    auto peers = opts["peers"].as<std::vector<sstring>>();
    for (auto& arg : peers) {
        cfg.nodes.push_back(broker_from_arg(arg));
    }
    // add self
    cfg.nodes.push_back(model::broker(
      model::node_id(opts["node-id"].as<int32_t>()),
      socket_address(
        net::inet_address(opts["ip"].as<sstring>()),
        opts["port"].as<uint16_t>()),
      socket_address(
        net::inet_address(opts["ip"].as<sstring>()),
        opts["port"].as<uint16_t>()),
      std::optional<sstring>(),
      model::broker_properties{
        .cores = smp::count,
      }));
    return cfg;
}

int main(int args, char** argv, char** env) {
    syschecks::initialize_intrinsics();
    std::setvbuf(stdout, nullptr, _IOLBF, 1024);
    seastar::sharded<rpc::server> serv;
    seastar::sharded<raft::client_cache> client_cache;
    seastar::sharded<simple_group_manager> group_manager;
    seastar::app_template app;
    cli_opts(app.add_options());
    return app.run(args, argv, [&] {
        return seastar::async([&] {
#ifndef NDEBUG
            std::cout.setf(std::ios::unitbuf);
#endif
            raft::raftlog.trace("ack");
            storage::stlog.trace("ack");
            stop_signal app_signal;
            auto& cfg = app.configuration();
            client_cache.start().get();
            auto ccd = seastar::defer(
              [&client_cache] { client_cache.stop().get(); });
            rpc::server_configuration scfg;
            scfg.addrs.push_back(socket_address(
              net::inet_address(cfg["ip"].as<sstring>()),
              cfg["port"].as<uint16_t>()));
            scfg.max_service_memory_per_core = memory::stats().total_memory()
                                               * .7;
            auto key = cfg["key"].as<sstring>();
            auto cert = cfg["cert"].as<sstring>();
            if (key != "" && cert != "") {
                auto builder = tls::credentials_builder();
                builder.set_dh_level(tls::dh_params::level::MEDIUM);
                builder.set_x509_key_file(cert, key, tls::x509_crt_format::PEM)
                  .get();
                scfg.credentials = std::move(builder);
            }
            initialize_client_cache_in_thread(
              client_cache, cfg["peers"].as<std::vector<sstring>>());

            const sstring workdir = fmt::format(
              "{}/greetings-{}",
              cfg["workdir"].as<sstring>(),
              cfg["node-id"].as<int32_t>());
            tronlog.info("Work directory:{}", workdir);

            // initialize group_manager
            group_manager
              .start(
                model::node_id(cfg["node-id"].as<int32_t>()),
                workdir,
                std::chrono::milliseconds(
                  cfg["heartbeat-timeout-ms"].as<int32_t>()),
                std::ref(client_cache))
              .get();
            serv.start(scfg).get();
            auto dserv = seastar::defer([&serv] { serv.stop().get(); });
            tronlog.info("registering service on all cores");
            simple_shard_lookup shard_table;
            serv
              .invoke_on_all([&shard_table, &group_manager](rpc::server& s) {
                  s.register_service<raft::tron::service<
                    simple_group_manager,
                    simple_shard_lookup>>(
                    default_scheduling_group(),
                    default_smp_service_group(),
                    group_manager,
                    shard_table);
                  s.register_service<
                    raft::service<simple_group_manager, simple_shard_lookup>>(
                    default_scheduling_group(),
                    default_smp_service_group(),
                    group_manager,
                    shard_table);
              })
              .get();
            tronlog.info("Invoking rpc start on all cores");
            serv.invoke_on_all(&rpc::server::start).get();
            tronlog.info("Starting group manager");
            group_manager
              .invoke_on_all([&cfg](simple_group_manager& m) {
                  return m.start(group_cfg_from_args(cfg));
              })
              .get();
            app_signal.wait().get();
        });
    });
}
