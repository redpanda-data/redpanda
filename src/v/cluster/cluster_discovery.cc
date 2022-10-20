// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/cluster_discovery.h"

#include "cluster/bootstrap_types.h"
#include "cluster/cluster_bootstrap_service.h"
#include "cluster/cluster_utils.h"
#include "cluster/controller_service.h"
#include "cluster/logger.h"
#include "config/node_config.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "seastarx.h"
#include "storage/kvstore.h"

#include <chrono>

using model::broker;
using model::node_id;
using std::vector;

namespace cluster {

cluster_discovery::cluster_discovery(
  const model::node_uuid& node_uuid,
  storage::api& storage,
  ss::abort_source& as)
  : _node_uuid(node_uuid)
  , _join_retry_jitter(config::shard_local_cfg().join_retry_timeout_ms())
  , _join_timeout(std::chrono::seconds(2))
  , _has_stored_cluster_uuid(storage.get_cluster_uuid().has_value())
  , _kvstore(storage.kvs())
  , _as(as) {}

ss::future<node_id> cluster_discovery::determine_node_id() {
    // TODO: read from disk if empty.
    const auto& configured_node_id = config::node().node_id();
    if (configured_node_id != std::nullopt) {
        clusterlog.info("Using configured node ID {}", configured_node_id);
        co_return *configured_node_id;
    }
    static const bytes invariants_key("configuration_invariants");
    auto invariants_buf = _kvstore.get(
      storage::kvstore::key_space::controller, invariants_key);
    if (invariants_buf) {
        auto invariants = reflection::from_iobuf<configuration_invariants>(
          std::move(*invariants_buf));
        co_return invariants.node_id;
    }

    if (auto cf_node_id = co_await get_cluster_founder_node_id(); cf_node_id) {
        clusterlog.info("Using index based node ID {}", *cf_node_id);
        co_return *cf_node_id;
    }
    model::node_id assigned_node_id;
    while (!_as.abort_requested()) {
        auto assigned = co_await dispatch_node_uuid_registration_to_seeds(
          assigned_node_id);
        if (assigned) {
            break;
        }
        co_await ss::sleep_abortable(_join_retry_jitter.next_duration(), _as);
    }
    co_return assigned_node_id;
}

namespace {

/**
 * Search for the current node's advertised RPC address in the seed_servers.
 * Precondition: emtpy_seed_starts_cluster=false
 * \return Index of this node in seed_servers list if found, or empty if
 * not.
 * \throw runtime_error if seed_servers is empty
 */
std::optional<model::node_id::type> get_node_index_in_seed_servers() {
    const std::vector<config::seed_server>& seed_servers
      = config::node().seed_servers();
    if (seed_servers.empty()) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Configuration error: seed_servers cannot be empty when "
          "empty_seed_starts_cluster is false"));
    }
    const auto it = std::find_if(
      seed_servers.cbegin(),
      seed_servers.cend(),
      [rpc_addr = config::node().advertised_rpc_api()](
        const config::seed_server& seed_server) {
          return rpc_addr == seed_server.addr;
      });
    if (it == seed_servers.cend()) {
        return {};
    }
    return {boost::numeric_cast<model::node_id::type>(
      std::distance(seed_servers.cbegin(), it))};
}

} // namespace

ss::future<cluster_discovery::brokers>
cluster_discovery::initial_seed_brokers() {
    // If configured as the root node, we'll want to start the cluster with
    // just this node as the initial seed.
    if (config::node().empty_seed_starts_cluster()) {
        if (config::node().seed_servers().empty()) {
            co_return brokers{make_self_broker(config::node())};
        }
        // Not a root
        co_return brokers{};
    }
    if (get_node_index_in_seed_servers()) {
        co_await fetch_cluster_bootstrap_info();
        brokers seed_brokers = get_seed_brokers_for_bootstrap();
        seed_brokers.push_back(make_self_broker(config::node()));
        co_return std::move(seed_brokers);
    }
    // Non-seed server
    co_return brokers{};
}

ss::future<cluster_discovery::brokers>
cluster_discovery::initial_seed_brokers_if_no_cluster() {
    if (!co_await is_cluster_founder()) {
        co_return brokers{};
    }
    co_return co_await initial_seed_brokers();
}

ss::future<bool> cluster_discovery::is_cluster_founder() {
    if (_has_stored_cluster_uuid) {
        co_return false;
    }
    const std::optional<model::node_id> cluster_founder_node_id
      = co_await get_cluster_founder_node_id();
    co_return cluster_founder_node_id.has_value();
}

ss::future<bool> cluster_discovery::dispatch_node_uuid_registration_to_seeds(
  model::node_id& assigned_node_id) {
    const auto& seed_servers = config::node().seed_servers();
    auto self = make_self_broker(config::node());
    for (const auto& s : seed_servers) {
        vlog(
          clusterlog.info,
          "Requesting node ID for node UUID {} from {}",
          _node_uuid,
          s.addr);
        result<join_node_reply> r(join_node_reply{});
        try {
            r = co_await do_with_client_one_shot<controller_client_protocol>(
              s.addr,
              config::node().rpc_server_tls(),
              _join_timeout,
              [&self, this](controller_client_protocol c) {
                  return c
                    .join_node(
                      join_node_request(
                        features::feature_table::get_latest_logical_version(),
                        _node_uuid().to_vector(),
                        self),
                      rpc::client_opts(rpc::clock_type::now() + _join_timeout))
                    .then(&rpc::get_ctx_data<join_node_reply>);
              });
        } catch (...) {
            vlog(
              clusterlog.debug,
              "Error registering node UUID {}, retrying: {}",
              _node_uuid,
              std::current_exception());
            continue;
        }
        if (r.has_error()) {
            vlog(
              clusterlog.debug,
              "Error registering UUID {}: {}, retrying",
              _node_uuid,
              r.error());
            continue;
        }
        if (!r.has_value() || !r.value().success) {
            vlog(
              clusterlog.debug,
              "Error registering node UUID {}, retrying",
              _node_uuid);
            continue;
        }
        const auto& reply = r.value();
        if (reply.id < 0) {
            // Something else went wrong. Maybe duplicate UUID?
            vlog(clusterlog.debug, "Negative node ID {}", reply.id);
            continue;
        }
        assigned_node_id = reply.id;
        co_return true;
    }
    co_return false;
}

ss::future<cluster_bootstrap_info_reply>
cluster_discovery::request_cluster_bootstrap_info_single(
  const net::unresolved_address addr) const {
    vlog(clusterlog.info, "Requesting cluster bootstrap info from {}", addr);
    _as.check();
    for (auto repeat_jitter = simple_time_jitter<model::timeout_clock>(1s);;) {
        result<cluster_bootstrap_info_reply> reply_result(
          std::errc::connection_refused);
        try {
            reply_result = co_await do_with_client_one_shot<
              cluster_bootstrap_client_protocol>(
              addr,
              config::node().rpc_server_tls(),
              2s,
              [](cluster_bootstrap_client_protocol c) {
                  return c
                    .cluster_bootstrap_info(
                      cluster_bootstrap_info_request{},
                      rpc::client_opts(rpc::clock_type::now() + 2s))
                    .then(&rpc::get_ctx_data<cluster_bootstrap_info_reply>);
              });
            if (reply_result) {
                vlog(
                  clusterlog.info,
                  "Obtained cluster bootstrap info from {}",
                  addr);
                vlog(clusterlog.debug, "{}", reply_result.value());
                co_return std::move(reply_result.value());
            }
            vlog(
              clusterlog.debug,
              "Cluster bootstrap info failed with {}",
              reply_result.error());
        } catch (...) {
            vlog(
              clusterlog.warn,
              "Error requesting cluster bootstrap info from {}, retrying. {}",
              addr,
              std::current_exception());
        }
        co_await ss::sleep_abortable(repeat_jitter.next_duration(), _as);
        vlog(clusterlog.trace, "Retrying cluster bootstrap info from {}", addr);
    }
}

ss::future<> cluster_discovery::fetch_cluster_bootstrap_info() {
    if (_cluster_bootstrap_info_fetched) {
        co_return;
    }
    const auto units = co_await _fetch_cluster_bootstrap_mx.get_units(_as);
    if (_cluster_bootstrap_info_fetched) {
        co_return;
    }

    const net::unresolved_address& self_addr
      = config::node().advertised_rpc_api();
    const std::vector<config::seed_server>& self_seed_servers
      = config::node().seed_servers();

    std::vector<
      std::pair<net::unresolved_address, cluster_bootstrap_info_reply>>
      peers;
    peers.reserve(self_seed_servers.size());
    for (const config::seed_server& seed_server : self_seed_servers) {
        // do not call oneself
        if (seed_server.addr == self_addr) {
            continue;
        }
        peers.emplace_back(seed_server.addr, cluster_bootstrap_info_reply{});
    }
    co_await ss::parallel_for_each(peers, [this](auto& peer) -> ss::future<> {
        peer.second = co_await request_cluster_bootstrap_info_single(
          peer.first);
        co_return;
    });
    _cluster_bootstrap_info = std::move(peers);
    _cluster_bootstrap_info_fetched = true;
}

namespace {

bool equal(
  const std::vector<net::unresolved_address>& lhs,
  const std::vector<config::seed_server>& rhs) {
    return std::equal(
      lhs.cbegin(),
      lhs.cend(),
      rhs.cbegin(),
      rhs.cend(),
      [](const net::unresolved_address& lhs, const config::seed_server& rhs) {
          return lhs == rhs.addr;
      });
}

void check_configuration_match(
  bool& failure,
  const net::unresolved_address& peer_addr,
  const cluster_bootstrap_info_reply& peer_reply) {
    const std::vector<config::seed_server>& self_seed_servers
      = config::node().seed_servers();

    if (!equal(peer_reply.seed_servers, self_seed_servers)) {
        vlog(
          clusterlog.error,
          "Cluster configuration error: seed server list mismatch, "
          "local: "
          "{}, {}: {}",
          self_seed_servers,
          peer_addr,
          peer_reply.seed_servers);
        failure = true;
    }
    if (
      peer_reply.empty_seed_starts_cluster
      != config::node().empty_seed_starts_cluster()) {
        vlog(
          clusterlog.error,
          "Cluster configuration error: empty_seed_starts_cluster "
          "mismatch, local: {}, {}: {}",
          config::node().empty_seed_starts_cluster(),
          peer_addr,
          peer_reply.empty_seed_starts_cluster);
        failure = true;
    }
}

} // namespace

cluster_discovery::brokers
cluster_discovery::get_seed_brokers_for_bootstrap() const {
    vassert(
      _cluster_bootstrap_info_fetched,
      "Cluster bootstrap info not fetched yet");

    // Validate the returned replies and populate the brokers vector
    bool failed = false;
    brokers seed_brokers;
    seed_brokers.reserve(_cluster_bootstrap_info.size());
    for (const auto& peer : _cluster_bootstrap_info) {
        if (
          peer.second.version
          != features::feature_table::get_latest_logical_version()) {
            vlog(
              clusterlog.error,
              "Cluster setup error: logical version mismatch, local: {}, {}: "
              "{}",
              features::feature_table::get_latest_logical_version(),
              peer.first,
              peer.second.version);
            failed = true;
        }
        if (peer.second.cluster_uuid.has_value()) {
            vlog(
              clusterlog.error,
              "Cluster setup error: peer is already a member of cluster {}",
              *peer.second.cluster_uuid);
            failed = true;
        }
        check_configuration_match(failed, peer.first, peer.second);
        seed_brokers.push_back(peer.second.broker);
    }
    if (failed) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Cannot bootstrap a cluster due to seed servers mismatch, check "
          "the log above for details"));
    }

    // The other seeds will likely mostly all have -1 as their node_id if their
    // node_id was not set explicitly. Have the returned seed broker node_id
    // populated with indices, relying on the fact that all seed broker's have
    // identical configuration. Note that the order is preserved from
    // seed_servers to seed_brokers, but the latter is missing one item for
    // self.
    const std::vector<config::seed_server>& self_seed_servers
      = config::node().seed_servers();
    for (auto [ib, is]
         = std::tuple{seed_brokers.begin(), self_seed_servers.cbegin()};
         ib != seed_brokers.cend();
         ++ib) {
        while (is != self_seed_servers.cend()
               && is->addr != ib->rpc_address()) {
            ++is;
        }
        if (ib->id() == model::unassigned_node_id) {
            vassert(
              is != self_seed_servers.cend(),
              "seed_brokers and seed_servers mismatch");
            const auto idx = boost::numeric_cast<model::node_id::type>(
              std::distance(self_seed_servers.cbegin(), is));
            ib->replace_unassigned_node_id(model::node_id{idx});
        }
    }

    vlog(clusterlog.debug, "Seed brokers discovered: {}", seed_brokers);
    return seed_brokers;
}

ss::future<std::optional<node_id>>
cluster_discovery::get_cluster_founder_node_id() {
    if (config::node().empty_seed_starts_cluster()) {
        if (config::node().seed_servers().empty()) {
            co_return node_id{0};
        }
        co_return std::nullopt;
    } else {
        if (auto idx = get_node_index_in_seed_servers(); idx) {
            co_await fetch_cluster_bootstrap_info();
            if (remote_cluster_uuid_exists()) {
                vlog(
                  clusterlog.info,
                  "Cluster presence detected in other seed servers");
                co_return std::nullopt;
            } else {
                co_return node_id{*idx};
            }
        }
        co_return std::nullopt;
    }
}

bool cluster_discovery::remote_cluster_uuid_exists() const {
    vassert(
      _cluster_bootstrap_info_fetched,
      "Cluster bootstrap info not fetched yet");

    bool failed = false;
    std::optional<model::cluster_uuid> remote_cluster_uuid;
    for (auto& [peer_addr, peer_reply] : _cluster_bootstrap_info) {
        if (peer_reply.cluster_uuid) {
            if (remote_cluster_uuid) {
                if (*remote_cluster_uuid != *peer_reply.cluster_uuid) {
                    vlog(
                      clusterlog.error,
                      "Cluster setup error: peer seed servers belong to "
                      "different clusters: {} and {}",
                      *peer_reply.cluster_uuid,
                      *remote_cluster_uuid);
                    failed = true;
                }
            } else {
                remote_cluster_uuid = peer_reply.cluster_uuid;
            }
        }
        check_configuration_match(failed, peer_addr, peer_reply);
    }
    if (failed) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Cannot start the node due to seed servers mismatch, check the log "
          "above for details"));
    }
    return remote_cluster_uuid.has_value();
}

} // namespace cluster
