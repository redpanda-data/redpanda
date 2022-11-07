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
#include "utils/directory_walker.h"

#include <seastar/core/seastar.hh>

#include <chrono>

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
  , _storage(storage)
  , _as(as) {}

ss::future<node_id> cluster_discovery::determine_node_id() {
    // Initialize cluster founder state, in case we are starting a new cluster.
    // This will validate our configured node ID if we are a cluster founder.
    bool is_founder = co_await is_cluster_founder();
    if (is_founder) {
        vassert(
          config::node().node_id().has_value(),
          "initializing founder state should have set the local node ID");
        co_return *config::node().node_id();
    }
    if (config::node().node_id() != std::nullopt) {
        // If we're not a founder but we already have a node ID, just return.
        // We will send it to the controller when we join the cluster.
        co_return *config::node().node_id();
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

cluster_discovery::brokers cluster_discovery::founding_brokers() const {
    vassert(
      _is_cluster_founder.has_value(), "must call discover_founding_brokers()");
    vassert(
      _founding_brokers.empty() == !*_is_cluster_founder,
      "Should return broker(s) if and only if this node is founding a new "
      "cluster");
    return _founding_brokers;
}

ss::future<bool> cluster_discovery::is_cluster_founder() {
    if (_is_cluster_founder.has_value()) {
        co_return *_is_cluster_founder;
    }
    // If there's anything in the controller directory, assume this node has
    // previously joined a cluster.
    auto controller_ntp_cfg = storage::ntp_config(
      model::controller_ntp, config::node().data_directory().as_sstring());
    const auto controller_dir = controller_ntp_cfg.work_directory();
    auto controller_dir_exists = co_await ss::file_exists(controller_dir);
    if (controller_dir_exists) {
        const auto controller_empty = co_await directory_walker::empty(
          std::filesystem::path(controller_dir));
        if (!controller_empty) {
            vlog(
              clusterlog.info,
              "Controller directory {} not empty; assuming existing cluster "
              "exists",
              controller_dir);
            _is_cluster_founder = false;
            co_return *_is_cluster_founder;
        }
    }
    if (config::node().empty_seed_starts_cluster()) {
        // When using root-driven bootstrap, only the node with the empty seed
        // servers list is a founder.
        if (!config::node().seed_servers().empty()) {
            _is_cluster_founder = false;
            co_return *_is_cluster_founder;
        }

        // If there is no node ID, assign one automatically.
        if (!config::node().node_id().has_value()) {
            co_await ss::smp::invoke_on_all([] {
                config::node().node_id.set_value(
                  std::make_optional(model::node_id(0)));
            });
        }
        _founding_brokers = brokers{make_self_broker(config::node())};
        _is_cluster_founder = true;
        co_return *_is_cluster_founder;
    }
    co_await discover_founding_brokers();
    vassert(_is_cluster_founder.has_value(), "must initialize founder state");
    co_return *_is_cluster_founder;
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
  net::unresolved_address addr) const {
    vlog(clusterlog.info, "Requesting cluster bootstrap info from {}", addr);
    _as.check();
    auto repeat_jitter = simple_time_jitter<model::timeout_clock>(1s);
    while (true) {
        result<cluster_bootstrap_info_reply> reply_result(
          std::errc::connection_refused);
        if (_is_cluster_founder.has_value()) {
            // Another fiber detected the presence of a cluster. Just exit
            // early.
            vassert(
              !*_is_cluster_founder,
              "We can only detect the presence of a cluster (indicating we are "
              "not a founder) early, not the absence");
            co_return reply_result.value();
        }
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
              "Cluster bootstrap info failed from {} with {}",
              addr,
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

// Search for the current node's advertised RPC address in the seed_servers.
// Precondition: emtpy_seed_starts_cluster=false
// \return Index of this node in seed_servers list if found, or empty if
// not.
// \throw runtime_error if seed_servers is empty
std::optional<int> get_node_index_in_seed_servers() {
    const std::vector<config::seed_server>& seed_servers
      = config::node().seed_servers();
    if (seed_servers.empty()) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Configuration error: seed_servers cannot be empty when "
          "empty_seed_starts_cluster is false"));
    }
    int idx = 0;
    for (const auto& ss : seed_servers) {
        if (ss.addr == config::node().advertised_rpc_api()) {
            return idx;
        }
        ++idx;
    }
    return std::nullopt;
}

} // anonymous namespace

ss::future<> cluster_discovery::discover_founding_brokers() {
    vassert(!_is_cluster_founder.has_value(), "fetching bootstrap info again");
    auto seed_idx = get_node_index_in_seed_servers();
    if (seed_idx == std::nullopt) {
        _is_cluster_founder = false;
        co_return;
    }
    if (_storage.get_cluster_uuid().has_value()) {
        _is_cluster_founder = false;
        co_return;
    }

    absl::flat_hash_map<net::unresolved_address, cluster_bootstrap_info_reply>
      replies;
    const net::unresolved_address& self_addr
      = config::node().advertised_rpc_api();
    const vector<config::seed_server>& config_seed_servers
      = config::node().seed_servers();
    for (const auto& seed_server : config_seed_servers) {
        if (seed_server.addr == self_addr) {
            continue;
        }
        replies.emplace(seed_server.addr, cluster_bootstrap_info_reply{});
    }
    co_await ss::parallel_for_each(
      replies, [this, &replies](auto& entry) mutable {
          return request_cluster_bootstrap_info_single(entry.first)
            .then([this, &replies, addr = entry.first](auto reply) mutable {
                if (reply.cluster_uuid.has_value()) {
                    vlog(
                      clusterlog.info,
                      "Cluster presence detected in other seed servers: {}",
                      *reply.cluster_uuid);
                    _is_cluster_founder = false;
                }
                replies[addr] = std::move(reply);
            });
      });
    if (_is_cluster_founder.has_value()) {
        vassert(
          !*_is_cluster_founder,
          "We can only detect the presence of a cluster (indicating we are "
          "not a founder) early, not the absence");
        co_return;
    }
    // Make sure we didn't exit the above calls without a reply.
    _as.check();

    // At this point, assuming the seed replies are consistent with ours, we
    // are a cluster founder.
    ss::sstring error_msg;
    brokers seed_brokers;
    seed_brokers.reserve(config_seed_servers.size());
    if (!config::node().node_id().has_value()) {
        clusterlog.info("Using index based node ID {}", seed_idx);
        co_await ss::smp::invoke_on_all([&] {
            config::node().node_id.set_value(
              std::optional<model::node_id>(seed_idx));
        });
    }
    bool failed = false;
    for (auto& seed : config_seed_servers) {
        if (seed.addr == self_addr) {
            seed_brokers.emplace_back(make_self_broker(config::node()));
            continue;
        }
        auto& reply = replies[seed.addr];
        vassert(!reply.cluster_uuid.has_value(), "Should have returned early");
        if (
          reply.version
          != features::feature_table::get_latest_logical_version()) {
            vlog(
              clusterlog.error,
              "logical version mismatch, local: {}, {}: {}",
              features::feature_table::get_latest_logical_version(),
              seed.addr,
              reply.version);
            failed = true;
        }
        if (!equal(reply.seed_servers, config_seed_servers)) {
            vlog(
              clusterlog.error,
              "seed server list mismatch, local: {}, {}: {}",
              config_seed_servers,
              seed.addr,
              reply.seed_servers);
            failed = true;
        }
        if (
          reply.empty_seed_starts_cluster
          != config::node().empty_seed_starts_cluster()) {
            vlog(
              clusterlog.error,
              "empty_seed_starts_cluster mismatch, local: {}, {}: {}",
              config::node().empty_seed_starts_cluster(),
              seed.addr,
              reply.empty_seed_starts_cluster);
            failed = true;
        }
        seed_brokers.emplace_back(std::move(reply.broker));
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
    // identical configuration.
    model::node_id::type idx = 0;
    absl::flat_hash_set<model::node_id> unique_node_ids;
    for (auto& broker : seed_brokers) {
        if (broker.id() == model::unassigned_node_id) {
            broker.replace_unassigned_node_id(model::node_id(idx));
        }
        unique_node_ids.emplace(broker.id());
        ++idx;
    }
    if (unique_node_ids.size() != seed_brokers.size()) {
        throw std::runtime_error(
          fmt_with_ctx(fmt::format, "Duplicate node IDs: {}", seed_brokers));
    }
    _founding_brokers = std::move(seed_brokers);
    _is_cluster_founder = true;
}

} // namespace cluster
