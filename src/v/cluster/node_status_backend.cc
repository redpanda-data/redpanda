/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/node_status_backend.h"

#include "cluster/cluster_utils.h"
#include "cluster/errc.h"
#include "cluster/logger.h"
#include "config/node_config.h"
#include "ssx/future-util.h"

namespace cluster {

node_status_backend::node_status_backend(
  model::node_id self,
  ss::sharded<members_table>& members_table,
  ss::sharded<features::feature_table>& feature_table,
  ss::sharded<node_status_table>& node_status_table,
  config::binding<std::chrono::milliseconds> period)
  : _self(self)
  , _members_table(members_table)
  , _feature_table(feature_table)
  , _node_status_table(node_status_table)
  , _period(std::move(period))
  , _rpc_tls_config(config::node().rpc_server_tls()) {
    if (!config::shard_local_cfg().disable_public_metrics()) {
        setup_metrics(_public_metrics);
    }

    if (!config::shard_local_cfg().disable_metrics()) {
        setup_metrics(_metrics);
    }

    _members_table_notification_handle
      = _members_table.local().register_members_updated_notification(
        [this](auto ids) {
            handle_members_updated_notification(std::move(ids));
        });

    _timer.set_callback([this] { tick(); });
}

void node_status_backend::handle_members_updated_notification(
  std::vector<model::node_id> node_ids) {
    for (const auto& node_id : node_ids) {
        if (node_id == _self || _discovered_peers.contains(node_id)) {
            continue;
        }

        vlog(
          clusterlog.info,
          "Node {} has been discovered via members table",
          node_id);
        _discovered_peers.insert(node_id);
    }
}

ss::future<> node_status_backend::start() {
    vassert(ss::this_shard_id() == shard, "invoked on a wrong shard");

    co_await _node_connection_cache.start(
      rpc::connection_cache_label{"node_status_backend"});

    _timer.rearm(ss::lowres_clock::now());
}

ss::future<> node_status_backend::stop() {
    vassert(ss::this_shard_id() == shard, "invoked on a wrong shard");

    _metrics.clear();
    _public_metrics.clear();

    _members_table.local().unregister_members_updated_notification(
      _members_table_notification_handle);
    _timer.cancel();
    return _gate.close().then([this] { return _node_connection_cache.stop(); });
}

void node_status_backend::tick() {
    ssx::background = ssx::spawn_with_gate_then(_gate, [this] {
                          return collect_and_store_updates().finally([this] {
                              if (!_gate.is_closed()) {
                                  _timer.rearm(
                                    ss::lowres_clock::now() + _period());
                              }
                          });
                      }).handle_exception([](const std::exception_ptr& e) {
        vlog(
          clusterlog.debug,
          "Exception during node status provider tick: {}",
          e);
    });
}

ss::future<> node_status_backend::collect_and_store_updates() {
    if (!_feature_table.local().is_active(
          features::feature::raftless_node_status)) {
        co_return;
    }

    auto updates = co_await collect_updates_from_peers();
    co_return co_await _node_status_table.invoke_on_all(
      [updates = std::move(updates)](auto& table) {
          table.update_peers(updates);
      });
}

ss::future<std::vector<node_status>>
node_status_backend::collect_updates_from_peers() {
    node_status_request request = {.sender_metadata = {.node_id = _self}};

    auto results = co_await ssx::parallel_transform(
      _discovered_peers.begin(),
      _discovered_peers.end(),
      [this, request = std::move(request)](auto peer_id) {
          return send_node_status_request(peer_id, request);
      });

    std::vector<node_status> updates;
    updates.reserve(results.size());

    for (auto& result : results) {
        if (!result.has_error()) {
            updates.push_back(std::move(result.value()));
        }
    }

    co_return updates;
}

ss::future<result<node_status>> node_status_backend::send_node_status_request(
  model::node_id target, node_status_request r) {
    auto gate_holder = _gate.hold();

    auto nm = _members_table.local().get_node_metadata_ref(target);
    if (!nm) {
        co_return make_error_code(errc::node_does_not_exists);
    }

    auto connection_source_shard = co_await maybe_create_client(
      target, nm->get().broker.rpc_address());

    // auto send_by = rpc::clock_type::now() + _period();
    auto opts = rpc::client_opts(_period());
    auto reply
      = co_await _node_connection_cache.local()
          .with_node_client<node_status_rpc_client_protocol>(
            _self,
            connection_source_shard,
            target,
            opts.timeout,
            [opts = std::move(opts), r = std::move(r)](auto client) mutable {
                return client.node_status(std::move(r), std::move(opts));
            })
          .then(&rpc::get_ctx_data<node_status_reply>);

    co_return process_reply(reply);
}

ss::future<ss::shard_id> node_status_backend::maybe_create_client(
  model::node_id target, net::unresolved_address address) {
    auto source_shard = target % ss::smp::count;
    auto target_shard = rpc::connection_cache::shard_for(
      _self, source_shard, target);

    co_await add_one_tcp_client(
      target_shard, _node_connection_cache, target, address, _rpc_tls_config);

    co_return source_shard;
}

result<node_status>
node_status_backend::process_reply(result<node_status_reply> reply) {
    vassert(ss::this_shard_id() == shard, "invoked on a wrong shard");

    if (!reply.has_error()) {
        _stats.rpcs_sent += 1;
        auto& replier_metadata = reply.value().replier_metadata;

        return node_status{
          .node_id = replier_metadata.node_id,
          .last_seen = rpc::clock_type::now()};
    } else {
        auto err = reply.error();
        if (
          err.category() == rpc::error_category()
          && static_cast<rpc::errc>(err.value())
               == rpc::errc::client_request_timeout) {
            _stats.rpcs_timed_out += 1;
        }
        static constexpr auto rate_limit = std::chrono::seconds(1);
        static ss::logger::rate_limit rate(rate_limit);
        clusterlog.log(
          ss::log_level::debug,
          rate,
          "Error occurred while sending node status request: {}",
          err.message());
        return err;
    }
}

ss::future<node_status_reply>
node_status_backend::process_request(node_status_request) {
    _stats.rpcs_received += 1;

    node_status_reply reply = {.replier_metadata = {.node_id = _self}};
    return ss::make_ready_future<node_status_reply>(std::move(reply));
}

void node_status_backend::setup_metrics(
  ss::metrics::metric_groups& metric_groups) {
    namespace sm = ss::metrics;

    metric_groups.add_group(
      prometheus_sanitize::metrics_name("node_status"),
      {
        sm::make_gauge(
          "rpcs_sent",
          [this] { return _stats.rpcs_sent; },
          sm::description("Number of node status RPCs sent by this node"))
          .aggregate({sm::shard_label}),
        sm::make_gauge(
          "rpcs_timed_out",
          [this] { return _stats.rpcs_timed_out; },
          sm::description(
            "Number of timed out node status RPCs from this node"))
          .aggregate({sm::shard_label}),
        sm::make_gauge(
          "rpcs_received",
          [this] { return _stats.rpcs_received; },
          sm::description("Number of node status RPCs received by this node"))
          .aggregate({sm::shard_label}),
      });
}

} // namespace cluster
