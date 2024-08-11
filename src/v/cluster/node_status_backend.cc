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
#include "cluster/node_status_rpc_service.h"
#include "cluster/node_status_table.h"
#include "config/node_config.h"
#include "model/metadata.h"
#include "rpc/types.h"
#include "ssx/future-util.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/util/defer.hh>

#include <bits/types/clock_t.h>

#include <exception>

namespace cluster {

node_status_backend::node_status_backend(
  model::node_id self,
  ss::sharded<members_table>& members_table,
  ss::sharded<features::feature_table>& feature_table,
  ss::sharded<node_status_table>& node_status_table,
  config::binding<std::chrono::milliseconds> period,
  config::binding<std::chrono::milliseconds> max_reconnect_backoff,
  ss::sharded<ss::abort_source>& as)
  : _self(self)
  , _members_table(members_table)
  , _feature_table(feature_table)
  , _node_status_table(node_status_table)
  , _period(std::move(period))
  , _max_reconnect_backoff(std::move(max_reconnect_backoff))
  , _rpc_tls_config(config::node().rpc_server_tls())
  , _node_connection_set(rpc::connection_cache_label{"node_status_backend"}) {
    if (!config::shard_local_cfg().disable_public_metrics()) {
        setup_metrics(_public_metrics);
    }

    if (!config::shard_local_cfg().disable_metrics()) {
        setup_metrics(_metrics);
    }

    _members_table_notification_handle
      = _members_table.local().register_members_updated_notification(
        [this](model::node_id n, model::membership_state state) {
            _pending_member_notifications.emplace_back(n, state);
            if (!_draining) {
                _draining = true;
                ssx::spawn_with_gate(
                  _gate, [this] { return drain_notifications_queue(); });
            }
        });

    _max_reconnect_backoff.watch([this]() {
        ssx::spawn_with_gate(_gate, [this] {
            auto new_backoff = _max_reconnect_backoff();
            vlog(
              clusterlog.info,
              "Updating max reconnect backoff to: {}ms",
              new_backoff.count());
            // Invalidate all transports so they are created afresh with new
            // backoff.
            return _node_connection_set.remove_all();
        });
    });

    _timer.set_callback([this] { tick(); });

    _as_subscription = as.local().subscribe([this]() mutable noexcept {
        ssx::spawn_with_gate(
          _gate, [this] { return _node_connection_set.remove_all(); });
    });
}

ss::future<> node_status_backend::drain_notifications_queue() {
    auto deferred = ss::defer([this] { _draining = false; });
    while (!_pending_member_notifications.empty()) {
        auto& notification = _pending_member_notifications.front();
        co_await handle_members_updated_notification(
          notification.id, notification.state);
        _pending_member_notifications.pop_front();
    }
    co_return;
}

ss::future<> node_status_backend::handle_members_updated_notification(
  model::node_id node_id, model::membership_state state) {
    switch (state) {
    case model::membership_state::active:
    case model::membership_state::draining:
        if (node_id != _self && !_discovered_peers.contains(node_id)) {
            vlog(
              clusterlog.info,
              "Node {} has been discovered via members table",
              node_id);
            _discovered_peers.insert(node_id);
            // update node status table with initial state
            co_await _node_status_table.invoke_on_all(
              [node_id](node_status_table& table) {
                  table.update_peers({node_status{
                    .node_id = node_id,
                    .last_seen = rpc::clock_type::now(),
                  }});
              });
        }
        break;
    case model::membership_state::removed:
        if (_discovered_peers.contains(node_id)) {
            vlog(
              clusterlog.info,
              "Node {} has been removed via members table",
              node_id);
            _discovered_peers.erase(node_id);

            co_await _node_status_table.invoke_on_all(
              [node_id](node_status_table& table) {
                  table.remove_peer(node_id);
              });
        }
        break;
    }
}

ss::future<> node_status_backend::start() {
    vassert(ss::this_shard_id() == shard, "invoked on a wrong shard");
    _timer.rearm(ss::lowres_clock::now());

    return ss::now();
}

ss::future<> node_status_backend::stop() {
    vassert(ss::this_shard_id() == shard, "invoked on a wrong shard");

    _metrics.clear();
    _public_metrics.clear();

    _members_table.local().unregister_members_updated_notification(
      _members_table_notification_handle);
    _timer.cancel();
    return _gate.close().then(
      [this] { return _node_connection_set.remove_all(); });
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

void node_status_backend::reset_node_backoff(model::node_id id) {
    vlog(clusterlog.debug, "Resetting reconnect backoff for node: {}", id);
    _node_connection_set.reset_client_backoff(id);
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

    co_await maybe_create_client(target, nm->get().broker.rpc_address());

    // auto send_by = rpc::clock_type::now() + _period();
    auto opts = rpc::client_opts(_period());
    auto timeout = opts.timeout;
    auto reply
      = co_await _node_connection_set
          .with_node_client<node_status_rpc_client_protocol>(
            target,
            timeout,
            [opts = std::move(opts), r = std::move(r)](auto client) mutable {
                return client.node_status(std::move(r), std::move(opts));
            })
          .then(&rpc::get_ctx_data<node_status_reply>);

    co_return process_reply(target, reply);
}

ss::future<> node_status_backend::maybe_create_client(
  model::node_id target, net::unresolved_address address) {
    co_await _node_connection_set.try_add_or_update(
      target, address, _rpc_tls_config, create_backoff_policy());
}

result<node_status> node_status_backend::process_reply(
  model::node_id target_node_id, result<node_status_reply> reply) {
    vassert(ss::this_shard_id() == shard, "invoked on a wrong shard");
    static constexpr auto rate_limit = std::chrono::seconds(1);
    if (reply.has_error()) {
        auto err = reply.error();
        if (
          err.category() == rpc::error_category()
          && static_cast<rpc::errc>(err.value())
               == rpc::errc::client_request_timeout) {
            _stats.rpcs_timed_out += 1;
        }
        static ss::logger::rate_limit rate(rate_limit);
        clusterlog.log(
          ss::log_level::debug,
          rate,
          "Error occurred while sending node status request: {}",
          err.message());
        return err;
    }

    _stats.rpcs_sent += 1;
    auto& replier_metadata = reply.value().replier_metadata;
    if (replier_metadata.node_id != target_node_id) {
        static ss::logger::rate_limit rate(rate_limit);
        clusterlog.log(
          ss::log_level::debug,
          rate,
          "Received reply from node with different node id. Expected: {}, "
          "current: {}",
          target_node_id,
          replier_metadata.node_id);
        return errc::invalid_target_node_id;
    }

    return node_status{
      .node_id = replier_metadata.node_id, .last_seen = rpc::clock_type::now()};
}

ss::future<node_status_reply>
node_status_backend::process_request(node_status_request request) {
    _stats.rpcs_received += 1;

    auto sender = request.sender_metadata.node_id;
    auto sender_md = _node_status_table.local().get_node_status(sender);
    if (
      sender_md
      // Check if the peer has atleast 2 missed heart beats. This avoids
      // a cross shard invoke in happy path when no reset is needed.
      && ss::lowres_clock::now() - sender_md->last_seen > 2 * _period()) {
        _node_connection_set.reset_client_backoff(sender);
    }

    co_return node_status_reply{.replier_metadata = {.node_id = _self}};
}

void node_status_backend::setup_metrics(
  metrics::metric_groups_base& metric_groups) {
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
