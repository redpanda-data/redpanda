/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "rpc/connection_set.h"

#include "config/configuration.h"
#include "rpc/rpc_utils.h"

namespace rpc {
ss::future<> connection_set::try_add_or_update(
  model::node_id node,
  net::unresolved_address rpc_address,
  config::tls_config tls_config,
  rpc::backoff_policy backoff) {
    auto connection = _connections.find(node);
    if (connection != _connections.end()) {
        // client is already there, check if configuration changed
        if (connection->second->server_address() == rpc_address) {
            // If configuration did not changed, do nothing
            co_return;
        }
        // configuration changed, first remove the client
        co_await remove(node);
    }

    auto cert_creds = co_await maybe_build_reloadable_certificate_credentials(
      std::move(tls_config));
    auto config = rpc::transport_configuration{
      .server_addr = std::move(rpc_address),
      .credentials = std::move(cert_creds),
      .disable_metrics = net::metrics_disabled(
        config::shard_local_cfg().disable_metrics),
      .version = get_default_transport_version()};
    auto trans = ss::make_lw_shared<rpc::reconnect_transport>(
      std::move(config), std::move(backoff), _label, node);

    _connections.emplace(node, std::move(trans));
}

ss::future<> connection_set::remove(model::node_id n) {
    auto it = _connections.find(n);
    if (it == _connections.end()) {
        co_return;
    }
    auto ptr = it->second;
    _connections.erase(it);

    if (!ptr) {
        co_return;
    }

    co_await ptr->stop();
}

ss::future<> connection_set::remove_all() {
    auto connections = std::exchange(_connections, {});
    co_await parallel_for_each(connections, [](auto& it) {
        auto& [_, cli] = it;
        return cli->stop();
    });
    connections.clear();
}

void connection_set::reset_client_backoff(model::node_id node_id) {
    auto conn_it = _connections.find(node_id);

    if (conn_it == _connections.end()) {
        // No client available
        return;
    }
    auto recon_transport = conn_it->second;
    recon_transport->reset_backoff();
}

} // namespace rpc
