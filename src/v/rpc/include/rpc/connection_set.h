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

#pragma once

#include "base/outcome.h"
#include "base/outcome_future_utils.h"
#include "config/tls_config.h"
#include "model/metadata.h"
#include "rpc/backoff_policy.h"
#include "rpc/errc.h"
#include "rpc/reconnect_transport.h"
#include "rpc/types.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/flat_hash_map.h>

#include <chrono>
#include <unordered_map>

namespace rpc {

class connection_set {
public:
    using transport_ptr = ss::lw_shared_ptr<rpc::reconnect_transport>;

    explicit connection_set(
      std::optional<connection_cache_label> label = std::nullopt)
      : _label(std::move(label)) {}

    static rpc::backoff_policy default_backoff_policy() {
        return rpc::make_exponential_backoff_policy<rpc::clock_type>(
          std::chrono::seconds(1), std::chrono::seconds(15));
    }

    /// \brief either add or update a connection to a node.
    ss::future<> try_add_or_update(
      model::node_id node,
      net::unresolved_address rpc_address,
      config::tls_config tls_config,
      rpc::backoff_policy backoff = default_backoff_policy());

    /// \brief removes the node *and* closes the connection
    ss::future<> remove(model::node_id n);

    transport_ptr get(model::node_id n) const {
        return _connections.find(n)->second;
    }

    bool contains(model::node_id n) const {
        return _connections.find(n) != _connections.cend();
    }

    void emplace(model::node_id n, transport_ptr tp) {
        _connections.emplace(n, std::move(tp));
    }

    void emplace(
      model::node_id n,
      rpc::transport_configuration c,
      backoff_policy backoff_policy) {
        auto tp = ss::make_lw_shared<rpc::reconnect_transport>(
          std::move(c), std::move(backoff_policy), _label, n);
        emplace(n, std::move(tp));
    }

    /// \brief similar to remove but removes all nodes.
    ss::future<> remove_all();

    transport_version get_default_transport_version() {
        return _default_transport_version;
    }

    void set_default_transport_version(transport_version v) {
        _default_transport_version = v;
    }

    template<typename Protocol, typename Func>
    requires requires(Func&& f, Protocol proto) { f(proto); }
    auto with_node_client(
      model::node_id node_id, timeout_spec connection_timeout, Func&& f) {
        using ret_t = result_wrap_t<std::invoke_result_t<Func, Protocol>>;
        auto conn_it = _connections.find(node_id);

        if (conn_it == _connections.end()) {
            // No client available
            return ss::futurize<ret_t>::convert(
              rpc::make_error_code(errc::missing_node_rpc_client));
        }

        return ss::do_with(
          conn_it->second,
          [connection_timeout = connection_timeout.timeout_at(),
           f = std::forward<Func>(f)](auto& transport_ptr) mutable {
              return transport_ptr->get_connected(connection_timeout)
                .then([f = std::forward<Func>(f)](
                        result<ss::lw_shared_ptr<rpc::transport>>
                          transport) mutable {
                    if (!transport) {
                        // Connection error
                        return ss::futurize<ret_t>::convert(transport.error());
                    }
                    return ss::futurize<ret_t>::convert(
                      f(Protocol(transport.value())));
                });
          });
    }

    template<typename Protocol, typename Func, RpcDurationOrPoint Timeout>
    requires requires(Func&& f, Protocol proto) { f(proto); }
    auto with_node_client(
      model::node_id node_id, Timeout connection_timeout, Func&& f) {
        return with_node_client<Protocol, Func>(
          node_id,
          timeout_spec::from_either(connection_timeout),
          std::forward<Func>(f));
    }

    /// If a reconnect_transport is in a backed-off state, reset
    /// it so that the next RPC will be dispatched.  This is useful
    /// when a down node comes back to life: the first time we see
    /// a message from a re-awakened peer, we reset their backoff.
    void reset_client_backoff(model::node_id node_id);

private:
    using underlying = std::unordered_map<model::node_id, transport_ptr>;

    underlying _connections;
    transport_version _default_transport_version{transport_version::v2};
    std::optional<connection_cache_label> _label;
};

} // namespace rpc
