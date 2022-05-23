/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "outcome.h"
#include "rpc/backoff_policy.h"
#include "rpc/transport.h"
#include "rpc/types.h"
#include "ssx/sformat.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/reactor.hh>
#include <seastar/net/socket_defs.hh>

namespace rpc {
class reconnect_transport {
public:
    explicit reconnect_transport(
      rpc::transport_configuration c, backoff_policy backoff_policy)
      : _transport(std::move(c))
      , _backoff_policy(std::move(backoff_policy)) {}

    bool is_valid() const { return _transport.is_valid(); }

    ss::future<result<rpc::transport*>> reconnect(clock_type::time_point);
    ss::future<result<rpc::transport*>> reconnect(clock_type::duration);

    rpc::transport& get() { return _transport; }

    /// safe client connect - attempts to reconnect if not connected
    ss::future<result<transport*>> get_connected(clock_type::time_point);
    ss::future<result<transport*>> get_connected(clock_type::duration);

    const net::unresolved_address& server_address() const {
        return _transport.server_address();
    }

    void reset_backoff() { _backoff_policy.reset(); }

    ss::future<> stop();

private:
    rpc::transport _transport;
    rpc::clock_type::time_point _stamp{rpc::clock_type::now()};
    ss::semaphore _connected_sem{1};
    ss::gate _dispatch_gate;
    backoff_policy _backoff_policy;
};
} // namespace rpc
