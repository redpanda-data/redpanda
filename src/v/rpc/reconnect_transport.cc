// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "rpc/reconnect_transport.h"

#include "model/timeout_clock.h"
#include "raft/logger.h"
#include "rpc/errc.h"
#include "rpc/logger.h"
#include "rpc/transport.h"
#include "rpc/types.h"

#include <seastar/net/inet_address.hh>

#include <chrono>
#include <functional>

namespace rpc {
namespace ch = std::chrono; // NOLINT

static inline bool has_backoff_expired(
  rpc::clock_type::time_point stamp, clock_type::duration backoff) {
    auto now = rpc::clock_type::now();
    if (now < stamp) {
        return false;
    }
    return now >= (stamp + backoff);
}

ss::future<> reconnect_transport::stop() {
    return _dispatch_gate.close().then([this] { return _transport.stop(); });
}
ss::future<result<transport*>>
reconnect_transport::get_connected(clock_type::duration connection_timeout) {
    return get_connected(clock_type::now() + connection_timeout);
}

ss::future<result<transport*>>
reconnect_transport::get_connected(clock_type::time_point connection_timeout) {
    if (is_valid()) {
        return ss::make_ready_future<result<transport*>>(&_transport);
    }
    return reconnect(connection_timeout);
}

ss::future<result<rpc::transport*>>
reconnect_transport::reconnect(clock_type::duration connection_timeout) {
    return reconnect(clock_type::now() + connection_timeout);
}

ss::future<result<transport*>>
reconnect_transport::reconnect(clock_type::time_point connection_timeout) {
    using ret_t = result<transport*>;
    if (!has_backoff_expired(
          _stamp, _backoff_policy.current_backoff_duration())) {
        return ss::make_ready_future<ret_t>(errc::exponential_backoff);
    }

    auto now = rpc::clock_type::now();
    if (now > connection_timeout) {
        return ss::make_ready_future<ret_t>(errc::client_request_timeout);
    }
    auto connection_timeout_duration = connection_timeout - now;

    _stamp = now;
    return with_gate(
      _dispatch_gate, [this, connection_timeout, connection_timeout_duration] {
          return with_semaphore(
            _connected_sem,
            1,
            connection_timeout_duration,
            [this, connection_timeout] {
                if (is_valid()) {
                    return ss::make_ready_future<ret_t>(&_transport);
                }
                vlog(
                  rpclog.trace,
                  "connecting to {}",
                  _transport.server_address());
                return _transport.connect(connection_timeout)
                  .then_wrapped([this](ss::future<> f) {
                      try {
                          f.get();
                          rpclog.debug(
                            "connected to {}", _transport.server_address());
                          _backoff_policy.reset();
                          return ss::make_ready_future<ret_t>(&_transport);
                      } catch (...) {
                          _backoff_policy.next_backoff();
                          rpclog.trace(
                            "error reconnecting {}", std::current_exception());
                          return ss::make_ready_future<ret_t>(
                            errc::disconnected_endpoint);
                      }
                  });
            });
      });
}
} // namespace rpc
