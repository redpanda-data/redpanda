#pragma once

#include "outcome.h"
#include "rpc/transport.h"
#include "rpc/types.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/reactor.hh>
#include <seastar/net/socket_defs.hh>

namespace rpc {
class reconnect_transport {
public:
    static inline uint32_t next_backoff(uint32_t current_backoff) {
        return std::min<uint32_t>(
          300, std::max<uint32_t>(1, current_backoff) << 1);
    }

    explicit reconnect_transport(rpc::transport_configuration c)
      : _transport(std::move(c)) {}

    bool is_valid() const { return _transport.is_valid(); }

    future<result<rpc::transport*>> reconnect();

    rpc::transport& get() { return _transport; }

    /// safe client connect - attempts to reconnect if not connected
    future<result<transport*>> get_connected();

    const socket_address& server_address() const {
        return _transport.server_address();
    }

    future<> stop();

private:
    rpc::transport _transport;
    rpc::clock_type::time_point _stamp{rpc::clock_type::now()};
    semaphore _connected_sem{1};
    uint32_t _backoff_secs{0};
    gate _dispatch_gate;
};
} // namespace rpc
