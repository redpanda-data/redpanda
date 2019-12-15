#pragma once

#include "outcome.h"
#include "raft/raftgen_service.h"
#include "rpc/types.h"

#include <seastar/core/gate.hh>
#include <seastar/core/reactor.hh>
#include <seastar/net/socket_defs.hh>

namespace raft {
class reconnect_client {
public:
    using client_type = rpc::client<raftgen_client_protocol>;
    static inline uint32_t next_backoff(uint32_t current_backoff) {
        return std::min<uint32_t>(
          300, std::max<uint32_t>(1, current_backoff) << 1);
    }

    explicit reconnect_client(rpc::client_configuration c)
      : _client(std::move(c)) {}

    bool is_valid() const { return _client.is_valid(); }

    future<result<client_type*>> reconnect();

    client_type& get() { return _client; }

    /// safe client connect - attempts to reconnect if not connected
 future<result<client_type*>> get_connected();

    const socket_address& server_address() const {
        return _client.server_address();
    }

    future<> stop();

private:
    client_type _client;
    rpc::clock_type::time_point _stamp{rpc::clock_type::now()};
    semaphore _connected_sem{1};
    uint32_t _backoff_secs{0};
    gate _dispatch_gate;
};
} // namespace raft
