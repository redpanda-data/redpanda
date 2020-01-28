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

    explicit reconnect_transport(
      rpc::transport_configuration c, clock_type::duration backoff_step)
      : _transport(std::move(c))
      , _backoff_step(backoff_step) {}

    bool is_valid() const { return _transport.is_valid(); }

    ss::future<result<rpc::transport*>> reconnect();

    rpc::transport& get() { return _transport; }

    /// safe client connect - attempts to reconnect if not connected
    ss::future<result<transport*>> get_connected();

    const ss::socket_address& server_address() const {
        return _transport.server_address();
    }

    ss::future<> stop();

private:
    rpc::transport _transport;
    rpc::clock_type::time_point _stamp{rpc::clock_type::now()};
    ss::semaphore _connected_sem{1};
    // backoff multiplier is a number that will be grown exponentially
    uint32_t _backoff_multiplier{0};
    ss::gate _dispatch_gate;

    // backoff step is a duration that multiplied by backoff multiplier will
    // result in effective backoff timeout
    // for example for backoff step equal to 1 second the backoffs sequence will
    // be following:
    // 1s,2s,4s,8s,16s,...
    // for backoff step equals 200ms it will be:
    // 200ms, 400ms, 1600ms, 3200ms,...
    clock_type::duration _backoff_step;
};
} // namespace rpc
