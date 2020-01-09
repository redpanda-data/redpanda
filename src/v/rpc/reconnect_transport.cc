#include "rpc/reconnect_transport.h"

#include "raft/logger.h"
#include "rpc/errc.h"
#include "rpc/logger.h"
#include "rpc/transport.h"

#include <seastar/net/inet_address.hh>

#include <chrono>
#include <functional>

namespace rpc {
namespace ch = std::chrono; // NOLINT

static inline bool
has_backoff_expired(rpc::clock_type::time_point stamp, int32_t backoff) {
    auto now = rpc::clock_type::now();
    if (now < stamp) {
        return false;
    }
    auto const secs = ch::duration_cast<ch::seconds>(now - stamp).count();
    return secs >= backoff;
}

ss::future<> reconnect_transport::stop() {
    _backoff_secs = std::numeric_limits<uint32_t>::max();
    return _dispatch_gate.close().then([this] { return _transport.stop(); });
}

ss::future<result<transport*>> reconnect_transport::get_connected() {
    if (is_valid()) {
        return ss::make_ready_future<result<transport*>>(&_transport);
    }
    return reconnect();
}

ss::future<result<transport*>> reconnect_transport::reconnect() {
    using ret_t = result<transport*>;
    if (!has_backoff_expired(_stamp, _backoff_secs)) {
        return ss::make_ready_future<ret_t>(errc::exponential_backoff);
    }
    _stamp = rpc::clock_type::now();
    return with_gate(_dispatch_gate, [this] {
        return with_semaphore(_connected_sem, 1, [this] {
            if (is_valid()) {
                return ss::make_ready_future<ret_t>(&_transport);
            }
            return _transport.connect().then_wrapped([this](ss::future<> f) {
                try {
                    f.get();
                    rpclog.debug(
                      "connected to {}", _transport.server_address());
                    _backoff_secs = 0;
                    return ss::make_ready_future<ret_t>(&_transport);
                } catch (...) {
                    _backoff_secs = next_backoff(_backoff_secs);
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
