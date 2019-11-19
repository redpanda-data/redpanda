#include "raft/reconnect_client.h"

#include "raft/logger.h"

#include <seastar/net/inet_address.hh>

#include <chrono>
#include <functional>

namespace raft {
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
future<> reconnect_client::stop() {
    _backoff_secs = std::numeric_limits<uint32_t>::max();
    return _dispatch_gate.close().then([this] { return _client.stop(); });
}

future<> reconnect_client::reconnect() {
    if (!has_backoff_expired(_stamp, _backoff_secs)) {
        return make_exception_future<>(disconnected_client_exception());
    }
    _stamp = rpc::clock_type::now();
    return with_gate(_dispatch_gate, [this] {
        return with_semaphore(_connected_sem, 1, [this] {
            if (_client.is_valid()) {
                return make_ready_future<>();
            }
            return _client.connect().then_wrapped([this](future<> f) {
                try {
                    f.get();
                    raftlog.debug(
                      "Successfully connected to {}:{}",
                      _client.cfg.server_addr.addr(),
                      _client.cfg.server_addr.port());
                    _backoff_secs = 0;
                    return make_ready_future<>();
                } catch (...) {
                    _backoff_secs = next_backoff(_backoff_secs);
                    raftlog.debug(
                      "Error reconnecting: {}", std::current_exception());
                    // keep the exception interface consistent
                    return make_exception_future<>(
                      disconnected_client_exception());
                }
            });
        });
    });
}
} // namespace raft
