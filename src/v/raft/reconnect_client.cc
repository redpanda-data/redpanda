#include "raft/reconnect_client.h"

#include "raft/errc.h"
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

future<result<reconnect_client::client_type*>>
reconnect_client::get_connected() {
    if (is_valid()) {
        return make_ready_future<result<client_type*>>(&_client);
    }
    return reconnect();
}

future<result<reconnect_client::client_type*>> reconnect_client::reconnect() {
    using ret_t = result<reconnect_client::client_type*>;
    if (!has_backoff_expired(_stamp, _backoff_secs)) {
        return make_ready_future<ret_t>(errc::exponential_backoff);
    }
    _stamp = rpc::clock_type::now();
    return with_gate(_dispatch_gate, [this] {
        return with_semaphore(_connected_sem, 1, [this] {
            if (is_valid()) {
                return make_ready_future<ret_t>(&_client);
            }
            return _client.connect().then_wrapped([this](future<> f) {
                try {
                    f.get();
                    raftlog.debug("connected to {}", _client.server_address());
                    _backoff_secs = 0;
                    return make_ready_future<ret_t>(&_client);
                } catch (...) {
                    _backoff_secs = next_backoff(_backoff_secs);
                    raftlog.trace(
                      "error reconnecting {}", std::current_exception());
                    return make_ready_future<ret_t>(
                      errc::disconnected_endpoint);
                }
            });
        });
    });
}
} // namespace raft
