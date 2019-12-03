#pragma once

#include "raft/raftgen_service.h"
#include "rpc/types.h"

#include <seastar/core/gate.hh>
#include <seastar/core/reactor.hh>

namespace raft {
class reconnect_client {
public:
    using client_type = raftgen_service::client;
    class disconnected_client_exception final : std::exception {
    public:
        explicit disconnected_client_exception(sstring host)
          : _host(std::move(host)) {}

        const char* what() const noexcept final { return _host.c_str(); }

    private:
        sstring _host;
    };
    static inline uint32_t next_backoff(uint32_t current_backoff) {
        return std::min<uint32_t>(
          300, std::max<uint32_t>(1, current_backoff) << 1);
    }

    explicit reconnect_client(rpc::client_configuration c)
      : _client(std::move(c)) {}

    /// \brief perform Func inside the gate which ensures that the client will
    /// not go away until the Func() finishes
    /// returns an exceptional future of disconnected_client_exception when
    /// remote endpoint is not present
    template<typename Func>
    auto with_client(Func&& f) {
        if (_client.is_valid()) {
            return f(_client);
        }
        return reconnect().then([this, f = std::forward<Func>(f)]() mutable {
            return with_gate(
              _dispatch_gate, [this, f = std::forward<Func>(f)]() mutable {
                  return f(_client);
              });
        });
    }

    future<> stop();

private:
    future<> reconnect();

    raftgen_service::client _client;
    rpc::clock_type::time_point _stamp{rpc::clock_type::now()};
    semaphore _connected_sem{1};
    uint32_t _backoff_secs{0};
    gate _dispatch_gate;
};
static inline std::ostream& operator<<(
  std::ostream& o, const reconnect_client::disconnected_client_exception& e) {
    return o << "reconnect_client::disconnected_client_exception(" << e.what()
             << ")";
}
} // namespace raft
