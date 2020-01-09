#pragma once

#include "rpc/batched_output_stream.h"
#include "rpc/client_probe.h"
#include "rpc/netbuf.h"
#include "rpc/parse_utils.h"
#include "rpc/response_handler.h"
#include "rpc/types.h"
#include "seastarx.h"

#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/net/api.hh>

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <utility>

namespace rpc {
class client_context_impl;

class base_transport {
public:
    struct configuration {
        ss::socket_address server_addr;
        std::optional<ss::tls::credentials_builder> credentials;
    };

    explicit base_transport(configuration c);
    virtual ~base_transport() = default;
    base_transport(base_transport&&) = default;

    virtual ss::future<> connect();
    ss::future<> stop();
    void shutdown();

    [[gnu::always_inline]] bool is_valid() const { return _fd && !_in.eof(); }

    const ss::socket_address& server_address() const { return _server_addr; }

protected:
    virtual void fail_outstanding_futures() {}

    ss::input_stream<char> _in;
    batched_output_stream _out;
    ss::gate _dispatch_gate;
    client_probe _probe;

private:
    ss::future<> do_connect();

    std::unique_ptr<ss::connected_socket> _fd;
    ss::socket_address _server_addr;
    ss::shared_ptr<ss::tls::certificate_credentials> _creds;
};

class transport final : public base_transport {
public:
    explicit transport(
      transport_configuration c,
      std::optional<ss::sstring> service_name = std::nullopt);
    transport(transport&&) = default;
    ~transport() override;
    ss::future<> connect() final;
    ss::future<std::unique_ptr<streaming_context>>
      send(netbuf, rpc::timer_type::time_point);

    template<typename Input, typename Output>
    ss::future<client_context<Output>> send_typed(
      Input, uint32_t, rpc::timer_type::time_point = rpc::no_timeout);

private:
    friend client_context_impl;

    ss::future<> do_reads();
    ss::future<> dispatch(header);
    void fail_outstanding_futures() final;
    void setup_metrics(const std::optional<ss::sstring>&);

    ss::semaphore _memory;
    std::unordered_map<uint32_t, internal::response_handler> _correlations;
    uint32_t _correlation_idx{0};
    ss::metrics::metric_groups _metrics;
};

template<typename Input, typename Output>
inline ss::future<client_context<Output>> transport::send_typed(
  Input r, uint32_t method_id, rpc::timer_type::time_point timeout) {
    auto b = rpc::netbuf();
    b.serialize_type(std::move(r));
    b.set_service_method_id(method_id);
    return send(std::move(b), timeout)
      .then([this](std::unique_ptr<streaming_context> sctx) mutable {
          return parse_type<Output>(_in, sctx->get_header())
            .then([sctx = std::move(sctx)](Output o) {
                sctx->signal_body_parse();
                using ctx_t = rpc::client_context<Output>;
                // TODO - don't copy the header
                ctx_t ctx(sctx->get_header());
                std::swap(ctx.data, o);
                return ss::make_ready_future<ctx_t>(std::move(ctx));
            });
      });
}

// clang-format off
CONCEPT(
template<typename Protocol>
concept RpcClientProtocol = requires (rpc::transport& t) {
    { Protocol(t) } -> Protocol;
};
)
// clang-format on

template<typename... Protocol>
CONCEPT(requires(RpcClientProtocol<Protocol>&&...))
class client : public Protocol... {
public:
    explicit client(transport_configuration cfg)
      : _transport(std::move(cfg))
      , Protocol(_transport)... {}

    ss::future<> connect() { return _transport.connect(); }
    ss::future<> stop() { return _transport.stop(); };
    void shutdown() { _transport.shutdown(); }

    [[gnu::always_inline]] bool is_valid() const {
        return _transport.is_valid();
    }

    const ss::socket_address& server_address() const {
        return _transport.server_address();
    }

private:
    rpc::transport _transport;
};

} // namespace rpc
