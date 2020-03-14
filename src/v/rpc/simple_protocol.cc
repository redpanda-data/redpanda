#include "rpc/simple_protocol.h"

#include "rpc/logger.h"
#include "rpc/types.h"

namespace rpc {
struct server_context_impl final : streaming_context {
    server_context_impl(server::resources s, header h)
      : _s(std::move(s))
      , _h(h) {}
    ss::future<ss::semaphore_units<>> reserve_memory(size_t ask) final {
        auto fut = get_units(_s.memory(), ask);
        if (_s.memory().waiters()) {
            _s.probe().waiting_for_available_memory();
        }
        return fut;
    }
    const header& get_header() const final { return _h; }
    void signal_body_parse() final { pr.set_value(); }
    server::resources _s;
    header _h;
    ss::promise<> pr;
};

ss::future<> simple_protocol::apply(server::resources rs) {
    return ss::do_until(
      [this, rs] { return rs.conn->input().eof() || rs.abort_requested(); },
      [this, rs]() mutable {
          return parse_header(rs.conn->input())
            .then([this, rs](std::optional<header> h) mutable {
                if (!h) {
                    rpclog.debug(
                      "could not parse header from client: {}", rs.conn->addr);
                    rs.probe().header_corrupted();
                    return ss::make_ready_future<>();
                }
                return dispatch_method_once(h.value(), rs);
            });
      });
}

ss::future<>
simple_protocol::dispatch_method_once(header h, server::resources rs) {
    const auto method_id = h.meta;
    auto ctx = ss::make_lw_shared<server_context_impl>(rs, h);
    auto it = std::find_if(
      _services.begin(),
      _services.end(),
      [method_id](std::unique_ptr<service>& srvc) {
          return srvc->method_from_id(method_id) != nullptr;
      });
    if (unlikely(it == _services.end())) {
        rs.probe().method_not_found();
        throw std::runtime_error(
          fmt::format("received invalid rpc request: {}", h));
    }
    auto fut = ctx->pr.get_future();
    method* m = it->get()->method_from_id(method_id);
    rs.probe().add_bytes_received(size_of_rpc_header + h.payload_size);
    // background!
    if (rs.conn_gate().is_closed()) {
        return ss::make_exception_future<>(ss::gate_closed_exception());
    }
    (void)with_gate(rs.conn_gate(), [this, rs, ctx, m]() mutable {
        return (*m)(rs.conn->input(), *ctx)
          .then(
            [this, ctx, rs, m = rs.hist().auto_measure()](netbuf n) mutable {
                n.set_correlation_id(ctx->get_header().correlation_id);
                auto view = std::move(n).as_scattered();
                if (rs.conn_gate().is_closed()) {
                    // do not write if gate is closed
                    rpclog.debug(
                      "Skipping write of {} bytes, connection is closed",
                      view.size());
                    return ss::make_ready_future<>();
                }
                return rs.conn->write(std::move(view))
                  .finally([m = std::move(m), ctx] {});
            })
          .finally([rs]() mutable { rs.probe().request_completed(); });
    });
    return fut;
}
} // namespace rpc
