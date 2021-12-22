// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "rpc/simple_protocol.h"

#include "rpc/logger.h"
#include "rpc/types.h"

#include <seastar/core/future-util.hh>

#include <exception>

namespace rpc {
struct server_context_impl final : streaming_context {
    server_context_impl(server::resources s, header h)
      : res(std::move(s))
      , hdr(h) {
        res.probe().request_received();
    }
    ss::future<ss::semaphore_units<>> reserve_memory(size_t ask) final {
        auto fut = get_units(res.memory(), ask);
        if (res.memory().waiters()) {
            res.probe().waiting_for_available_memory();
        }
        return fut;
    }
    ~server_context_impl() override { res.probe().request_completed(); }
    const header& get_header() const final { return hdr; }
    void signal_body_parse() final { pr.set_value(); }
    void body_parse_exception(std::exception_ptr e) final {
        pr.set_exception(std::move(e));
    }
    server::resources res;
    header hdr;
    ss::promise<> pr;
};

ss::future<> simple_protocol::apply(server::resources rs) {
    return ss::do_until(
      [rs] { return rs.conn->input().eof() || rs.abort_requested(); },
      [this, rs]() mutable {
          return parse_header(rs.conn->input())
            .then([this, rs](std::optional<header> h) mutable {
                if (!h) {
                    rpclog.debug(
                      "could not parse header from client: {}", rs.conn->addr);
                    rs.probe().header_corrupted();
                    // Have to shutdown the connection as data in receiving
                    // buffer may be corrupted
                    rs.conn->shutdown_input();
                    return ss::now();
                }
                return dispatch_method_once(h.value(), rs);
            });
      });
}

ss::future<>
send_reply(ss::lw_shared_ptr<server_context_impl> ctx, netbuf buf) {
    buf.set_min_compression_bytes(1024);
    buf.set_compression(rpc::compression_type::zstd);
    buf.set_correlation_id(ctx->get_header().correlation_id);

    auto view = std::move(buf).as_scattered();
    if (ctx->res.conn_gate().is_closed()) {
        // do not write if gate is closed
        rpclog.debug(
          "Skipping write of {} bytes, connection is closed", view.size());
        return ss::make_ready_future<>();
    }
    return ctx->res.conn->write(std::move(view))
      .handle_exception([ctx](std::exception_ptr e) {
          vlog(rpclog.info, "Error dispatching method: {}", e);
          ctx->res.conn->shutdown_input();
      });
}

ss::future<>
simple_protocol::dispatch_method_once(header h, server::resources rs) {
    const auto method_id = h.meta;
    auto ctx = ss::make_lw_shared<server_context_impl>(rs, h);
    rs.probe().add_bytes_received(size_of_rpc_header + h.payload_size);
    if (rs.conn_gate().is_closed()) {
        return ss::make_exception_future<>(ss::gate_closed_exception());
    }

    auto fut = ctx->pr.get_future();

    // background!
    (void)with_gate(rs.conn_gate(), [this, method_id, rs, ctx]() mutable {
        auto it = std::find_if(
          _services.begin(),
          _services.end(),
          [method_id](std::unique_ptr<service>& srvc) {
              return srvc->method_from_id(method_id) != nullptr;
          });
        if (unlikely(it == _services.end())) {
            rs.probe().method_not_found();
            netbuf reply_buf;
            reply_buf.set_status(rpc::status::method_not_found);
            return send_reply(ctx, std::move(reply_buf)).then([ctx]() mutable {
                ctx->signal_body_parse();
            });
        }

        method* m = it->get()->method_from_id(method_id);

        return m->handle(ctx->res.conn->input(), *ctx)
          .then_wrapped([ctx, m, l = ctx->res.hist().auto_measure(), rs](
                          ss::future<netbuf> fut) mutable {
              netbuf reply_buf;
              try {
                  reply_buf = fut.get0();
                  reply_buf.set_status(rpc::status::success);
              } catch (const rpc_internal_body_parsing_exception& e) {
                  // We have to distinguish between exceptions thrown by the
                  // service handler and the one caused by the corrupted
                  // payload. Data corruption on the wire may lead to the
                  // situation where connection is not longer usable and so it
                  // have to be terminated.
                  ctx->pr.set_exception(e);
                  return ss::now();
              } catch (const ss::timed_out_error& e) {
                  reply_buf.set_status(rpc::status::request_timeout);
              } catch (const ss::gate_closed_exception& e) {
                  // gate_closed is typical during shutdown.  Treat
                  // it like a timeout: request was not erroneous
                  // but we will not give a rseponse.
                  rpclog.debug("Timing out request on gate_closed_exception "
                               "(shutting down)");
                  reply_buf.set_status(rpc::status::request_timeout);
              } catch (...) {
                  rpclog.error(
                    "Service handler threw an exception: {}",
                    std::current_exception());
                  rs.probe().service_error();
                  reply_buf.set_status(rpc::status::server_error);
              }
              return send_reply(ctx, std::move(reply_buf))
                .finally([m, l = std::move(l)]() mutable {
                    m->probes.latency_hist().record(std::move(l));
                });
          });
    });
    return fut;
}
} // namespace rpc
