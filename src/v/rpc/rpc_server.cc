// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "rpc/rpc_server.h"

#include "config/configuration.h"
#include "rpc/logger.h"
#include "rpc/types.h"
#include "ssx/semaphore.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>

#include <exception>

namespace rpc {

/*
 * the size threshold above which a reply message will use compression.
 */
static constexpr size_t reply_min_compression_bytes = 1024;

struct server_context_impl final : streaming_context {
    server_context_impl(
      net::server& server, ss::lw_shared_ptr<net::connection> conn, header h)
      : server(server)
      , conn(conn)
      , hdr(h) {
        server.probe().request_received();
    }
    ss::future<ssx::semaphore_units> reserve_memory(size_t ask) final {
        auto fut = get_units(server.memory(), ask);
        if (server.memory().waiters()) {
            server.probe().waiting_for_available_memory();
        }
        return fut;
    }
    ~server_context_impl() override { server.probe().request_completed(); }
    const header& get_header() const final { return hdr; }
    void signal_body_parse() final { pr.set_value(); }
    void body_parse_exception(std::exception_ptr e) final {
        pr.set_exception(std::move(e));
    }
    net::server& server;
    ss::lw_shared_ptr<net::connection> conn;
    header hdr;
    ss::promise<> pr;
};

ss::future<> rpc_server::apply(ss::lw_shared_ptr<net::connection> conn) {
    return ss::do_until(
      [this, conn] { return conn->input().eof() || abort_requested(); },
      [this, conn]() mutable {
          return parse_header(conn->input())
            .then([this, conn](std::optional<header> h) mutable {
                if (!h) {
                    rpclog.debug(
                      "could not parse header from client: {}", conn->addr);
                    probe().header_corrupted();
                    // Have to shutdown the connection as data in receiving
                    // buffer may be corrupted
                    conn->shutdown_input();
                    return ss::now();
                }
                return dispatch_method_once(h.value(), conn);
            });
      });
}

ss::future<>
rpc_server::send_reply(ss::lw_shared_ptr<server_context_impl> ctx, netbuf buf) {
    if (config::shard_local_cfg().rpc_server_compress_replies()) {
        buf.set_min_compression_bytes(reply_min_compression_bytes);
        buf.set_compression(rpc::compression_type::zstd);
    }
    buf.set_correlation_id(ctx->get_header().correlation_id);

    auto view = co_await std::move(buf).as_scattered();
    if (conn_gate().is_closed()) {
        // do not write if gate is closed
        rpclog.debug(
          "Skipping write of {} bytes, connection is closed", view.size());
        co_return;
    }
    try {
        co_await ctx->conn->write(std::move(view));
    } catch (...) {
        auto e = std::current_exception();
        auto disconnect = net::is_disconnect_exception(e);
        if (disconnect) {
            vlog(
              rpclog.info,
              "Disconnected {} ({})",
              ctx->conn->addr,
              disconnect.value());
        } else {
            vlog(rpclog.warn, "Error dispatching method: {}", e);
        }
        ctx->conn->shutdown_input();
    }
}

ss::future<> rpc_server::send_reply_skip_payload(
  ss::lw_shared_ptr<server_context_impl> ctx, netbuf buf) {
    co_await ctx->conn->input().skip(ctx->get_header().payload_size);
    co_await send_reply(std::move(ctx), std::move(buf));
}

ss::future<> rpc_server::dispatch_method_once(
  header h, ss::lw_shared_ptr<net::connection> conn) {
    const auto method_id = h.meta;
    auto ctx = ss::make_lw_shared<server_context_impl>(*this, conn, h);
    probe().add_bytes_received(size_of_rpc_header + h.payload_size);
    if (conn_gate().is_closed()) {
        return ss::make_exception_future<>(ss::gate_closed_exception());
    }

    auto fut = ctx->pr.get_future();

    // background!
    ssx::background
      = ssx::spawn_with_gate_then(
          conn_gate(),
          [this, method_id, ctx]() mutable {
              if (unlikely(
                    ctx->get_header().version
                    > transport_version::max_supported)) {
                  vlog(
                    rpclog.debug,
                    "Received a request at an unsupported transport version {} "
                    "> {} from {}",
                    ctx->get_header().version,
                    transport_version::max_supported,
                    ctx->conn->addr);
                  netbuf reply_buf;
                  reply_buf.set_status(rpc::status::version_not_supported);
                  /*
                   * client is not expected to react to max_supported being
                   * returned, but it may be useful in future scenarios for the
                   * client to know more about the state of the server.
                   */
                  reply_buf.set_version(transport_version::max_supported);
                  return send_reply_skip_payload(ctx, std::move(reply_buf))
                    .then_wrapped([ctx](ss::future<> f) {
                        // If the connection is closed then this can be an
                        // exceptional future.
                        if (f.failed()) {
                            ctx->body_parse_exception(f.get_exception());
                        } else {
                            ctx->signal_body_parse();
                        }
                    });
              }
              auto it = std::find_if(
                _services.begin(),
                _services.end(),
                [method_id](std::unique_ptr<service>& srvc) {
                    return srvc->method_from_id(method_id) != nullptr;
                });
              if (unlikely(it == _services.end())) {
                  ss::sstring msg_suffix;
                  rpc::status s = rpc::status::method_not_found;
                  if (!_all_services_added && _service_unavailable_allowed) {
                      msg_suffix = " during startup. Ignoring...";
                      s = rpc::status::service_unavailable;
                  }
                  vlog(
                    rpclog.debug,
                    "Received a request for an unknown method {} from {}{}",
                    method_id,
                    ctx->conn->addr,
                    msg_suffix);
                  probe().method_not_found();
                  netbuf reply_buf;
                  reply_buf.set_version(ctx->get_header().version);
                  reply_buf.set_status(s);
                  return send_reply_skip_payload(ctx, std::move(reply_buf))
                    .then_wrapped([ctx](ss::future<> f) {
                        // If the connection is closed then this can be an
                        // exceptional future.
                        if (f.failed()) {
                            ctx->body_parse_exception(f.get_exception());
                        } else {
                            ctx->signal_body_parse();
                        }
                    });
              }

              method* m = it->get()->method_from_id(method_id);

              return m->handle(ctx->conn->input(), *ctx)
                .then_wrapped([this,
                               ctx,
                               m,
                               method_id,
                               l = hist().auto_measure()](
                                ss::future<netbuf> fut) mutable {
                    bool error = true;
                    netbuf reply_buf;
                    try {
                        reply_buf = fut.get();
                        reply_buf.set_status(rpc::status::success);
                        error = false;
                    } catch (const rpc_internal_body_parsing_exception& e) {
                        // We have to distinguish between exceptions thrown by
                        // the service handler and the one caused by the
                        // corrupted payload. Data corruption on the wire may
                        // lead to the situation where connection is not longer
                        // usable and so it have to be terminated.
                        ctx->pr.set_exception(e);
                        return ss::now();
                    } catch (const ss::timed_out_error& e) {
                        vlog(
                          rpclog.debug,
                          "Timing out request on timed_out_error "
                          "(shutting down)");
                        reply_buf.set_status(rpc::status::request_timeout);
                    } catch (const ss::condition_variable_timed_out& e) {
                        vlog(
                          rpclog.debug,
                          "Timing out request on condition_variable_timed_out");
                        reply_buf.set_status(rpc::status::request_timeout);
                    } catch (const ss::gate_closed_exception& e) {
                        // gate_closed is typical during shutdown.  Treat
                        // it like a timeout: request was not erroneous
                        // but we will not give a rseponse.
                        vlog(
                          rpclog.debug,
                          "Timing out request on gate_closed_exception "
                          "(shutting down)");
                        reply_buf.set_status(rpc::status::request_timeout);
                    } catch (const ss::broken_condition_variable& e) {
                        vlog(
                          rpclog.debug,
                          "Timing out request on broken_condition_variable "
                          "(shutting down)");
                        reply_buf.set_status(rpc::status::request_timeout);
                    } catch (const ss::abort_requested_exception& e) {
                        vlog(
                          rpclog.debug,
                          "Timing out request on abort_requested_exception "
                          "(shutting down)");
                        reply_buf.set_status(rpc::status::request_timeout);
                    } catch (const ss::broken_semaphore& e) {
                        vlog(
                          rpclog.debug,
                          "Timing out request on broken_semaphore "
                          "(shutting down)");
                        reply_buf.set_status(rpc::status::request_timeout);
                    } catch (...) {
                        vlog(
                          rpclog.error,
                          "Service handler for method {} threw an exception: "
                          "{}",
                          method_id,
                          std::current_exception());
                        probe().service_error();
                        reply_buf.set_status(rpc::status::server_error);
                    }
                    if (error) {
                        /*
                         * when an exception occurs while processing a request
                         * reply_buf is sent back to the client as a reply with
                         * a status/error code and no body. the version needs to
                         * also be set to match the request version to avoid the
                         * client complaining about an unexpected version.
                         *
                         * in the error free case the handler will provide a
                         * fully defined (with version) netbuf which replaces
                         * reply_buf (see try block above).
                         */
                        reply_buf.set_version(ctx->get_header().version);
                    }
                    return send_reply(ctx, std::move(reply_buf))
                      .finally([m, l = std::move(l)]() mutable {
                          m->probes.latency_hist().record(
                            l->compute_total_latency().count());
                      });
                });
          })
          .handle_exception([](const std::exception_ptr& e) {
              vlog(rpclog.error, "Error dispatching: {}", e);
          });

    return fut;
}
} // namespace rpc
