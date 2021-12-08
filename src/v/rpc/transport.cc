// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "rpc/transport.h"

#include "likely.h"
#include "rpc/dns.h"
#include "rpc/logger.h"
#include "rpc/parse_utils.h"
#include "rpc/response_handler.h"
#include "rpc/types.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/net/api.hh>
#include <seastar/net/socket_defs.hh>

#include <memory>
#include <type_traits>

namespace rpc {
struct client_context_impl final : streaming_context {
    client_context_impl(transport& s, header h)
      : _c(std::ref(s))
      , _h(std::move(h)) {}
    ss::future<ss::semaphore_units<>> reserve_memory(size_t ask) final {
        auto fut = get_units(_c.get()._memory, ask);
        if (_c.get()._memory.waiters()) {
            _c.get()._probe.waiting_for_available_memory();
        }
        return fut;
    }
    const header& get_header() const final { return _h; }
    void signal_body_parse() final { pr.set_value(); }
    void body_parse_exception(std::exception_ptr e) final {
        pr.set_exception(std::move(e));
    }
    std::reference_wrapper<transport> _c;
    header _h;
    ss::promise<> pr;
};

base_transport::base_transport(configuration c)
  : _server_addr(c.server_addr)
  , _creds(c.credentials)
  , _tls_sni_hostname(c.tls_sni_hostname) {}

transport::transport(
  transport_configuration c,
  [[maybe_unused]] std::optional<ss::sstring> service_name)
  : base_transport(base_transport::configuration{
    .server_addr = std::move(c.server_addr),
    .credentials = std::move(c.credentials),
  })
  , _memory(c.max_queued_bytes) {
    if (!c.disable_metrics) {
        setup_metrics(service_name);
    }
}

ss::future<ss::connected_socket> connect_with_timeout(
  const seastar::socket_address& address, rpc::clock_type::time_point timeout) {
    auto socket = ss::make_lw_shared<ss::socket>(ss::engine().net().socket());
    auto f = socket->connect(address).finally([socket] {});
    return ss::with_timeout(timeout, std::move(f))
      .handle_exception([socket, address](const std::exception_ptr& e) {
          rpclog.trace("error connecting to {} - {}", address, e);
          socket->shutdown();
          return ss::make_exception_future<ss::connected_socket>(e);
      });
}

ss::future<> base_transport::do_connect(clock_type::time_point timeout) {
    // hold invariant of having an always valid dispatch gate
    // and make sure we don't have a live connection already
    if (is_valid() || _dispatch_gate.is_closed()) {
        throw std::runtime_error(fmt::format(
          "cannot do_connect with a valid connection. remote:{}",
          server_address()));
    }
    try {
        auto resolved_address = co_await resolve_dns(server_address());
        ss::connected_socket fd = co_await connect_with_timeout(
          resolved_address, timeout);

        if (_creds) {
            fd = co_await ss::tls::wrap_client(
              _creds,
              std::move(fd),
              _tls_sni_hostname ? *_tls_sni_hostname : ss::sstring{});
        }
        _fd = std::make_unique<ss::connected_socket>(std::move(fd));
        _probe.connection_established();
        _in = _fd->input();

        // Never implicitly destroy a live output stream here: output streams
        // are only safe to destroy after/during stop()
        vassert(!_out.is_valid(), "destroyed output_stream without stopping");
        _out = batched_output_stream(_fd->output());
    } catch (...) {
        auto e = std::current_exception();
        _probe.connection_error(e);
        std::rethrow_exception(e);
    }

    co_return;
}

ss::future<>
base_transport::connect(clock_type::time_point connection_timeout) {
    // in order to hold concurrency correctness invariants we must guarantee 3
    // things before we attempt to send a payload:
    // 1. there are no background futures waiting
    // 2. the _dispatch_gate() is open
    // 3. the connection is valid
    //
    return stop().then([this, connection_timeout] {
        _dispatch_gate = {};
        return do_connect(connection_timeout);
    });
}
ss::future<> base_transport::stop() {
    fail_outstanding_futures();

    return _dispatch_gate.close().then([this]() {
        // We must call stop() on our output stream, because
        // seastar::output_stream may not be safely destroyed without a call to
        // close(), and this class may be destroyed after stop() is called.
        return _out.stop().then_wrapped([this](ss::future<> f) {
            // Invalidate _out here, so that do_connect can assert that
            // it isn't dropping an un-stopped output stream when it
            // assigns to _out
            try {
                f.get();
            } catch (...) {
                // Closing the output stream can throw bad pipe if
                // it had unflushed bytes, as we already closed FD.
                vlog(
                  rpclog.debug,
                  "Exception while stopping transport: {}",
                  std::current_exception());
            }
            _out = {};
        });
    });
}
void transport::fail_outstanding_futures() noexcept {
    // must close the socket
    shutdown();
    for (auto& [_, p] : _correlations) {
        p->set_value(errc::disconnected_endpoint);
    }
    _last_seq = sequence_t{0};
    _seq = sequence_t{0};
    _requests_queue.clear();
    _correlations.clear();
}
void base_transport::shutdown() noexcept {
    try {
        if (_fd) {
            _fd->shutdown_input();
            _fd->shutdown_output();
            _fd.reset();
        }
    } catch (...) {
        vlog(
          rpclog.debug,
          "Failed to shutdown transport: {}",
          std::current_exception());
    }
}

ss::future<> transport::connect(clock_type::duration connection_timeout) {
    return connect(connection_timeout + rpc::clock_type::now());
}

ss::future<>
transport::connect(rpc::clock_type::time_point connection_timeout) {
    return base_transport::connect(connection_timeout).then([this] {
        _correlation_idx = 0;
        _last_seq = sequence_t{0};
        _seq = sequence_t{0};
        // background
        (void)ss::with_gate(_dispatch_gate, [this] {
            return do_reads().then_wrapped([this](ss::future<> f) {
                _probe.connection_closed();
                fail_outstanding_futures();
                try {
                    f.get();
                } catch (...) {
                    _probe.read_dispatch_error(std::current_exception());
                }
            });
        });
    });
}

ss::future<result<std::unique_ptr<streaming_context>>>
transport::send(netbuf b, rpc::client_opts opts) {
    return do_send(_seq++, std::move(b), std::move(opts));
}

ss::future<result<std::unique_ptr<streaming_context>>>
transport::make_response_handler(netbuf& b, const rpc::client_opts& opts) {
    if (_correlations.find(_correlation_idx + 1) != _correlations.end()) {
        _probe.client_correlation_error();
        throw std::runtime_error("Invalid transport state. Doubly "
                                 "registered correlation_id");
    }
    const uint32_t idx = ++_correlation_idx;
    auto item = std::make_unique<internal::response_handler>();
    auto item_raw_ptr = item.get();
    // capture the future _before_ inserting promise in the map
    // in case there is a concurrent error w/ the connection and it
    // fails the future before we return from this function
    auto response_future = item_raw_ptr->get_future();
    b.set_correlation_id(idx);
    auto [_, success] = _correlations.emplace(idx, std::move(item));
    if (unlikely(!success)) {
        throw std::logic_error(
          fmt::format("Tried to reuse correlation id: {}", idx));
    }
    item_raw_ptr->with_timeout(opts.timeout, [this, idx] {
        auto it = _correlations.find(idx);
        if (likely(it != _correlations.end())) {
            vlog(rpclog.info, "Request timeout, correlation id: {}", idx);
            _probe.request_timeout();
            _correlations.erase(it);
        }
    });

    return response_future;
}

ss::future<result<std::unique_ptr<streaming_context>>>
transport::do_send(sequence_t seq, netbuf b, rpc::client_opts opts) {
    using ret_t = result<std::unique_ptr<streaming_context>>;
    // hold invariant of always having a valid connection _and_ a working
    // dispatch gate where we can wait for async futures
    if (!is_valid() || _dispatch_gate.is_closed()) {
        _last_seq = std::max(_last_seq, seq);
        return ss::make_ready_future<ret_t>(errc::disconnected_endpoint);
    }
    return ss::with_gate(
      _dispatch_gate,
      [this, b = std::move(b), opts = std::move(opts), seq]() mutable {
          auto f = make_response_handler(b, opts);

          // send
          auto sz = b.buffer().size_bytes();
          return get_units(_memory, sz)
            .then([this,
                   b = std::move(b),
                   f = std::move(f),
                   seq,
                   u = std::move(opts.resource_units)](
                    ss::semaphore_units<> units) mutable {
                auto e = entry{
                  .buffer = std::make_unique<netbuf>(std::move(b)),
                  .resource_units = std::move(u),
                };

                _requests_queue.emplace(
                  seq, std::make_unique<entry>(std::move(e)));
                dispatch_send();
                return std::move(f).finally([u = std::move(units)] {});
            })
            .finally([this, seq] {
                // update last sequence to make progress, for successfull
                // dispatches this will be noop, as _last_seq was already update
                // before sending data
                _last_seq = std::max(_last_seq, seq);
            });
      });
}

void transport::dispatch_send() {
    (void)ss::with_gate(_dispatch_gate, [this]() mutable {
        return ss::do_until(
          [this] {
              return _requests_queue.empty()
                     || _requests_queue.begin()->first
                          > (_last_seq + sequence_t(1));
          },
          [this] {
              auto it = _requests_queue.begin();
              _last_seq = it->first;
              auto buffer = std::move(it->second->buffer).get();
              auto units = std::move(it->second->resource_units);
              auto v = std::move(*buffer).as_scattered();
              auto msg_size = v.size();
              _requests_queue.erase(it->first);
              return _out.write(std::move(v))
                .finally([this, msg_size, units = std::move(units)] {
                    _probe.add_bytes_sent(msg_size);
                });
          });
    }).handle_exception([this](std::exception_ptr e) {
        vlog(rpclog.info, "Error dispatching socket write:{}", e);
        _probe.request_error();
        fail_outstanding_futures();
    });
}

ss::future<> transport::do_reads() {
    return ss::do_until(
      [this] { return !is_valid(); },
      [this] {
          return parse_header(_in).then([this](std::optional<header> h) {
              if (!h) {
                  vlog(
                    rpclog.debug,
                    "could not parse header from server: {}",
                    server_address());
                  _probe.header_corrupted();
                  fail_outstanding_futures();
                  return ss::make_ready_future<>();
              }
              return dispatch(std::move(h.value()));
          });
      });
}

/// - this needs a streaming_context.
///
ss::future<> transport::dispatch(header h) {
    auto it = _correlations.find(h.correlation_id);
    if (it == _correlations.end()) {
        // We removed correlation already
        _probe.server_correlation_error();
        vlog(
          rpclog.debug,
          "Unable to find handler for correlation {}",
          h.correlation_id);
        // we have to skip received bytes to make input stream state correct
        return _in.skip(h.payload_size);
    }
    _probe.add_bytes_received(size_of_rpc_header + h.payload_size);
    auto ctx = std::make_unique<client_context_impl>(*this, h);
    auto fut = ctx->pr.get_future();
    // delete before setting value so that we don't run into nested exceptions
    // of broken promises
    auto pr = std::move(it->second);
    _correlations.erase(it);
    pr->set_value(std::move(ctx));
    _probe.request_completed();
    return fut;
}

void transport::setup_metrics(const std::optional<ss::sstring>& service_name) {
    _probe.setup_metrics(_metrics, service_name, server_address());
}

transport::~transport() {
    vlog(rpclog.debug, "RPC Client probes: {}", _probe);
    vassert(
      !is_valid(),
      "connection '{}' is still valid. must call stop() before destroying",
      *this);
}

std::ostream& operator<<(std::ostream& o, const transport& t) {
    fmt::print(
      o,
      "(server:{}, _correlations:{}, _correlation_idx:{})",
      t.server_address(),
      t._correlations.size(),
      t._correlation_idx);
    return o;
}
} // namespace rpc
