// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "rpc/transport.h"

#include "likely.h"
#include "net/connection.h"
#include "rpc/logger.h"
#include "rpc/parse_utils.h"
#include "rpc/response_handler.h"
#include "rpc/types.h"
#include "ssx/future-util.h"
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
    ss::future<ssx::semaphore_units> reserve_memory(size_t ask) final {
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

transport::transport(
  transport_configuration c,
  [[maybe_unused]] std::optional<ss::sstring> service_name)
  : base_transport(base_transport::configuration{
    .server_addr = std::move(c.server_addr),
    .credentials = std::move(c.credentials),
  })
  , _memory(c.max_queued_bytes, "rpc/transport-mem") {
    if (!c.disable_metrics) {
        setup_metrics(service_name);
    }
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

ss::future<> transport::connect(clock_type::duration connection_timeout) {
    return connect(connection_timeout + rpc::clock_type::now());
}

void transport::reset_state() {
    /*
     * the _correlation_idx is explicitly not reset as a pragmatic approach to
     * dealing with an apparent race condition in which two requests with the
     * same correlation id are in flight shortly after a reset.
     */
    _last_seq = sequence_t{0};
    _seq = sequence_t{0};
    _version = transport_version::v1;
}

ss::future<>
transport::connect(rpc::clock_type::time_point connection_timeout) {
    return base_transport::connect(connection_timeout).then([this] {
        // background
        ssx::spawn_with_gate(_dispatch_gate, [this] {
            return do_reads().then_wrapped([this](ss::future<> f) {
                _probe.connection_closed();
                fail_outstanding_futures();
                try {
                    f.get();
                } catch (...) {
                    auto e = std::current_exception();
                    if (net::is_disconnect_exception(e)) {
                        rpc::rpclog.info(
                          "Disconnected from server {}: {}",
                          server_address(),
                          e);
                    } else {
                        rpc::rpclog.error(
                          "Error dispatching client reads to {}: {}",
                          server_address(),
                          e);
                    }
                    _probe.read_dispatch_error();
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
transport::make_response_handler(
  netbuf& b, const rpc::client_opts& opts, sequence_t seq) {
    if (_correlations.find(_correlation_idx + 1) != _correlations.end()) {
        _probe.client_correlation_error();
        vlog(
          rpclog.error,
          "Invalid transport state, reusing correlation id: {}",
          _correlation_idx + 1);
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
    item_raw_ptr->with_timeout(opts.timeout, [this, idx, seq] {
        /*
         * remove pending entry from requests queue. If a timeout occurred
         * before an entry was sent we can not keep the entry alive as it may
         * contain caller semaphore units, the units must be released when we
         * notify caller with the result.
         */
        _requests_queue.erase(seq);
        auto it = _correlations.find(idx);
        if (likely(it != _correlations.end())) {
            vlog(
              rpclog.info,
              "Request timeout to {}, correlation id: {} ({} in flight)",
              server_address(),
              idx,
              _correlations.size());
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
          auto f = make_response_handler(b, opts, seq);

          // send
          auto sz = b.buffer().size_bytes();
          return get_units(_memory, sz)
            .then([this,
                   b = std::move(b),
                   f = std::move(f),
                   seq,
                   u = std::move(opts.resource_units)](
                    ssx::semaphore_units units) mutable {
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

ss::future<> transport::do_dispatch_send() {
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
          // These units are released once we are out of scope here
          // and that is intentional because the underlying write call
          // to the batched output stream guarantees us the in-order
          // delivery of the dispatched write calls, which is the intent
          // of holding on to the units up until this point.
          auto units = std::move(it->second->resource_units);
          auto v = std::move(*buffer).as_scattered();
          auto msg_size = v.size();
          _requests_queue.erase(it->first);
          auto f = _out.write(std::move(v));
          return std::move(f).finally(
            [this, msg_size] { _probe.add_bytes_sent(msg_size); });
      });
}

void transport::dispatch_send() {
    ssx::spawn_with_gate(_dispatch_gate, [this]() mutable {
        return ssx::ignore_shutdown_exceptions(do_dispatch_send())
          .handle_exception([this](std::exception_ptr e) {
              vlog(rpclog.info, "Error dispatching socket write:{}", e);
              _probe.request_error();
              fail_outstanding_futures();
          });
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
        // we have to skip received bytes to make input stream
        // state correct
        return _in.skip(h.payload_size);
    }
    _probe.add_bytes_received(size_of_rpc_header + h.payload_size);
    auto ctx = std::make_unique<client_context_impl>(*this, h);
    auto fut = ctx->pr.get_future();
    // delete before setting value so that we don't run into nested
    // exceptions of broken promises
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
      "connection '{}' is still valid. must call stop() before "
      "destroying",
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
