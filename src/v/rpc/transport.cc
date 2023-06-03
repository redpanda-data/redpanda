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

#include <fmt/core.h>

#include <chrono>
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
  std::optional<connection_cache_label> label,
  std::optional<model::node_id> node_id)
  : base_transport(base_transport::configuration{
    .server_addr = std::move(c.server_addr),
    .credentials = std::move(c.credentials),
  })
  , _memory(c.max_queued_bytes, "rpc/transport-mem")
  , _version(c.version)
  , _default_version(c.version) {
    if (!c.disable_metrics) {
        setup_metrics(label, node_id);
    }
}

void transport::fail_outstanding_futures() noexcept {
    // must close the socket
    shutdown();
    for (auto& [_, p] : _correlations) {
        p->handler.set_value(errc::disconnected_endpoint);
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
    _version = _default_version;
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
  netbuf& b, rpc::client_opts& opts, sequence_t seq) {
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
    b.set_correlation_id(idx);

    auto entry = std::make_unique<response_entry>();

    // Normally resource_units will be released when we send the request to
    // remote server. But if a timeout or disconnect happens before sending,
    // they must be released when the caller is notified with the result.
    entry->resource_units = std::move(opts.resource_units);

    // set initial timing info for this request
    entry->timing.timeout = opts.timeout;
    entry->timing.enqueued_at = timing_info::clock_type::now();

    auto handler_raw_ptr = &entry->handler;
    // capture the future _before_ inserting promise in the map
    // in case there is a concurrent error w/ the connection and it
    // fails the future before we return from this function
    auto response_future = handler_raw_ptr->get_future();

    auto [_, success] = _correlations.emplace(idx, std::move(entry));
    if (unlikely(!success)) {
        throw std::logic_error(
          fmt::format("Tried to reuse correlation id: {}", idx));
    }
    handler_raw_ptr->with_timeout(
      opts.timeout, [this, method = b.name(), idx, seq] {
          auto format_ms = [](clock_type::duration d) {
              auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                d);
              return fmt::format("{} ms", ms.count());
          };

          auto from_now =
            [now = timing_info::clock_type::now(), format_ms](
              timing_info::clock_type::time_point earlier) -> std::string {
              if (earlier == timing_info::unset) {
                  return "unset";
              }
              return format_ms(now - earlier);
          };
          _requests_queue.erase(seq);
          auto it = _correlations.find(idx);
          // The timeout may race with the completion of the request (and
          // removal from _correlations map) in which case we treat this as a
          // not-timed-out request.
          if (likely(it != _correlations.end())) {
              auto& timing = it->second->timing;
              vlog(
                rpclog.info,
                "RPC timeout ({}) to {}, method: {}, correlation id: {}, {} "
                "in flight, time since: {{init: {}, enqueue: {}, "
                "memory_reserved: {} dispatch: "
                "{}, written: {}}}, flushed: {}",
                format_ms(timing.timeout.timeout_period),
                server_address(),
                method,
                idx,
                _correlations.size(),
                from_now(
                  timing.timeout.timeout_at() - timing.timeout.timeout_period),
                from_now(timing.enqueued_at),
                from_now(timing.memory_reserved_at),
                from_now(timing.dispatched_at),
                from_now(timing.written_at),
                timing.flushed);
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
          auto corr = b.correlation_id();
          return get_units(_memory, sz)
            .then([b = std::move(b), corr, this](
                    ssx::semaphore_units units) mutable {
                auto it = _correlations.find(corr);
                if (likely(it != _correlations.end())) {
                    auto& timing = it->second->timing;
                    timing.memory_reserved_at = clock_type::now();
                }
                return std::move(b).as_scattered().then(
                  [u = std::move(units)](
                    ss::scattered_message<char> scattered_message) mutable {
                      return std::make_tuple(
                        std::move(u), std::move(scattered_message));
                  });
            })
            .then_unpack(
              [this, f = std::move(f), seq, corr](
                ssx::semaphore_units units,
                ss::scattered_message<char> scattered_message) mutable {
                  // Check that the request hasn't been finished yet.
                  // If it has (due to timeout or disconnect), we don't need to
                  // send it.
                  if (_correlations.contains(corr)) {
                      auto e = entry{
                        .scattered_message
                        = std::make_unique<ss::scattered_message<char>>(
                          std::move(scattered_message)),
                        .correlation_id = corr};

                      _requests_queue.emplace(
                        seq, std::make_unique<entry>(std::move(e)));
                      dispatch_send();
                  }
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
    ssx::background
      = ssx::spawn_with_gate_then(_dispatch_gate, [this]() mutable {
            return ss::do_until(
              [this] {
                  return _requests_queue.empty()
                         || _requests_queue.begin()->first
                              > (_last_seq + sequence_t(1));
              },
              // Be careful adding any scheduling points in the lambda below.
              //
              // If a scheduling point is added before `_requests_queue.erase`
              // then two concurrent instances of dispatch_send could try
              // sending the same message. Resulting in one of them throwing a
              // seg. fault.
              //
              // And if a scheduling point is added after
              // `_requests_queue.erase` the conditional for executing the
              // lambda could succeed for two different messages concurrently
              // resulting in incorrect ordering of the sent messages.
              [this] {
                  auto it = _requests_queue.begin();
                  _last_seq = it->first;
                  auto v = std::move(*it->second->scattered_message);
                  auto corr = it->second->correlation_id;
                  _requests_queue.erase(it);

                  auto resp_it = _correlations.find(corr);
                  if (resp_it == _correlations.end()) {
                      // request had already completed even before we sent it
                      // (probably due to timeout or disconnect). We don't need
                      // to do anything.
                      return ss::now();
                  }
                  auto& resp_entry = resp_it->second;

                  // These units are released once we are out of scope here
                  // and that is intentional because the underlying write call
                  // to the batched output stream guarantees us the in-order
                  // delivery of the dispatched write calls, which is the intent
                  // of holding on to the units up until this point.
                  auto units = std::move(resp_entry->resource_units);
                  auto msg_size = v.size();

                  auto f = _out.write(std::move(v));
                  resp_entry->timing.dispatched_at = clock_type::now();
                  return std::move(f)
                    .then([this, corr](bool flushed) {
                        if (auto maybe_timing = get_timing(corr)) {
                            maybe_timing->written_at = clock_type::now();
                            maybe_timing->flushed = flushed;
                        }
                    })
                    .finally(
                      [this, msg_size] { _probe.add_bytes_sent(msg_size); });
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
    pr->handler.set_value(std::move(ctx));
    _probe.request_completed();
    return fut;
}

void transport::setup_metrics(
  const std::optional<connection_cache_label>& label,
  const std::optional<model::node_id>& node_id) {
    _probe.setup_metrics(_metrics, label, node_id, server_address());
}

timing_info* transport::get_timing(uint32_t correlation) {
    auto it = _correlations.find(correlation);
    return it == _correlations.end() ? nullptr : &it->second->timing;
}

transport::~transport() {
    vlog(rpclog.debug, "RPC Client to {} probes: {}", server_address(), _probe);
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
