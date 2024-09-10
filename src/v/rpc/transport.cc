// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "rpc/transport.h"

#include "base/likely.h"
#include "base/vassert.h"
#include "base/vlog.h"
#include "net/connection.h"
#include "rpc/logger.h"
#include "rpc/parse_utils.h"
#include "rpc/response_handler.h"
#include "rpc/types.h"
#include "ssx/future-util.h"

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
      , _h(h) {}
    ss::future<ssx::semaphore_units> reserve_memory(size_t ask) final {
        auto fut = get_units(_c.get()._memory, ask);
        if (_c.get()._memory.waiters()) {
            _c.get()._probe->waiting_for_available_memory();
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
  : base_transport(
      base_transport::configuration{
        .server_addr = std::move(c.server_addr),
        .credentials = std::move(c.credentials),
      },
      &rpclog)
  , _memory(c.max_queued_bytes, "rpc/transport-mem")
  , _version(c.version)
  , _default_version(c.version)
  , _probe(std::make_unique<client_probe>()) {
    set_probe(_probe.get());
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
                _probe->connection_closed();
                fail_outstanding_futures();
                try {
                    f.get();
                } catch (...) {
                    auto e = std::current_exception();
                    if (net::is_disconnect_exception(e)) {
                        vlog(
                          rpc::rpclog.info,
                          "Disconnected from server {}: {}",
                          server_address(),
                          e);
                    } else {
                        vlog(
                          rpc::rpclog.error,
                          "Error dispatching client reads to {}: {}",
                          server_address(),
                          e);
                    }
                    _probe->read_dispatch_error();
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
transport::make_response_handler(netbuf& b, rpc::client_opts& opts) {
    if (_correlations.find(_correlation_idx + 1) != _correlations.end()) {
        _probe->client_correlation_error();
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
    handler_raw_ptr->with_timeout(opts.timeout, [this, method = b.name(), idx] {
        auto format_ms = [](clock_type::duration d) {
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(d);
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
            _probe->request_timeout();
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
        return ss::make_ready_future<ret_t>(errc::disconnected_endpoint);
    }
    return ss::with_gate(
      _dispatch_gate,
      [this, b = std::move(b), opts = std::move(opts), seq]() mutable {
          auto f = make_response_handler(b, opts);

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
                  auto e = std::make_unique<entry>(
                    std::move(scattered_message), corr);
                  _requests_queue.emplace(seq, std::move(e));

                  // By this point the request may already have timed out but
                  // we still do dispatch_send where it is handled. This is
                  // needed for two reasons:
                  // - Monotonic updates to _last_seq
                  // - Draining of the request_queue which could otherwise be
                  //   stalled by missing sequence number.
                  dispatch_send();
                  return std::move(f).finally([u = std::move(units)] {});
              })
            .handle_exception([this, seq, corr](std::exception_ptr eptr) {
                // This is unlikely but may potentially mean dispatch_send()
                // is not called, stalling the sequence number. Shut it down
                // because in this case it is not usable anymore.
                vlog(
                  rpclog.error,
                  "Exception {} dispatching rpc with sequence: {}, "
                  "correlation_idx: {}, last_seq: {}",
                  eptr,
                  seq,
                  corr,
                  _last_seq);
                _probe->request_error();
                fail_outstanding_futures();
                return ss::make_exception_future<ret_t>(eptr);
            });
      });
}

ss::future<> transport::do_dispatch_send() {
    return ss::do_until(
      [this] {
          if (_requests_queue.empty()) {
              return true;
          }
          auto queue_begin_sequence = _requests_queue.begin()->first;
          auto out_of_order = queue_begin_sequence
                              > (_last_seq + sequence_t(1));
          if (unlikely(out_of_order)) {
              vlog(
                rpclog.debug,
                "Dispatch request queue out of order. Last seq: "
                "{}, "
                "queue begin seq: {}",
                _last_seq,
                queue_begin_sequence);
          }
          return out_of_order;
      },
      // Be careful adding any scheduling points in the lambda
      // below.
      //
      // If a scheduling point is added before
      // `_requests_queue.erase` then two concurrent instances of
      // dispatch_send could try sending the same message.
      // Resulting in one of them throwing a seg. fault.
      //
      // And if a scheduling point is added after
      // `_requests_queue.erase` the conditional for executing the
      // lambda could succeed for two different messages
      // concurrently resulting in incorrect ordering of the sent
      // messages.
      [this] {
          auto it = _requests_queue.begin();
          _last_seq = it->first;
          auto v = std::move(it->second->scattered_message);
          auto corr = it->second->correlation_id;
          _requests_queue.erase(it);

          auto resp_it = _correlations.find(corr);
          if (resp_it == _correlations.end()) {
              // request had already completed even before we sent
              // it (probably due to timeout or disconnect). We
              // don't need to do anything.
              return ss::now();
          }
          auto& resp_entry = resp_it->second;

          // These units are released once we are out of scope here
          // and that is intentional because the underlying write
          // call to the batched output stream guarantees us the
          // in-order delivery of the dispatched write calls, which
          // is the intent of holding on to the units up until this
          // point.
          auto units = std::move(resp_entry->resource_units);
          auto msg_size = v.size();

          auto f = _out.write(std::move(v));
          resp_entry->timing.dispatched_at = clock_type::now();
          vlog(
            rpclog.trace,
            "Dispatched request with sequence: {}, "
            "correlation_idx: {}, "
            "pending queue_size: {}, target_address: {}",
            _last_seq,
            corr,
            _requests_queue.size(),
            server_address());
          return std::move(f)
            .then([this, corr](bool flushed) {
                if (auto maybe_timing = get_timing(corr)) {
                    maybe_timing->written_at = clock_type::now();
                    maybe_timing->flushed = flushed;
                }
            })
            .finally([this, msg_size] { _probe->add_bytes_sent(msg_size); });
      });
}

void transport::dispatch_send() {
    // Callers expect this function does not throw, so check if the gate is
    // closed so we know that `hold()` will never throw.
    if (_dispatch_gate.is_closed()) {
        return;
    }
    auto holder = _dispatch_gate.hold();
    ssx::background = ssx::ignore_shutdown_exceptions(do_dispatch_send())
                        .then_wrapped(
                          [this, h = std::move(holder)](ss::future<> fut) {
                              if (fut.failed()) {
                                  vlog(
                                    rpclog.info,
                                    "Error dispatching socket write:{}",
                                    fut.get_exception());
                                  _probe->request_error();
                                  fail_outstanding_futures();
                              }
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
                  _probe->header_corrupted();
                  fail_outstanding_futures();
                  return ss::make_ready_future<>();
              }
              return dispatch(h.value());
          });
      });
}

/// - this needs a streaming_context.
///
ss::future<> transport::dispatch(header h) {
    auto it = _correlations.find(h.correlation_id);
    if (it == _correlations.end()) {
        // We removed correlation already
        _probe->server_correlation_error();
        vlog(
          rpclog.debug,
          "Unable to find handler for correlation {}",
          h.correlation_id);
        // we have to skip received bytes to make input stream
        // state correct
        return _in.skip(h.payload_size);
    }
    _probe->add_bytes_received(size_of_rpc_header + h.payload_size);
    auto ctx = std::make_unique<client_context_impl>(*this, h);
    auto fut = ctx->pr.get_future();
    // delete before setting value so that we don't run into nested
    // exceptions of broken promises
    auto pr = std::move(it->second);
    _correlations.erase(it);
    pr->handler.set_value(std::move(ctx));
    _probe->request_completed();
    return fut;
}

void transport::setup_metrics(
  const std::optional<connection_cache_label>& label,
  const std::optional<model::node_id>& node_id) {
    namespace sm = ss::metrics;
    auto target = sm::label("target");
    std::vector<sm::label_instance> labels = {target(
      ssx::sformat("{}:{}", server_address().host(), server_address().port()))};
    if (label) {
        labels.push_back(sm::label("connection_cache_label")((*label)()));
    }
    std::vector<sm::label> aggregate_labels;
    // Label the metrics for a given server with the node ID so Seastar can
    // differentiate between them, in case multiple node IDs start at the same
    // address (e.g. in an ungraceful decommission). Aggregate on node ID so
    // the user is presented metrics for each server regardless of node ID.
    if (node_id) {
        auto node_id_label = sm::label("node_id");
        labels.push_back(node_id_label(*node_id));
        aggregate_labels.push_back(node_id_label);
    }
    _probe->setup_metrics(
      "rpc_client",
      labels,
      aggregate_labels,
      _probe->defs(labels, aggregate_labels));
}

timing_info* transport::get_timing(uint32_t correlation) {
    auto it = _correlations.find(correlation);
    return it == _correlations.end() ? nullptr : &it->second->timing;
}

transport::~transport() {
    vlog(
      rpclog.debug, "RPC Client to {} probes: {}", server_address(), *_probe);
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

std::vector<ss::metrics::metric_definition> client_probe::defs(
  const std::vector<ss::metrics::label_instance>& labels,
  const std::vector<ss::metrics::label>& aggregate_labels) {
    namespace sm = ss::metrics;
    std::vector<sm::metric_definition> ret;

    ret.emplace_back(sm::make_counter(
                       "requests",
                       [this] { return _requests; },
                       sm::description("Number of requests"),
                       labels)
                       .aggregate(aggregate_labels));

    ret.emplace_back(sm::make_gauge(
                       "requests_pending",
                       [this] { return _requests_pending; },
                       sm::description("Number of requests pending"),
                       labels)
                       .aggregate(aggregate_labels));

    ret.emplace_back(sm::make_counter(
                       "request_errors",
                       [this] { return _request_errors; },
                       sm::description("Number or requests errors"),
                       labels)
                       .aggregate(aggregate_labels));

    ret.emplace_back(sm::make_counter(
                       "request_timeouts",
                       [this] { return _request_timeouts; },
                       sm::description("Number or requests timeouts"),
                       labels)
                       .aggregate(aggregate_labels));

    ret.emplace_back(
      sm::make_total_bytes(
        "out_bytes",
        [this] { return _out_bytes; },
        sm::description("Total number of bytes sent (including headers)"),
        labels)
        .aggregate(aggregate_labels));

    ret.emplace_back(sm::make_total_bytes(
                       "in_bytes",
                       [this] { return _in_bytes; },
                       sm::description("Total number of bytes received"),
                       labels)
                       .aggregate(aggregate_labels));
    ret.emplace_back(
      sm::make_counter(
        "read_dispatch_errors",
        [this] { return _read_dispatch_errors; },
        sm::description("Number of errors while dispatching responses"),
        labels)
        .aggregate(aggregate_labels));

    ret.emplace_back(
      sm::make_counter(
        "corrupted_headers",
        [this] { return _corrupted_headers; },
        sm::description("Number of responses with corrupted headers"),
        labels)
        .aggregate(aggregate_labels));

    ret.emplace_back(
      sm::make_counter(
        "server_correlation_errors",
        [this] { return _server_correlation_errors; },
        sm::description("Number of responses with wrong correlation id"),
        labels)
        .aggregate(aggregate_labels));

    ret.emplace_back(
      sm::make_counter(
        "client_correlation_errors",
        [this] { return _client_correlation_errors; },
        sm::description("Number of errors in client correlation id"),
        labels)
        .aggregate(aggregate_labels));

    ret.emplace_back(
      sm::make_counter(
        "requests_blocked_memory",
        [this] { return _requests_blocked_memory; },
        sm::description("Number of requests that are blocked because"
                        " of insufficient memory"),
        labels)
        .aggregate(aggregate_labels));

    return ret;
}

std::ostream& operator<<(std::ostream& o, const client_probe& p) {
    o << "{"
      << " requests_sent: " << p._requests
      << ", requests_pending: " << p._requests_pending
      << ", requests_completed: " << p._requests_completed
      << ", request_errors: " << p._request_errors
      << ", request_timeouts: " << p._request_timeouts
      << ", in_bytes: " << p._in_bytes << ", out_bytes: " << p._out_bytes
      << ", connects: " << p._connects << ", connections: " << p._connections
      << ", connection_errors: " << p._connection_errors
      << ", read_dispatch_errors: " << p._read_dispatch_errors
      << ", corrupted_headers: " << p._corrupted_headers
      << ", server_correlation_errors: " << p._server_correlation_errors
      << ", client_correlation_errors: " << p._client_correlation_errors
      << ", requests_blocked_memory: " << p._requests_blocked_memory << " }";
    return o;
}

} // namespace rpc
