/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/server/connection_context.h"

#include "config/configuration.h"
#include "kafka/server/protocol.h"
#include "kafka/server/protocol_utils.h"
#include "kafka/server/quota_manager.h"
#include "kafka/server/request_context.h"

#include <seastar/core/scattered_message.hh>
#include <seastar/core/sleep.hh>

namespace kafka {

ss::future<> connection_context::process_one_request() {
    return parse_size(_rs.conn->input())
      .then([this](std::optional<size_t> sz) mutable {
          if (!sz) {
              return ss::make_ready_future<>();
          }
          return parse_header(_rs.conn->input())
            .then(
              [this, s = sz.value()](std::optional<request_header> h) mutable {
                  _rs.probe().request_received();
                  _rs.probe().add_bytes_received(s);
                  if (!h) {
                      vlog(
                        klog.debug,
                        "could not parse header from client: {}",
                        _rs.conn->addr);
                      _rs.probe().header_corrupted();
                      return ss::make_ready_future<>();
                  }
                  return dispatch_method_once(std::move(h.value()), s);
              });
      });
}

bool connection_context::is_finished_parsing() const {
    return _rs.conn->input().eof() || _rs.abort_requested();
}

ss::future<connection_context::session_resources>
connection_context::throttle_request(
  std::optional<std::string_view> client_id, size_t request_size) {
    // update the throughput tracker for this client using the
    // size of the current request and return any computed delay
    // to apply for quota throttling.
    //
    // note that when throttling is first applied the request is
    // allowed to pass through and subsequent requests and
    // delayed. this is a similar strategy used by kafka: the
    // response is important because it allows clients to
    // distinguish throttling delays from real delays. delays
    // applied to subsequent messages allow backpressure to take
    // affect.
    auto delay = _proto.quota_mgr().record_tp_and_throttle(
      client_id, request_size);

    auto fut = ss::now();
    if (!delay.first_violation) {
        fut = ss::sleep_abortable(delay.duration, _rs.abort_source());
    }
    return fut
      .then(
        [this, request_size] { return reserve_request_units(request_size); })
      .then([this, delay](ss::semaphore_units<> units) {
          return session_resources{
            .backpressure_delay = delay.duration,
            .memlocks = std::move(units),
            .method_latency = _rs.hist().auto_measure(),
          };
      });
}

ss::future<ss::semaphore_units<>>
connection_context::reserve_request_units(size_t size) {
    // Allow for extra copies and bookkeeping
    auto mem_estimate = size * 2 + 8000; // NOLINT
    if (mem_estimate >= (size_t)std::numeric_limits<int32_t>::max()) {
        // TODO: Create error response using the specific API?
        throw std::runtime_error(fmt::format(
          "request too large > 1GB (size: {}; estimate: {})",
          size,
          mem_estimate));
    }
    auto fut = ss::get_units(_rs.memory(), mem_estimate);
    if (_rs.memory().waiters()) {
        _rs.probe().waiting_for_available_memory();
    }
    return fut;
}

ss::future<>
connection_context::dispatch_method_once(request_header hdr, size_t size) {
    return throttle_request(hdr.client_id, size)
      .then([this, hdr = std::move(hdr), size](session_resources sres) mutable {
          if (_rs.abort_requested()) {
              // protect against shutdown behavior
              return ss::make_ready_future<>();
          }
          auto remaining = size - sizeof(raw_request_header)
                           - hdr.client_id_buffer.size();
          return read_iobuf_exactly(_rs.conn->input(), remaining)
            .then([this, hdr = std::move(hdr), sres = std::move(sres)](
                    iobuf buf) mutable {
                if (_rs.abort_requested()) {
                    // _proto._cntrl etc might not be alive
                    return;
                }
                auto self = shared_from_this();
                auto rctx = request_context(
                  self,
                  std::move(hdr),
                  std::move(buf),
                  sres.backpressure_delay);
                // background process this one full request
                (void)ss::with_gate(
                  _rs.conn_gate(),
                  [this, rctx = std::move(rctx)]() mutable {
                      return do_process(std::move(rctx));
                  })
                  .handle_exception([self](std::exception_ptr e) {
                      vlog(
                        klog.info, "Detected error processing request: {}", e);
                      self->_rs.conn->shutdown_input();
                  })
                  .finally([s = std::move(sres), self] {});
            });
      });
}

ss::future<> connection_context::do_process(request_context ctx) {
    const auto correlation = ctx.header().correlation;
    const sequence_id seq = _seq_idx;
    _seq_idx = _seq_idx + sequence_id(1);
    return kafka::process_request(std::move(ctx), _proto.smp_group())
      .then([this, seq, correlation](response_ptr r) mutable {
          r->set_correlation(correlation);
          _responses.insert({seq, std::move(r)});
          return process_next_response();
      });
}

ss::future<> connection_context::process_next_response() {
    return ss::repeat([this]() mutable {
        auto it = _responses.find(_next_response);
        if (it == _responses.end()) {
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::yes);
        }
        // found one; increment counter
        _next_response = _next_response + sequence_id(1);

        auto r = std::move(it->second);
        _responses.erase(it);
        _rs.probe().request_completed();

        if (r->is_noop()) {
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::no);
        }

        auto msg = response_as_scattered(std::move(r));
        _rs.probe().add_bytes_sent(msg.size());
        try {
            return _rs.conn->write(std::move(msg)).then([] {
                return ss::make_ready_future<ss::stop_iteration>(
                  ss::stop_iteration::no);
            });
        } catch (...) {
            vlog(
              klog.debug,
              "Failed to process request: {}",
              std::current_exception());
        }
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    });
}

} // namespace kafka
