/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/server/connection_context.h"

#include "bytes/iobuf.h"
#include "config/configuration.h"
#include "kafka/protocol/sasl_authenticate.h"
#include "kafka/server/protocol.h"
#include "kafka/server/protocol_utils.h"
#include "kafka/server/quota_manager.h"
#include "kafka/server/request_context.h"
#include "security/exceptions.h"
#include "units.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/scattered_message.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/with_timeout.hh>

#include <chrono>
#include <memory>

using namespace std::chrono_literals;

namespace kafka {

ss::future<> connection_context::process_one_request() {
    return parse_size(_rs.conn->input())
      .then([this](std::optional<size_t> sz) mutable {
          if (!sz) {
              return ss::make_ready_future<>();
          }
          /*
           * Intercept the wire protocol when:
           *
           * 1. sasl is enabled (implied by 2)
           * 2. during auth phase
           * 3. handshake was v0
           */
          if (unlikely(
                sasl().state()
                  == security::sasl_server::sasl_state::authenticate
                && sasl().handshake_v0())) {
              return handle_auth_v0(*sz).handle_exception(
                [this](std::exception_ptr e) {
                    vlog(klog.info, "Detected error processing request: {}", e);
                    _rs.conn->shutdown_input();
                });
          }
          return parse_header(_rs.conn->input())
            .then(
              [this, s = sz.value()](std::optional<request_header> h) mutable {
                  _rs.probe().add_bytes_received(s);
                  if (!h) {
                      vlog(
                        klog.debug,
                        "could not parse header from client: {}",
                        _rs.conn->addr);
                      _rs.probe().header_corrupted();
                      return ss::make_ready_future<>();
                  }
                  return handle_mtls_auth()
                    .then([this, h = std::move(h.value()), s]() mutable {
                        return dispatch_method_once(std::move(h), s);
                    })
                    .handle_exception_type([this](const std::bad_alloc&) {
                        // In general, dispatch_method_once does not throw,
                        // but bad_allocs are an exception.  Log it cleanly
                        // to avoid this bubbling up as an unhandled
                        // exceptional future.
                        vlog(
                          klog.error,
                          "Request from {} failed on memory exhaustion "
                          "(std::bad_alloc)",
                          _rs.conn->addr);
                    });
              });
      });
}

/*
 * handle mtls authentication. this should only happen once when the connection
 * is setup. even though this is called in the normal request handling path,
 * this property should hold becuase:
 *
 * 1. is a noop if a mtls principal has been extracted
 * 2. all code paths that don't set the principal throw and drop the connection
 *
 * NOTE: handle_mtls_auth is called after reading header off the wire. this is
 * odd because we would expect that tls negotation etc... all happens before we
 * here to the application layer. however, it appears that the way seastar works
 * that we need to read some data off the wire to drive this process within the
 * internal connection handling.
 */
ss::future<> connection_context::handle_mtls_auth() {
    if (!_use_mtls || _mtls_principal.has_value()) {
        return ss::now();
    }
    return ss::with_timeout(
             model::timeout_clock::now() + 5s,
             _rs.conn->get_distinguished_name())
      .then([this](std::optional<ss::session_dn> dn) {
          if (!dn.has_value()) {
              throw security::exception(
                security::errc::invalid_credentials,
                "failed to fetch distinguished name");
          }
          /*
           * for now it probably is fine to store the mapping per connection.
           * but it seems like we could also share this across all connections
           * with the same tls configuration.
           */
          _mtls_principal = _rs.conn->get_principal_mapping()->apply(
            dn->subject);
          if (!_mtls_principal) {
              throw security::exception(
                security::errc::invalid_credentials,
                fmt::format(
                  "failed to extract principal from distinguished name: {}",
                  dn->subject));
          }

          vlog(
            _authlog.debug,
            "got principal: {}, from distinguished name: {}",
            *_mtls_principal,
            dn->subject);
      });
}

/*
 * The SASL authentication flow for a client using version 0 of SASL handshake
 * doesn't use an envelope request for tokens. This method intercepts the
 * authentication phase and builds an envelope request so that all of the normal
 * request processing can be re-used.
 *
 * Even though we build and decode a request/response, the payload is a small
 * authentication string. https://github.com/redpanda-data/redpanda/issues/1315.
 * When this ticket is complete we'll be able to easily remove this extra
 * serialization step and and easily operate on non-encoded requests/responses.
 */
ss::future<> connection_context::handle_auth_v0(const size_t size) {
    vlog(klog.debug, "Processing simulated SASL authentication request");

    /*
     * very generous upper bound for some added safety. generally the size is
     * small and corresponds to the representation of hashes being exchanged but
     * there is some flexibility as usernames, nonces, etc... have no strict
     * limits. future non-SCRAM mechanisms may have other size requirements.
     */
    if (unlikely(size > 256_KiB)) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format, "Auth (handshake_v0) message too large", size));
    }

    const api_version version(0);
    iobuf request_buf;
    {
        auto data = co_await read_iobuf_exactly(_rs.conn->input(), size);
        sasl_authenticate_request request;
        request.data.auth_bytes = iobuf_to_bytes(data);
        response_writer writer(request_buf);
        request.encode(writer, version);
    }

    sasl_authenticate_response response;
    {
        auto ctx = request_context(
          shared_from_this(),
          request_header{
            .key = sasl_authenticate_api::key,
            .version = version,
          },
          std::move(request_buf),
          0s);
        auto resp = co_await kafka::process_request(
                      std::move(ctx), _proto.smp_group())
                      .response;
        auto data = std::move(*resp).release();
        response.decode(std::move(data), version);
    }

    if (response.data.error_code != error_code::none) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Auth (handshake v0) error {}: {}",
          response.data.error_code,
          response.data.error_message));
    }

    if (sasl().state() == security::sasl_server::sasl_state::failed) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format, "Auth (handshake v0) failed with unknown error"));
    }

    iobuf data;
    response_writer writer(data);
    writer.write(response.data.auth_bytes);
    auto msg = iobuf_as_scattered(std::move(data));
    co_await _rs.conn->write(std::move(msg));
}

bool connection_context::is_finished_parsing() const {
    return _rs.conn->input().eof() || _rs.abort_requested();
}

ss::future<connection_context::session_resources>
connection_context::throttle_request(
  const request_header& hdr, size_t request_size) {
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
      hdr.client_id, request_size);
    auto tracker = std::make_unique<request_tracker>(_rs.probe());
    auto fut = ss::now();
    if (!delay.first_violation) {
        fut = ss::sleep_abortable(delay.duration, _rs.abort_source());
    }
    auto track = track_latency(hdr.key);
    return fut
      .then(
        [this, request_size] { return reserve_request_units(request_size); })
      .then([this, delay, track, tracker = std::move(tracker)](
              ss::semaphore_units<> units) mutable {
          return server().get_request_unit().then(
            [this,
             delay,
             mem_units = std::move(units),
             track,
             tracker = std::move(tracker)](
              ss::semaphore_units<> qd_units) mutable {
                session_resources r{
                  .backpressure_delay = delay.duration,
                  .memlocks = std::move(mem_units),
                  .queue_units = std::move(qd_units),
                  .tracker = std::move(tracker),
                };
                if (track) {
                    r.method_latency = _rs.hist().auto_measure();
                }
                return r;
            });
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
    return throttle_request(hdr, size).then([this, hdr = std::move(hdr), size](
                                              session_resources sres) mutable {
        if (_rs.abort_requested()) {
            // protect against shutdown behavior
            return ss::make_ready_future<>();
        }
        auto remaining = size - request_header_size
                         - hdr.client_id_buffer.size() - hdr.tags_size_bytes;
        return read_iobuf_exactly(_rs.conn->input(), remaining)
          .then([this, hdr = std::move(hdr), sres = std::move(sres)](
                  iobuf buf) mutable {
              if (_rs.abort_requested()) {
                  // _proto._cntrl etc might not be alive
                  return ss::now();
              }
              auto self = shared_from_this();
              auto rctx = request_context(
                self, std::move(hdr), std::move(buf), sres.backpressure_delay);
              /*
               * we process requests in order since all subsequent requests
               * are dependent on authentication having completed.
               *
               * the other important reason for disabling pipeling is because
               * when a sasl handshake with version=0 is processed, the next
               * data on the wire is _not_ another request: it is a
               * size-prefixed authentication payload without a request
               * envelope, and requires special handling.
               *
               * a well behaved client should implicitly provide a data stream
               * that invokes this behavior in the server: that is, it won't
               * send auth data (or any other requests) until handshake or the
               * full auth-process completes, etc... but representing these
               * nuances of the protocol _explicitly_ in the server makes its
               * behavior easier to understand and avoids misbehaving clients
               * creating server-side errors that will appear as a corrupted
               * stream at best and at worst some odd behavior.
               */

              const auto correlation = rctx.header().correlation;
              const sequence_id seq = _seq_idx;
              _seq_idx = _seq_idx + sequence_id(1);
              auto res = kafka::process_request(
                std::move(rctx), _proto.smp_group());
              /**
               * first stage processed in a foreground.
               */
              return res.dispatched
                .then_wrapped([this,
                               f = std::move(res.response),
                               seq,
                               correlation,
                               self,
                               s = std::move(sres)](ss::future<> d) mutable {
                    /*
                     * if the dispatch/first stage failed, then we need to
                     * need to consume the second stage since it might be
                     * an exceptional future. if we captured `f` in the
                     * lambda but didn't use `then_wrapped` then the
                     * lambda would be destroyed and an ignored
                     * exceptional future would be caught by seastar.
                     */
                    if (d.failed()) {
                        return f.discard_result()
                          .handle_exception([](std::exception_ptr e) {
                              vlog(
                                klog.info,
                                "Discarding second stage failure {}",
                                e);
                          })
                          .finally([self, d = std::move(d)]() mutable {
                              self->_rs.probe().service_error();
                              self->_rs.probe().request_completed();
                              return std::move(d);
                          });
                    }
                    /**
                     * second stage processed in background.
                     */
                    ssx::background
                      = ssx::spawn_with_gate_then(
                          _rs.conn_gate(),
                          [this, f = std::move(f), seq, correlation]() mutable {
                              return f.then([this, seq, correlation](
                                              response_ptr r) mutable {
                                  r->set_correlation(correlation);
                                  _responses.insert({seq, std::move(r)});
                                  return process_next_response();
                              });
                          })
                          .handle_exception([self](std::exception_ptr e) {
                              // ssx::spawn_with_gate already caught
                              // shutdown-like exceptions, so we should only be
                              // taking this path for real errors.  That also
                              // means that on shutdown we don't bother to call
                              // shutdown_input on the connection, so rely
                              // on any future reader to check the abort
                              // source before considering reading the
                              // connection.

                              auto disconnected = net::is_disconnect_exception(
                                e);
                              if (disconnected) {
                                  vlog(
                                    klog.info,
                                    "Disconnected {} ({})",
                                    self->_rs.conn->addr,
                                    disconnected.value());
                              } else {
                                  vlog(
                                    klog.warn,
                                    "Error processing request: {}",
                                    e);
                              }

                              self->_rs.probe().service_error();
                              self->_rs.conn->shutdown_input();
                          })
                          .finally([s = std::move(s), self] {});
                    return d;
                })
                .handle_exception([self](std::exception_ptr e) {
                    vlog(
                      klog.info, "Detected error dispatching request: {}", e);
                    self->_rs.conn->shutdown_input();
                });
          });
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

        if (r->is_noop()) {
            return ss::make_ready_future<ss::stop_iteration>(
              ss::stop_iteration::no);
        }

        auto msg = response_as_scattered(std::move(r));
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
