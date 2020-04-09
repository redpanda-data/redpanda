#include "rpc/transport.h"

#include "likely.h"
#include "rpc/logger.h"
#include "rpc/parse_utils.h"
#include "rpc/types.h"
#include "vlog.h"

#include <seastar/core/reactor.hh>

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
    std::reference_wrapper<transport> _c;
    header _h;
    ss::promise<> pr;
};

base_transport::base_transport(configuration c)
  : _server_addr(c.server_addr)
  , _creds(
      c.credentials ? c.credentials->build_certificate_credentials()
                    : nullptr) {}

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

ss::future<> base_transport::do_connect() {
    // hold invariant of having an always valid dispatch gate
    // and make sure we don't have a live connection already
    if (is_valid() || _dispatch_gate.is_closed()) {
        return ss::make_exception_future<>(std::runtime_error(fmt::format(
          "cannot do_connect with a valid connection. remote:{}",
          server_address())));
    }
    return ss::engine()
      .net()
      .connect(
        server_address(),
        ss::socket_address(sockaddr_in{AF_INET, INADDR_ANY, {0}}),
        ss::transport::TCP)
      .then([this](ss::connected_socket fd) mutable {
          if (_creds) {
              return ss::tls::wrap_client(_creds, std::move(fd));
          }
          return ss::make_ready_future<ss::connected_socket>(std::move(fd));
      })
      .then_wrapped([this](ss::future<ss::connected_socket> f_fd) mutable {
          try {
              _fd = std::make_unique<ss::connected_socket>(f_fd.get0());
          } catch (...) {
              auto e = std::current_exception();
              _probe.connection_error(e);
              std::rethrow_exception(e);
          }
          _probe.connection_established();
          _in = _fd->input();
          _out = batched_output_stream(_fd->output());
      });
}
ss::future<> base_transport::connect() {
    // in order to hold concurrency correctness invariants we must guarantee 3
    // things before we attempt to send a payload:
    // 1. there are no background futures waiting
    // 2. the _dispatch_gate() is open
    // 3. the connection is valid
    //
    return stop().then([this] {
        _dispatch_gate = {};
        return do_connect();
    });
}
ss::future<> base_transport::stop() {
    fail_outstanding_futures();
    return _dispatch_gate.close();
}
void transport::fail_outstanding_futures() {
    // must close the socket
    shutdown();
    for (auto& [_, p] : _correlations) {
        p->set_exception(std::runtime_error("failing outstanding futures"));
    }
    _correlations.clear();
}
void base_transport::shutdown() {
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

ss::future<> transport::connect() {
    return base_transport::connect().then([this] {
        _correlation_idx = 0;
        // background
        (void)ss::with_gate(_dispatch_gate, [this] {
            return do_reads().then_wrapped([this](ss::future<> f) {
                _probe.connection_closed();
                try {
                    f.get();
                } catch (...) {
                    fail_outstanding_futures();
                    _probe.read_dispatch_error(std::current_exception());
                }
            });
        });
    });
}

ss::future<std::unique_ptr<streaming_context>>
transport::send(netbuf b, rpc::client_opts opts) {
    // hold invariant of always having a valid connection _and_ a working
    // dispatch gate where we can wait for async futures
    if (!is_valid() || _dispatch_gate.is_closed()) {
        return ss::make_exception_future<std::unique_ptr<streaming_context>>(
          std::runtime_error(fmt::format(
            "cannot send payload with invalid connection. remote:{}",
            server_address())));
    }
    return ss::with_gate(
      _dispatch_gate, [this, b = std::move(b), opts]() mutable {
          if (_correlations.find(_correlation_idx + 1) != _correlations.end()) {
              _probe.client_correlation_error();
              throw std::runtime_error(
                "Invalid transport state. Doubly registered correlation_id");
          }
          const uint32_t idx = ++_correlation_idx;
          auto item = std::make_unique<internal::response_handler>();
          auto it = item.get();
          // capture the future _before_ inserting promise in the map
          // in case there is a concurrent error w/ the connection and it fails
          // the future before we return from this function
          auto fut = it->get_future();
          b.set_correlation_id(idx);
          _correlations.emplace(idx, std::move(item));
          it->with_timeout(opts.timeout, [this, idx] {
              auto it = _correlations.find(idx);
              if (likely(it != _correlations.end())) {
                  vlog(
                    rpclog.debug, "Request timeout, correlation id: {}", idx);
                  _probe.request_timeout();
                  _correlations.erase(it);
              }
          });

          // send
          auto view = std::move(b).as_scattered();
          const auto sz = view.size();
          return get_units(_memory, sz)
            .then([this, v = std::move(view), f = std::move(fut)](
                    ss::semaphore_units<> units) mutable {
                /// background
                (void)ss::with_gate(
                  _dispatch_gate,
                  [this, v = std::move(v), u = std::move(units)]() mutable {
                      auto msg_size = v.size();
                      return _out.write(std::move(v))
                        .then([this, msg_size, u = std::move(u)] {
                            _probe.add_bytes_sent(msg_size);
                        });
                  })
                  .handle_exception([this](std::exception_ptr e) {
                      vlog(rpclog.info, "Error dispatching socket write:{}", e);
                      _probe.request_error();
                      fail_outstanding_futures();
                  });
                return std::move(f);
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
