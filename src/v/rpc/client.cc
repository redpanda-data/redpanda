#include "rpc/client.h"

#include "rpc/logger.h"
#include "rpc/parse_utils.h"

#include <seastar/core/reactor.hh>

namespace rpc {
struct client_context_impl final : streaming_context {
    client_context_impl(client& s, header h)
      : _c(std::ref(s))
      , _h(std::move(h)) {}
    future<semaphore_units<>> reserve_memory(size_t ask) final {
        auto fut = get_units(_c.get()._memory, ask);
        if (_c.get()._memory.waiters()) {
            _c.get()._probe.waiting_for_available_memory();
        }
        return fut;
    }
    const header& get_header() const final { return _h; }
    void signal_body_parse() final { pr.set_value(); }
    std::reference_wrapper<client> _c;
    header _h;
    promise<> pr;
};

base_client::base_client(configuration c)
  : _server_addr(std::move(c.server_addr))
  , _creds(
      c.credentials ? c.credentials->build_certificate_credentials()
                    : nullptr) {}

client::client(client_configuration c, std::optional<sstring> service_name)
  : base_client(base_client::configuration{
    .server_addr = std::move(c.server_addr),
    .credentials = std::move(c.credentials),
  })
  , _memory(c.max_queued_bytes) {
    // setup_metrics(service_name);
}

future<> base_client::do_connect() {
    // hold invariant of having an always valid dispatch gate
    // and make sure we don't have a live connection already
    if (is_valid() || _dispatch_gate.is_closed()) {
        return make_exception_future<>(std::runtime_error(fmt::format(
          "cannot do_connect with a valid connection. remote:{}",
          server_address())));
    }
    return engine()
      .net()
      .connect(
        server_address(),
        socket_address(sockaddr_in{AF_INET, INADDR_ANY, {0}}),
        transport::TCP)
      .then([this](connected_socket fd) mutable {
          if (_creds) {
              return tls::wrap_client(_creds, std::move(fd));
          }
          return make_ready_future<connected_socket>(std::move(fd));
      })
      .then_wrapped([this](future<connected_socket> f_fd) mutable {
          try {
              _fd = std::make_unique<connected_socket>(std::move(f_fd.get0()));
          } catch (...) {
              auto e = std::current_exception();
              _probe.connection_error(e);
              throw e;
          }
          _probe.connection_established();
          _in = std::move(_fd->input());
          _out = batched_output_stream(std::move(_fd->output()));
      });
}
future<> base_client::connect() {
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
future<> base_client::stop() {
    fail_outstanding_futures();
    return _dispatch_gate.close();
}
void client::fail_outstanding_futures() {
    // must close the socket
    shutdown();
    for (auto& [_, p] : _correlations) {
        p.set_exception(std::runtime_error("failing outstanding futures"));
    }
    _correlations.clear();
}
void base_client::shutdown() {
    try {
        if (_fd) {
            _fd->shutdown_input();
            _fd->shutdown_output();
            _fd.reset();
        }
    } catch (...) {
        rpclog.debug("Failed to shutdown client: {}", std::current_exception());
    }
}

future<> client::connect() {
    return base_client::connect().then([this] {
        _correlation_idx = 0;
        // background
        (void)with_gate(_dispatch_gate, [this] {
            return do_reads().then_wrapped([this](future<> f) {
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

future<std::unique_ptr<streaming_context>> client::send(netbuf b) {
    // hold invariant of always having a valid connection _and_ a working
    // dispatch gate where we can wait for async futures
    if (!is_valid() || _dispatch_gate.is_closed()) {
        return make_exception_future<std::unique_ptr<streaming_context>>(
          std::runtime_error(fmt::format(
            "cannot send payload with invalid connection. remote:{}",
            server_address())));
    }
    return with_gate(_dispatch_gate, [this, b = std::move(b)]() mutable {
        if (_correlations.find(_correlation_idx + 1) != _correlations.end()) {
            _probe.client_correlation_error();
            throw std::runtime_error(
              "Invalid client state. Doubly registered correlation_id");
        }
        const uint32_t idx = ++_correlation_idx;
        promise_t item;
        // capture the future _before_ inserting promise in the map
        // in case there is a concurrent error w/ the connection and it fails
        // the future before we return from this function
        auto fut = item.get_future();
        b.set_correlation_id(idx);
        _correlations.emplace(idx, std::move(item));

        // send
        auto view = std::move(b).as_scattered();
        const auto sz = view.size();
        return get_units(_memory, sz)
          .then([this, v = std::move(view), f = std::move(fut)](
                  semaphore_units<> units) mutable {
              /// background
              (void)with_gate(
                _dispatch_gate,
                [this, v = std::move(v), u = std::move(units)]() mutable {
                    auto msg_size = v.size();
                    return _out.write(std::move(v))
                      .then([this, msg_size, u = std::move(u)] {
                          _probe.request_sent();
                          _probe.add_bytes_sent(msg_size);
                      });
                });
              return std::move(f);
          });
    });
}

future<> client::do_reads() {
    return do_until(
      [this] { return !is_valid(); },
      [this] {
          return parse_header(_in).then([this](std::optional<header> h) {
              if (!h) {
                  rpclog.debug(
                    "could not parse header from server: {}", server_address());
                  _probe.header_corrupted();
                  return make_ready_future<>();
              }
              return dispatch(std::move(h.value()));
          });
      });
}

/// - this needs a streaming_context.
///
future<> client::dispatch(header h) {
    static constexpr auto header_size = sizeof(header);
    auto it = _correlations.find(h.correlation_id);
    if (it == _correlations.end()) {
        // the background future on connect will fail all outstanding futures
        // and close the connection
        _probe.server_correlation_error();
        return make_exception_future<>(std::runtime_error(
          fmt::format("cannot find correlation_id: {}", h.correlation_id)));
    }
    _probe.add_bytes_received(header_size + h.size);
    auto ctx = std::make_unique<client_context_impl>(*this, std::move(h));
    auto fut = ctx->pr.get_future();
    // delete before setting value so that we don't run into nested exceptions
    // of broken promises
    auto pr = std::move(it->second);
    _correlations.erase(it);
    pr.set_value(std::move(ctx));
    return fut.then([this] { _probe.request_completed(); });
}

void client::setup_metrics(const std::optional<sstring>& service_name) {
    _probe.setup_metrics(_metrics, service_name, server_address());
}

client::~client() {
    rpclog.debug("RPC Client probes: {}", _probe);
    if (is_valid()) {
        rpclog.error(
          "connection '{}' is still valid. must call stop() before destroying",
          server_address());
        std::terminate();
    }
}

} // namespace rpc
