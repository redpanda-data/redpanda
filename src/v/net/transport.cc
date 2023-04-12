#include "net/transport.h"

#include "net/dns.h"
#include "rpc/logger.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/with_timeout.hh>

namespace {

ss::future<ss::connected_socket> connect_with_timeout(
  const seastar::socket_address& address, net::clock_type::time_point timeout) {
    auto socket = ss::make_lw_shared<ss::socket>(ss::engine().net().socket());
    auto f = socket->connect(address).finally([socket] {});
    return ss::with_timeout(timeout, std::move(f))
      .handle_exception([socket, address](const std::exception_ptr& e) {
          rpc::rpclog.trace("error connecting to {} - {}", address, e);
          socket->shutdown();
          return ss::make_exception_future<ss::connected_socket>(e);
      });
}

} // namespace

namespace net {

base_transport::base_transport(configuration c)
  : _server_addr(c.server_addr)
  , _creds(c.credentials)
  , _tls_sni_hostname(c.tls_sni_hostname) {}

ss::future<> base_transport::do_connect(clock_type::time_point timeout) {
    // hold invariant of having an always valid dispatch gate
    // and make sure we don't have a live connection already
    if (is_valid() || _dispatch_gate.is_closed()) {
        throw std::runtime_error(fmt::format(
          "cannot do_connect with a valid connection. remote:{}",
          server_address()));
    }
    try {
        base_transport::reset_state();
        reset_state();
        auto resolved_address = co_await net::resolve_dns(server_address());
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
        _out = net::batched_output_stream(_fd->output());
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
                  rpc::rpclog.debug,
                  "Exception while stopping transport: {}",
                  std::current_exception());
            }
            _out = {};
        });
    });
}

void base_transport::shutdown() noexcept {
    try {
        if (_fd && !std::exchange(_shutdown, true)) {
            _fd->shutdown_input();
            _fd->shutdown_output();
        }
    } catch (...) {
        vlog(
          rpc::rpclog.debug,
          "Failed to shutdown transport: {}",
          std::current_exception());
    }
}

ss::future<> base_transport::wait_input_shutdown() {
    if (_fd && _shutdown) {
        co_return co_await _fd->wait_input_shutdown();
    }
}

} // namespace net
