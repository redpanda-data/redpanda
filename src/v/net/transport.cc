#include "net/transport.h"

#include "base/compiler_utils.h"
#include "base/vassert.h"
#include "base/vlog.h"
#include "net/dns.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/with_timeout.hh>

#include <string_view>
#include <system_error>

namespace {

class timed_out_error : public ss::timed_out_error {
public:
    explicit timed_out_error(ss::sstring msg)
      : _msg{std::move(msg)} {}
    const char* what() const noexcept override { return _msg.c_str(); }

private:
    ss::sstring _msg;
};

ss::future<ss::connected_socket> connect_with_timeout(
  const seastar::socket_address& address,
  net::clock_type::time_point timeout,
  seastar::logger* log) {
    auto socket = ss::make_lw_shared<ss::socket>(ss::engine().net().socket());
    auto f = socket->connect(address).finally([socket] {});
    return ss::with_timeout(timeout, std::move(f))
      .handle_exception([socket, address, log](const std::exception_ptr& e) {
          try {
              std::rethrow_exception(e);
          } catch (const ss::timed_out_error& ex) {
              socket->shutdown();
              return ss::make_exception_future<ss::connected_socket>(
                timed_out_error(
                  ssx::sformat("connection to {} - {}", address, e)));
          } catch (const std::system_error& ex) {
              socket->shutdown();
              return ss::make_exception_future<ss::connected_socket>(
                std::system_error(
                  ex.code(), fmt::format("connection to {}", address)));
          } catch (...) {
              vlog(log->trace, "error connecting to {} - {}", address, e);
              socket->shutdown();
              return ss::make_exception_future<ss::connected_socket>(e);
          }
      });
}

/// Sends an HTTP CONNECT request over `out`/`in` and reads the
/// response. Throws net::proxy_connect_error on non-200 status,
/// malformed response, or transport error. Does not close the
/// streams; the caller owns their lifetime.
ss::future<> send_connect_and_read_response(
  ss::output_stream<char>& out,
  ss::input_stream<char>& in,
  const net::unresolved_address& origin,
  const net::unresolved_address& proxy,
  seastar::logger* log) {
    // IPv6 literals must be bracketed in request authority
    // (RFC 9112 §3.2 / RFC 3986 §3.2.2); see format_connect_authority.
    auto authority = net::detail::format_connect_authority(
      origin.host(), origin.port());
    auto request = fmt::format(
      "CONNECT {} HTTP/1.1\r\n"
      "Host: {}\r\n"
      "\r\n",
      authority,
      authority);

    vlog(
      log->trace, "Sending CONNECT to proxy {} for origin {}", proxy, origin);

    co_await out.write(request);
    co_await out.flush();

    net::detail::connect_response_parser parser;
    co_await in.consume(parser);

    if (parser.limit_exceeded) {
        throw net::proxy_connect_error(
          proxy, origin, "proxy response headers exceeded size limits");
    }
    if (!parser.saw_terminator || !parser.saw_status) {
        throw net::proxy_connect_error(
          proxy,
          origin,
          parser.status_line.empty()
            ? "proxy closed connection before sending status line"
            : "proxy closed connection mid-headers");
    }

    // Status line is "HTTP/1.x NNN <reason>"; we require exactly 200.
    std::string_view sl(parser.status_line);
    if (!sl.starts_with("HTTP/1.")) {
        throw net::proxy_connect_error(
          proxy,
          origin,
          fmt::format("malformed status line: {}", parser.status_line));
    }
    auto sp1 = sl.find(' ');
    if (sp1 == std::string_view::npos || sl.size() < sp1 + 4) {
        throw net::proxy_connect_error(
          proxy,
          origin,
          fmt::format("malformed status line: {}", parser.status_line));
    }
    auto code = sl.substr(sp1 + 1, 3);
    if (code != "200") {
        throw net::proxy_connect_error(
          proxy, origin, fmt::format("non-200 status: {}", parser.status_line));
    }
}

} // namespace

namespace net {

namespace detail {

std::string format_connect_authority(std::string_view host, uint16_t port) {
    bool is_unbracketed_ipv6 = host.find(':') != std::string_view::npos
                               && !(
                                 host.starts_with('[') && host.ends_with(']'));
    return is_unbracketed_ipv6 ? fmt::format("[{}]:{}", host, port)
                               : fmt::format("{}:{}", host, port);
}

ss::future<connect_response_parser::result_t>
connect_response_parser::operator()(ss::temporary_buffer<char> buf) {
    if (buf.empty()) {
        return ss::make_ready_future<result_t>(ss::stop_consuming<char>({}));
    }
    size_t i = 0;
    while (i < buf.size() && !saw_terminator && !limit_exceeded) {
        char c = buf.get()[i++];
        if (
          ++total_bytes > max_total_bytes
          || current_line.size() >= max_line_bytes) {
            limit_exceeded = true;
            break;
        }
        current_line.append(&c, 1);
        if (current_line.ends_with("\r\n")) {
            current_line.resize(current_line.size() - 2);
            if (!saw_status) {
                status_line = std::move(current_line);
                saw_status = true;
            } else if (current_line.empty()) {
                saw_terminator = true;
                break;
            }
            current_line = {};
        }
    }
    if (saw_terminator || limit_exceeded) {
        return ss::make_ready_future<result_t>(
          ss::stop_consuming<char>(buf.share(i, buf.size() - i)));
    }
    return ss::make_ready_future<result_t>(ss::continue_consuming{});
}

} // namespace detail

base_transport::base_transport(configuration c, seastar::logger* log)
  : _server_addr(c.server_addr)
  , _creds(c.credentials)
  , _tls_sni_hostname(c.tls_sni_hostname)
  , _wait_for_tls_server_eof(c.wait_for_tls_server_eof)
  , _proxy(std::move(c.proxy))
  , _log(log) {
    // Plaintext origins through a proxy aren't supported. dassert
    // surfaces the misuse as a SIGABRT in CI; throw so a future
    // caller's misuse fails the request rather than the broker.
    dassert(
      !_proxy.has_value() || _creds,
      "base_transport configured with proxy but without credentials");
    if (_proxy.has_value() && !_creds) {
        throw std::invalid_argument(
          "base_transport configured with proxy but without credentials; "
          "plaintext origins through a proxy are not supported");
    }
}

ss::future<> base_transport::do_connect(clock_type::time_point timeout) {
    // hold invariant of having an always valid dispatch gate
    // and make sure we don't have a live connection already
    if (is_valid() || _dispatch_gate.is_closed()) {
        throw std::runtime_error(
          fmt::format(
            "cannot do_connect with a valid connection. remote:{}",
            server_address()));
    }
    try {
        base_transport::reset_state();
        reset_state();
        const auto& tcp_target = _proxy.has_value() ? _proxy->address
                                                    : server_address();
        auto resolved_address = co_await net::resolve_dns(tcp_target);
        vlog(_log->trace, "Resolved address {}", resolved_address);
        ss::connected_socket fd = co_await connect_with_timeout(
          resolved_address, timeout, _log);

        if (_proxy.has_value() && _proxy->credentials) {
            // https:// proxy: TLS-wrap to the proxy before CONNECT.
            // SNI HostName must not be an IPv4/IPv6 literal (RFC 3546
            // §3.1 / RFC 6066 §3); omit it when the proxy is addressed
            // by IP.
            const auto& proxy_host = _proxy->address.host();
            auto proxy_sni
              = ss::net::inet_address::parse_numerical(proxy_host).has_value()
                  ? ss::sstring{}
                  : proxy_host;
            // CORE-14958
            REDPANDA_BEGIN_IGNORE_DEPRECATIONS
            fd = co_await ss::tls::wrap_client(
              _proxy->credentials,
              std::move(fd),
              ss::tls::tls_options{
                .wait_for_eof_on_shutdown = _wait_for_tls_server_eof,
                .server_name = std::move(proxy_sni)});
            REDPANDA_END_IGNORE_DEPRECATIONS
        }

        if (_proxy.has_value()) {
            _proxy_out.emplace(fd.output());
            auto proxy_in = fd.input();
            co_await send_connect_and_read_response(
              *_proxy_out, proxy_in, server_address(), _proxy->address, _log);
        }

        if (_creds) {
            // CORE-14958
            REDPANDA_BEGIN_IGNORE_DEPRECATIONS
            fd = co_await ss::tls::wrap_client(
              _creds,
              std::move(fd),
              ss::tls::tls_options{
                .wait_for_eof_on_shutdown = _wait_for_tls_server_eof,
                .server_name = _tls_sni_hostname.value_or("")});
            REDPANDA_END_IGNORE_DEPRECATIONS
        }
        _fd = std::make_unique<ss::connected_socket>(std::move(fd));
        if (auto* p = _probe.value_or(nullptr); p != nullptr) {
            p->connection_established();
        }
        _in = _fd->input();

        // Never implicitly destroy a live output stream here: output streams
        // are only safe to destroy after/during stop()
        vassert(
          !_out.has_value() || !_out->is_valid(),
          "destroyed output_stream without stopping");
        _out = net::batched_output_stream(_fd->output());
    } catch (...) {
        auto e = std::current_exception();
        if (auto* p = _probe.value_or(nullptr); p != nullptr) {
            p->connection_error();
        }
        vlog(_log->trace, "Connection error: {}", e);
        std::rethrow_exception(e);
    }

    co_return;
}

void base_transport::set_keepalive_parameters(
  const ss::net::keepalive_params& params) {
    if (_fd) {
        _fd->set_keepalive_parameters(params);
    }
}

void base_transport::set_keepalive(bool keepalive) {
    if (_fd) {
        _fd->set_keepalive(keepalive);
    }
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

    co_await _dispatch_gate.close();

    // We must call stop() on our output stream, because
    // seastar::output_stream may not be safely destroyed without a call to
    // close(), and this class may be destroyed after stop() is called.

    try {
        if (_out.has_value()) {
            co_await _out->stop();
        }
    } catch (...) {
        // Closing the output stream can throw bad pipe if
        // it had unflushed bytes, as we already closed FD.
        vlog(
          _log->debug,
          "Exception while stopping transport: {}",
          std::current_exception());
    }

    // Set _out to nullopt here, so that do_connect can assert that
    // it isn't dropping an un-stopped output stream when it
    // assigns to _out. Note that this happens even if _out->stop()
    // above throws: because the most common case is that the flush
    // implied by stop(), but close() still closes the stream in that
    // case using a finally. So though we don't *know* if stop() closed
    // the underlying stream, we *hope* it did.
    _out = std::nullopt;

    // _proxy_out closes before _in so _in->close() stays the last
    // action: if it throws, no further cleanup is left dangling.
    try {
        if (_proxy_out.has_value()) {
            co_await _proxy_out->close();
        }
    } catch (...) {
        vlog(
          _log->debug,
          "Exception while closing proxy output stream: {}",
          std::current_exception());
    }
    _proxy_out = std::nullopt;

    if (_in.has_value()) {
        co_await _in->close();
        _in = std::nullopt;
    }
}

void base_transport::shutdown() noexcept {
    try {
        if (_fd && !std::exchange(_shutdown, true)) {
            _fd->shutdown_input();
            _fd->shutdown_output();
        }
    } catch (...) {
        vlog(
          _log->debug,
          "Failed to shutdown transport: {}",
          std::current_exception());
    }
}

ss::future<> base_transport::wait_input_shutdown() {
    if (_fd && _shutdown) {
        co_return co_await _fd->wait_input_shutdown();
    }
}

void base_transport::set_probe(client_probe* probe) {
    vassert(!_probe.has_value(), "Transport already has registered probe");
    _probe = probe;
}

} // namespace net
