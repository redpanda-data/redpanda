/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once
#include "base/seastarx.h"
#include "base/vassert.h"
#include "net/batched_output_stream.h"
#include "net/client_probe.h"
#include "net/types.h"
#include "utils/unresolved_address.h"

#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/api.hh>
#include <seastar/net/tls.hh>
#include <seastar/util/log.hh>

#include <fmt/format.h>

#include <memory>
#include <optional>
#include <stdexcept>

namespace net {

/// Thrown when the forward-proxy CONNECT handshake fails. Names the
/// proxy and the origin so operator logs identify which hop failed.
class proxy_connect_error : public std::runtime_error {
public:
    proxy_connect_error(
      const unresolved_address& proxy,
      const unresolved_address& origin,
      std::string_view detail)
      : std::runtime_error(
          fmt::format(
            "proxy {} failed to CONNECT to origin {}: {}",
            proxy,
            origin,
            detail)) {}
};

/*
 * Wrapper around a network socket that encapsulates setting up an initial
 * connection with some credentials.
 *
 * This class only provides an interface with which to establish a connection
 * with the socket. As such, superclasses must provide interfaces with which to
 * send and receive bytes using the socket.
 *
 * Metric probes
 * -------------
 *
 * A subclass should of net::base_transport should inherit from
 * net::client_probe and add its own probes. A pointer to the superclass should
 * be passed into base_transport::set_probe.
 */
class base_transport {
public:
    struct configuration {
        /// Optional forward-proxy configuration. When set, do_connect
        /// connects to the proxy (optionally via TLS for https://
        /// proxies) and issues an HTTP CONNECT request for server_addr.
        /// The origin TLS handshake (configuration::credentials) then
        /// runs inside the tunnel; plaintext origins through a proxy
        /// are not supported.
        struct proxy_config {
            unresolved_address address;
            /// If non-null, the proxy socket is TLS-wrapped with these
            /// credentials before CONNECT is sent (https:// proxy).
            /// Distinct from configuration::credentials, which applies
            /// to the origin TLS handshake inside the CONNECT tunnel.
            /// SNI for the TLS handshake is derived from address.host().
            ss::shared_ptr<ss::tls::certificate_credentials> credentials;
            /// Opaque Proxy-Authorization header value (e.g.
            /// "Basic <base64>") sent on the CONNECT request. The
            /// transport emits it verbatim and does not interpret it.
            /// std::nullopt means no Proxy-Authorization header is sent.
            std::optional<ss::sstring> authorization;
        };

        unresolved_address server_addr;
        ss::shared_ptr<ss::tls::certificate_credentials> credentials;
        net::metrics_disabled disable_metrics = net::metrics_disabled::no;
        net::public_metrics_disabled disable_public_metrics
          = net::public_metrics_disabled::no;
        /// Optional server name indication (SNI) for TLS connection
        std::optional<ss::sstring> tls_sni_hostname;
        /// Potentially skip wait for EOF after BYE message on TLS session end
        bool wait_for_tls_server_eof = true;
        std::optional<proxy_config> proxy;
    };

    base_transport(configuration c, seastar::logger* log);

    virtual ~base_transport() noexcept = default;
    base_transport(base_transport&&) noexcept = default;
    base_transport& operator=(base_transport&&) noexcept = default;
    base_transport(const base_transport&) = delete;
    base_transport& operator=(const base_transport&) = delete;

    virtual ss::future<>
      connect(clock_type::time_point = clock_type::time_point::max());

    // override this method to reset internal state when connection attempt is
    // being made
    virtual void reset_state() {
        _fd.reset();
        _shutdown = false;
    }

    ss::future<> stop();
    void shutdown() noexcept;
    ss::future<> wait_input_shutdown();

    void set_keepalive_parameters(const ss::net::keepalive_params& params);
    void set_keepalive(bool);

    [[gnu::always_inline]] bool is_valid() const {
        return _fd && !_shutdown && (_in && !in().eof());
    }

    const unresolved_address& server_address() const { return _server_addr; }

    /*
     * Sets the probe instance to use. Must only be called once.
     */
    void set_probe(client_probe*);

    bool has_tls() const { return static_cast<bool>(_creds); }
    bool has_proxy() const { return _proxy.has_value(); }

protected:
    virtual void fail_outstanding_futures() {}

    // Return the input stream associated with the transport.
    // The transport must not be in initial/stopped state.
    const ss::input_stream<char>& in() const {
        vassert(_in.has_value(), "input stream not initialized");
        return *_in;
    }

    ss::input_stream<char>& in() {
        vassert(_in.has_value(), "input stream not initialized");
        return *_in;
    }

    const net::batched_output_stream& out() const {
        vassert(_out.has_value(), "output stream not initialized");
        return *_out;
    }

    net::batched_output_stream& out() {
        vassert(_out.has_value(), "output stream not initialized");
        return *_out;
    }

    ss::gate _dispatch_gate;

private:
    ss::future<> do_connect(clock_type::time_point);

    std::unique_ptr<ss::connected_socket> _fd;
    std::optional<ss::input_stream<char>> _in;
    std::optional<net::batched_output_stream> _out;

    // CONNECT request stream. Closed in stop(); writing through it
    // after do_connect would bypass origin TLS.
    std::optional<ss::output_stream<char>> _proxy_out;

    unresolved_address _server_addr;
    ss::shared_ptr<ss::tls::certificate_credentials> _creds;
    std::optional<ss::sstring> _tls_sni_hostname;
    bool _wait_for_tls_server_eof;
    std::optional<configuration::proxy_config> _proxy;
    seastar::logger* _log;

    // Track if shutdown was called on the current `_fd`
    bool _shutdown{false};
    std::optional<client_probe*> _probe;
};

namespace detail {

/// Formats a `host:port` authority for an HTTP CONNECT request line,
/// bracketing IPv6 literals as required by RFC 9112 §3.2 / RFC 3986
/// §3.2.2. Hosts already bracketed (e.g. `[::1]`) are left alone; a
/// colon in an unbracketed host is treated as an IPv6 literal marker.
std::string format_connect_authority(std::string_view host, uint16_t port);

/// Builds the CONNECT request line + headers for `authority`
/// (host:port). When `authorization` is set, includes a
/// Proxy-Authorization header. Terminated with the blank line.
std::string format_connect_request(
  std::string_view authority, const std::optional<ss::sstring>& authorization);

/// Consumes an HTTP CONNECT response in a single pass, capturing the
/// status line and discarding header values. Bounds per-line length
/// and total response-header bytes against a misbehaving proxy.
struct connect_response_parser {
    static constexpr size_t max_line_bytes = 8 * 1024;
    static constexpr size_t max_total_bytes = 32 * 1024;

    ss::sstring status_line;
    ss::sstring current_line;
    size_t total_bytes = 0;
    bool saw_status = false;
    bool saw_terminator = false;
    bool limit_exceeded = false;

    using result_t = ss::consumption_result<char>;

    ss::future<result_t> operator()(ss::temporary_buffer<char> buf);
};

} // namespace detail

} // namespace net
