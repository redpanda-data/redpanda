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
#include "net/batched_output_stream.h"
#include "net/client_probe.h"
#include "net/types.h"
#include "net/unresolved_address.h"
#include "seastarx.h"

#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/api.hh>
#include <seastar/net/tls.hh>

#include <memory>
#include <optional>

namespace net {

/*
 * Wrapper around a network socket that encapsulates setting up an initial
 * connection with some credentials.
 *
 * This class only provides an interface with which to establish a connection
 * with the socket. As such, superclasses must provide interfaces with which to
 * send and receive bytes using the socket.
 *
 * TODO:
 *  - client_probe needs to be split apart from simple_protocol
 *  - allow subclasses to provide logger
 */
class base_transport {
public:
    struct configuration {
        unresolved_address server_addr;
        ss::shared_ptr<ss::tls::certificate_credentials> credentials;
        net::metrics_disabled disable_metrics = net::metrics_disabled::no;
        net::public_metrics_disabled disable_public_metrics
          = net::public_metrics_disabled::no;
        /// Optional server name indication (SNI) for TLS connection
        std::optional<ss::sstring> tls_sni_hostname;
    };

    explicit base_transport(configuration c);
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

    [[gnu::always_inline]] bool is_valid() const {
        return _fd && !_shutdown && !_in.eof();
    }

    const unresolved_address& server_address() const { return _server_addr; }

protected:
    virtual void fail_outstanding_futures() {}

    ss::input_stream<char> _in;
    net::batched_output_stream _out;
    ss::gate _dispatch_gate;
    client_probe _probe;

private:
    ss::future<> do_connect(clock_type::time_point);

    std::unique_ptr<ss::connected_socket> _fd;
    unresolved_address _server_addr;
    ss::shared_ptr<ss::tls::certificate_credentials> _creds;
    std::optional<ss::sstring> _tls_sni_hostname;

    // Track if shutdown was called on the current `_fd`
    bool _shutdown{false};
};

} // namespace net
