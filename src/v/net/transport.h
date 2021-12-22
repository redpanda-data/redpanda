/*
 * Copyright 2021 Vectorized, Inc.
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
#include "rpc/client_probe.h"
#include "rpc/types.h"
#include "seastarx.h"
#include "utils/unresolved_address.h"

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
 * TODO:
 *  - clock_type should migrate to net::
 *  - client_probe needs to be split apart from simple_protocol
 *  - allow subclasses to provide logger
 */
using clock_type = rpc::clock_type;

class base_transport {
public:
    struct configuration {
        unresolved_address server_addr;
        ss::shared_ptr<ss::tls::certificate_credentials> credentials;
        rpc::metrics_disabled disable_metrics = rpc::metrics_disabled::no;
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
    ss::future<> stop();
    void shutdown() noexcept;

    [[gnu::always_inline]] bool is_valid() const { return _fd && !_in.eof(); }

    const unresolved_address& server_address() const { return _server_addr; }

protected:
    virtual void fail_outstanding_futures() {}

    ss::input_stream<char> _in;
    net::batched_output_stream _out;
    ss::gate _dispatch_gate;
    rpc::client_probe _probe;

private:
    ss::future<> do_connect(clock_type::time_point);

    std::unique_ptr<ss::connected_socket> _fd;
    unresolved_address _server_addr;
    ss::shared_ptr<ss::tls::certificate_credentials> _creds;
    std::optional<ss::sstring> _tls_sni_hostname;
};

} // namespace net
