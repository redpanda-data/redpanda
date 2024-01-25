/*
 * Copyright 2024 Redpanda Data, Inc.
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
#include "connection.h"
#include "ossl_context_service.h"
#include "ssl_utils.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/api.hh>
#include <seastar/net/socket_defs.hh>

/**
 * This service will accept new connections from clients and perform the SSL
 * handshake and then echo back any data it receives
 */
class ossl_tls_service final {
public:
    ossl_tls_service(
      ss::sharded<ossl_context_service>& ssl_ctx_service,
      ss::socket_address addr,
      ss::sstring key_path,
      ss::sstring cert_path);
    ~ossl_tls_service() = default;

    ossl_tls_service(const ossl_tls_service&) = delete;
    ossl_tls_service(ossl_tls_service&&) = delete;
    ossl_tls_service& operator=(const ossl_tls_service&) = delete;
    ossl_tls_service& operator=(ossl_tls_service&&) = delete;

    ss::future<> start();
    ss::future<> shutdown_input();
    ss::future<> wait_for_shutdown();
    ss::future<> stop();

    ss::abort_source& abort_source() { return _as; }

private:
    ss::sharded<ossl_context_service>& _ssl_ctx_service;
    std::optional<ss::server_socket> _listener;
    boost::intrusive::list<connection> _connections;
    ss::abort_source _as;
    ss::gate _accept_gate;
    ss::gate _conn_gate;
    ss::socket_address _addr;
    SSL_CTX_ptr _ssl_ctx;
    ss::sstring _key_path;
    ss::sstring _cert_path;

private:
    ss::future<> accept();
    ss::future<ss::stop_iteration> accept_finish(ss::future<ss::accept_result>);
    ss::future<> apply_proto(ss::lw_shared_ptr<connection>);
};
