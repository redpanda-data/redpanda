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

#include "ossl_tls_service.h"

#include "context.h"
#include "logger.h"
#include "ossl-poc/ossl_context_service.h"
#include "ssx/abort_source.h"
#include "ssx/future-util.h"

#include <seastar/core/seastar.hh>

#include <openssl/ssl.h>

ossl_tls_service::ossl_tls_service(
  ss::sharded<ossl_context_service>& ssl_ctx_service,
  ss::socket_address addr,
  ss::sstring key_path,
  ss::sstring cert_path)
  : _ssl_ctx_service(ssl_ctx_service)
  , _addr(addr)
  , _ssl_ctx(
      SSL_CTX_new_ex(
        _ssl_ctx_service.local().get_ossl_context(), nullptr, TLS_method()),
      SSL_CTX_free)
  , _key_path(std::move(key_path))
  , _cert_path(std::move(cert_path)) {
    if (!_ssl_ctx) {
        throw ossl_error("Failed to create SSL context");
    }
}

ss::future<> ossl_tls_service::start() {
    lg.info("Starting TLS service...");

    SSL_CTX_set_verify(_ssl_ctx.get(), SSL_VERIFY_NONE, nullptr);
    auto x509 = co_await load_x509_from_file(_cert_path);
    auto pkey = co_await load_evp_pkey_from_file(_key_path);
    if (!SSL_CTX_use_cert_and_key(
          _ssl_ctx.get(), x509.get(), pkey.get(), nullptr, 1)) {
        throw ossl_error(fmt::format(
          "Failed to load key ({}) and cert ({})", _key_path, _cert_path));
    }

    if (!SSL_CTX_set_min_proto_version(_ssl_ctx.get(), TLS1_2_VERSION)) {
        throw ossl_error("Failed to set min version of TLSv1.2");
    }

    if (!SSL_CTX_set_ciphersuites(
          _ssl_ctx.get(),
          "TLS_AES_128_GCM_SHA256:TLS_AES_256_GCM_SHA384:TLS_AES_128_CCM_"
          "SHA256:TLS_AES_128_CCM_8_SHA256")) {
        throw ossl_error("Failed to set TLSv1.3 cipher suites");
    }

    ss::listen_options lo;
    lo.reuse_address = true;
    _listener = ss::listen(_addr, lo);

    ssx::spawn_with_gate(_accept_gate, [this]() { return accept(); });
}

ss::future<> ossl_tls_service::shutdown_input() {
    if (_listener) {
        _listener->abort_accept();
    }

    _as.request_abort_ex(ssx::shutdown_requested_exception{});
    co_await _accept_gate.close();
}

ss::future<> ossl_tls_service::wait_for_shutdown() {
    if (!_as.abort_requested()) {
        co_await shutdown_input();
    }

    co_return co_await _conn_gate.close().then([this] {
        return ss::do_for_each(
          _connections, [](connection& c) { return c.shutdown(); });
    });
}

ss::future<> ossl_tls_service::stop() {
    if (_as.abort_requested()) {
        co_return;
    }

    co_await wait_for_shutdown();
}

ss::future<> ossl_tls_service::accept() {
    return ss::repeat([this]() {
        return _listener->accept().then_wrapped(
          [this](ss::future<ss::accept_result> ar) {
              return accept_finish(std::move(ar));
          });
    });
}

ss::future<ss::stop_iteration>
ossl_tls_service::accept_finish(ss::future<ss::accept_result> f_ar) {
    if (_as.abort_requested()) {
        f_ar.ignore_ready_future();
        co_return ss::stop_iteration::yes;
    }

    auto ar = f_ar.get();
    ar.connection.set_nodelay(true);
    ar.connection.set_keepalive(true);

    lg.info("Accepted connection from {}", ar.remote_address);

    auto conn = ss::make_lw_shared<connection>(
      _connections, std::move(ar.connection), ar.remote_address, _ssl_ctx);

    _as.check();

    ssx::spawn_with_gate(
      _conn_gate, [this, conn] { return apply_proto(conn); });

    co_return ss::stop_iteration::no;
}

ss::future<> ossl_tls_service::apply_proto(ss::lw_shared_ptr<connection> c) {
    auto ctx = ss::make_lw_shared<context>(std::nullopt, *this, c);

    std::exception_ptr eptr;
    try {
        co_await ctx->start();
        co_await ctx->process();
    } catch (...) {
        eptr = std::current_exception();
    }

    if (!eptr) {
        co_await ctx->stop();
    } else {
        co_await ctx->abort_source().request_abort_ex(eptr);
        co_await ctx->stop();
    }
}
