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

#include "context.h"

#include "logger.h"
#include "ossl_tls_service.h"
#include "ssx/future-util.h"

context::context(
  std::optional<std::reference_wrapper<boost::intrusive::list<context>>> hook,
  class ossl_tls_service& ossl_tls_service,
  ss::lw_shared_ptr<connection> conn)
  : _hook(std::move(hook))
  , _ossl_tls_service(ossl_tls_service)
  , _conn(conn)
  , _as()
  , _ssl(SSL_new(_conn->ssl_ctx().get()), SSL_free) {
    if (!_ssl) {
        throw ossl_error("Failed to create SSL from context");
    }
}

ss::future<> context::start() {
    co_await _as.start(_ossl_tls_service.abort_source());
    if (_conn) {
        ssx::background
          = _conn->wait_for_input_shutdown()
              .finally([this] {
                  lg.debug("Shutting down connection");
                  return _as.request_abort_ex(std::system_error{
                    std::make_error_code(std::errc::connection_aborted)});
              })
              .finally([this] { _wait_input_shutdown.set_value(); });
    } else {
        _wait_input_shutdown.set_value();
    }

    if (_hook) {
        _hook->get().push_back(*this);
    }
}

ss::future<> context::stop() {
    if (_hook) {
        _hook->get().erase(_hook->get().iterator_to(*this));
    }
    if (_conn) {
        _conn->shutdown_input();
    }

    co_await _wait_input_shutdown.get_future();
    co_await _as.request_abort_ex(ssx::connection_aborted_exception{});
    co_await _as.stop();
}

bool context::is_finished_parsing() const {
    return _conn->input().eof() || abort_requested();
}

ss::future<> context::process() {
    while (true) {
        if (is_finished_parsing()) {
            lg.info("context is finished parsing");
            break;
        }
        co_await process_one_request();
    }
}

ss::future<> context::process_one_request() {
    auto buf = co_await _conn->input().read();
    lg.info("Read {} bytes", buf.size());
    co_await _conn->output().write(std::move(buf));
    co_await _conn->output().flush();
}
