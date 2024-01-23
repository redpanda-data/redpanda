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
#include "ossl-poc/ssl_utils.h"
#include "ossl_tls_service.h"
#include "ssx/future-util.h"

#include <seastar/core/loop.hh>
#include <seastar/core/scattered_message.hh>
#include <seastar/core/temporary_buffer.hh>

#include <openssl/bio.h>
#include <openssl/ssl.h>

#include <limits>

context::context(
  std::optional<std::reference_wrapper<boost::intrusive::list<context>>> hook,
  class ossl_tls_service& ossl_tls_service,
  ss::lw_shared_ptr<connection> conn)
  : _hook(std::move(hook))
  , _ossl_tls_service(ossl_tls_service)
  , _conn(conn)
  , _as()
  , _rbio(BIO_new(BIO_s_mem()))
  , _wbio(BIO_new(BIO_s_mem()))
  , _ssl(SSL_new(_conn->ssl_ctx().get()), SSL_free) {
    if (!_ssl) {
        throw ossl_error("Failed to create SSL from context");
    }
    SSL_set_accept_state(_ssl.get());
    // SSL_set_bio transfers ownership of the read and write bios to the SSL
    // instance
    SSL_set_bio(_ssl.get(), _rbio, _wbio);
}

ss::future<> context::start() {
    co_await _as.start(_ossl_tls_service.abort_source());
    if (_conn) {
        ssx::background
          = _conn->wait_for_input_shutdown()
              .finally([this] {
                  lg.info("Shutting down connection");
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

    lg.info("Context stopped...");
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
    on_read(std::move(buf));

    if (!_write_buf.empty()) {
        lg.info("Needing to send {} bytes", _write_buf.size_bytes());
        auto in = iobuf::iterator_consumer(
          _write_buf.cbegin(), _write_buf.cend());
        ss::scattered_message<char> msg;
        int chunk_no = 0;
        in.consume(
          _write_buf.size_bytes(),
          [&msg, &chunk_no, this](const char* src, size_t sz) {
              ++chunk_no;
              vassert(
                chunk_no <= std::numeric_limits<int16_t>::max(),
                "Invalid construction of scattered_message. max count: {}, "
                "Usually a bug with small append() to iobuf. {}",
                chunk_no,
                _write_buf);
              msg.append_static(src, sz);
              return ss::stop_iteration::no;
          });
        co_await _conn->output().write(std::move(msg));
        co_await _conn->output().flush();
    }
}

context::ssl_status context::get_sslstatus(SSL* ssl, int n) {
    switch (SSL_get_error(ssl, n)) {
    case SSL_ERROR_NONE:
        return context::ssl_status::SSLSTATUS_OK;
    case SSL_ERROR_WANT_WRITE:
    case SSL_ERROR_WANT_READ:
        return context::ssl_status::SSLSTATUS_WANT_IO;
    default:
        return context::ssl_status::SSLSTATUS_FAIL;
    }
}

void context::on_read(ss::temporary_buffer<char> srcb) {
    std::array<char, 64> buf{};
    int n;
    int len = srcb.size();
    const char* src = srcb.get();
    ssl_status status;

    while (len > 0) {
        n = BIO_write(_rbio, src, len);
        if (n <= 0) {
            throw ossl_error("Failed to write to OSSL BIO buffer");
        }

        lg.info("Wrote {} bytes to BIO", n);
        src += n;
        len -= n;

        if (!SSL_is_init_finished(_ssl.get())) {
            lg.info("Not yet finished");
            n = SSL_accept(_ssl.get());
            status = get_sslstatus(_ssl.get(), n);

            if (status == ssl_status::SSLSTATUS_WANT_IO) {
                do {
                    n = BIO_read(_wbio, buf.data(), buf.size());
                    if (n > 0) {
                        lg.info("Appending {} bytes to write buffer", n);
                        _write_buf.append(buf.data(), n);
                    } else if (!BIO_should_retry(_wbio)) {
                        throw ossl_error("Failure during bio read");
                    }
                } while (n > 0);
            } else if (status == ssl_status::SSLSTATUS_FAIL) {
                throw ossl_error("Error during SSL accept");
            }

            if (!SSL_is_init_finished(_ssl.get())) {
                lg.info("Still not done");
                return;
            }
        } else {
            lg.info("SSL is initialized");
        }

        do {
            n = SSL_read(_ssl.get(), buf.data(), buf.size());
            lg.info("SSL_read: {}", n);
            if (n > 0) {
                lg.info("Read {} bytes from SSL", n);
            }
        } while (n > 0);

        status = get_sslstatus(_ssl.get(), n);

        if (status == ssl_status::SSLSTATUS_WANT_IO) {
            lg.info("Possible peer renegotiation");
            do {
                n = BIO_read(_wbio, buf.data(), buf.size());
                if (n > 0) {
                    lg.info(
                      "Negotiation appending {} bytes to write buffer", n);
                    _write_buf.append(buf.data(), n);
                } else if (!BIO_should_retry(_wbio)) {
                    throw ossl_error("Error while attempting BIO read");
                }
            } while (n > 0);
        }

        if (status == ssl_status::SSLSTATUS_FAIL) {
            throw ossl_error("Error with SSL reading");
        }
    }
}
