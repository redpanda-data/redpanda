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
#include <seastar/core/sharded.hh>
#include <seastar/core/temporary_buffer.hh>

#include <openssl/bio.h>
#include <openssl/ssl.h>

#include <limits>
#include <memory>

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
        BIO_free(_wbio);
        BIO_free(_rbio);
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
    auto resp = on_read(std::move(buf));

    if (!resp->buf().empty()) {
        auto& buf = resp->buf();
        lg.debug("Needing to send {} bytes", buf.size_bytes());
        auto in = iobuf::iterator_consumer(buf.cbegin(), buf.cend());
        ss::scattered_message<char> msg;
        int chunk_no = 0;
        in.consume(
          buf.size_bytes(),
          [&msg, &chunk_no, &buf](const char* src, size_t sz) {
              ++chunk_no;
              vassert(
                chunk_no <= std::numeric_limits<int16_t>::max(),
                "Invalid construction of scattered_message.  max count: {}, "
                "Usually a bug with small append() to iobuf.  {}",
                chunk_no,
                buf);
              msg.append_static(src, sz);
              return ss::stop_iteration::no;
          });
        msg.on_delete([resp = std::move(resp)] {});
        co_await _conn->output().write(std::move(msg));
        co_await _conn->output().flush();
    }
}

context::ssl_status context::get_sslstatus(SSL* ssl, int n) {
    switch (SSL_get_error(ssl, n)) {
    case SSL_ERROR_NONE:
        return context::ssl_status::SSLSTATUS_OK;
    case SSL_ERROR_WANT_WRITE:
        return context::ssl_status::SSLSTATUS_WANT_WRITE;
    case SSL_ERROR_WANT_READ:
        return context::ssl_status::SSLSTATUS_WANT_READ;
    default:
        return context::ssl_status::SSLSTATUS_FAIL;
    }
}

context::response_ptr context::on_read(ss::temporary_buffer<char> tb) {
    std::array<char, 4096> buf{};

    auto resp = std::make_unique<response>();

    int n = 0;
    int ssl_err = 0;
    while (tb.size() > 0) {
        n = BIO_write(_rbio, tb.get(), tb.size());
        if (n <= 0) {
            throw ossl_error("Failed to write to OpenSSL read bio");
        }

        lg.debug("Wrote {} bytes to SSL read bio", n);
        tb.trim_front(n);

        if (!SSL_is_init_finished(_ssl.get())) {
            lg.debug("SSL initialization not yet finished, calling SSL_accept");
            n = SSL_accept(_ssl.get());
            ssl_err = SSL_get_error(_ssl.get(), n);
            switch (ssl_err) {
            case SSL_ERROR_NONE:
                break;
            case SSL_ERROR_WANT_READ:
                lg.debug("Requesting data to be read from SSL wbio");
                do {
                    n = BIO_read(_wbio, buf.data(), buf.size());
                    if (n > 0) {
                        lg.debug("Consumed {} bytes from SSL write bio", n);
                        resp->buf().append(buf.data(), n);
                    } else if (!BIO_should_retry(_wbio)) {
                        throw ossl_error(
                          "Failure consuming from SSL write bio");
                    }
                } while (n > 0);
                break;
            case SSL_ERROR_WANT_WRITE:
                lg.debug("Need data to be written to SSL _rbio");
                return std::move(resp);
            default:
                throw ossl_error("Failed to perform SSL_accept");
            }

            if (!SSL_is_init_finished(_ssl.get())) {
                lg.debug("Not yet done");
                return std::move(resp);
            } else {
                lg.debug("SSL has finished initialization");
            }
        } else {
            lg.debug("SSL is initialized");
        }

        do {
            n = SSL_read(_ssl.get(), buf.data(), buf.size());
            lg.debug("SSL_read: {}", n);
            if (n > 0) {
                lg.debug("Read {} bytes from SSL", n);
                // Echo back exactly what we just read
                SSL_write(_ssl.get(), buf.data(), n);
            }
        } while (n > 0);

        ssl_err = SSL_get_error(_ssl.get(), n);

        switch (ssl_err) {
        case SSL_ERROR_NONE:
            break;
        case SSL_ERROR_WANT_WRITE:
            lg.debug("Requesting more data to wbio");
            return std::move(resp);
        case SSL_ERROR_WANT_READ:
            lg.debug("Requesting more read data");
            do {
                n = BIO_read(_wbio, buf.data(), buf.size());
                if (n > 0) {
                    lg.debug("Read additional {} bytes", n);
                    resp->buf().append(buf.data(), n);
                } else if (!BIO_should_retry(_wbio)) {
                    throw ossl_error(
                      "Error reading from wbio on additional read");
                }
            } while (n > 0);
            break;
        default:
            throw ossl_error("Error while performing SSL_read");
        }
    }

    return std::move(resp);
}
