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
#include "bytes/iobuf.h"
#include "connection.h"
#include "container/include/container/fragmented_vector.h"
#include "logger.h"
#include "ssx/abort_source.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>

class context final
  : public ss::enable_lw_shared_from_this<context>
  , public boost::intrusive::list_base_hook<> {
public:
    enum class ssl_status {
        SSLSTATUS_OK,
        SSLSTATUS_WANT_READ,
        SSLSTATUS_WANT_WRITE,
        SSLSTATUS_FAIL
    };
    context(
      std::optional<std::reference_wrapper<boost::intrusive::list<context>>>,
      class ossl_tls_service& ossl_tls_service,
      ss::lw_shared_ptr<connection>);
    ~context() noexcept { lg.info("Context being destructed"); }

    ss::future<> start();
    ss::future<> process();
    ss::future<> process_one_request();
    ss::future<> stop();
    ssx::sharded_abort_source& abort_source() { return _as; }
    bool abort_requested() const { return _as.abort_requested(); }

    static ssl_status get_sslstatus(SSL* ssl, int n);

private:
    class response {
    public:
        response() = default;
        ~response() = default;

        const iobuf& buf() const { return _buf; };
        iobuf& buf() { return _buf; };
        iobuf release() && { return std::move(_buf); }

    private:
        iobuf _buf;
    };
    using response_ptr = ss::foreign_ptr<std::unique_ptr<response>>;
    bool is_finished_parsing() const;
    response_ptr on_read(ss::temporary_buffer<char>);

private:
    std::optional<std::reference_wrapper<boost::intrusive::list<context>>>
      _hook;
    class ossl_tls_service& _ossl_tls_service;
    ss::lw_shared_ptr<connection> _conn;
    ssx::sharded_abort_source _as;
    BIO* _rbio;
    BIO* _wbio;
    SSL_ptr _ssl;

    ss::promise<> _wait_input_shutdown;
};
