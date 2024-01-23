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
#include "ssx/abort_source.h"

#include <seastar/core/shared_ptr.hh>

class context final
  : public ss::enable_lw_shared_from_this<context>
  , public boost::intrusive::list_base_hook<> {
public:
    context(
      std::optional<std::reference_wrapper<boost::intrusive::list<context>>>,
      class ossl_tls_service& ossl_tls_service,
      ss::lw_shared_ptr<connection>);
    ~context() noexcept = default;

    ss::future<> start();
    ss::future<> process();
    ss::future<> process_one_request();
    ss::future<> stop();
    ssx::sharded_abort_source& abort_source() { return _as; }
    bool abort_requested() const { return _as.abort_requested(); }

private:
    bool is_finished_parsing() const;

private:
    std::optional<std::reference_wrapper<boost::intrusive::list<context>>>
      _hook;
    class ossl_tls_service& _ossl_tls_service;
    ss::lw_shared_ptr<connection> _conn;
    ssx::sharded_abort_source _as;
    SSL_ptr _ssl;
    ss::promise<> _wait_input_shutdown;
};
