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
#include "ossl-poc/ssl_utils.h"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/net/api.hh>
#include <seastar/net/socket_defs.hh>

#include <boost/intrusive/list_hook.hpp>
class connection
  : public ss::enable_lw_shared_from_this<connection>
  , public boost::intrusive::list_base_hook<> {
public:
    connection(
      boost::intrusive::list<connection>& hook,
      ss::connected_socket fd,
      ss::socket_address addr,
      SSL_CTX_ptr& ssl_ctx);

    ~connection() noexcept;
    connection(const connection&) = delete;
    connection& operator=(const connection&) = delete;
    connection(connection&&) noexcept = default;
    connection& operator=(connection&&) noexcept = delete;

    ss::input_stream<char>& input() { return _in; }
    ss::output_stream<char>& output() { return _out; }

    ss::future<> shutdown();
    void shutdown_input();

    ss::future<> wait_for_input_shutdown() {
        return _fd ? _fd.wait_input_shutdown() : ss::make_ready_future();
    }

    const ss::socket_address& local_address() const noexcept { return _addr; }

    SSL_CTX_ptr& ssl_ctx() noexcept { return _ssl_ctx; }

private:
    boost::intrusive::list<connection>& _hook;
    ss::connected_socket _fd;
    ss::socket_address _addr;
    SSL_CTX_ptr& _ssl_ctx;
    ss::input_stream<char> _in;
    ss::output_stream<char> _out;
};
