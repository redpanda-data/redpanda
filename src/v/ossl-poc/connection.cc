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

#include "connection.h"

#include "logger.h"

connection::connection(
  boost::intrusive::list<connection>& hook,
  ss::connected_socket fd,
  ss::socket_address addr,
  SSL_CTX_ptr& ssl_ctx)
  : _hook(hook)
  , _fd(std::move(fd))
  , _addr(addr)
  , _ssl_ctx(ssl_ctx)
  , _in(_fd.input())
  , _out(_fd.output()) {
    _hook.push_back(*this);
}

connection::~connection() noexcept { _hook.erase(_hook.iterator_to(*this)); }

void connection::shutdown_input() {
    try {
        _fd.shutdown_input();
    } catch (...) {
        lg.error("Failed to shutdown connection: {}", std::current_exception());
    }
}

ss::future<> connection::shutdown() { return _out.close(); }
