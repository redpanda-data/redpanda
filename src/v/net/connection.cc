// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "net/connection.h"

#include "rpc/logger.h"

namespace net {

connection::connection(
  boost::intrusive::list<connection>& hook,
  ss::sstring name,
  ss::connected_socket f,
  ss::socket_address a,
  server_probe& p,
  std::optional<security::tls::principal_mapper> tls_pm)
  : addr(a)
  , _hook(hook)
  , _name(std::move(name))
  , _fd(std::move(f))
  , _in(_fd.input())
  , _out(_fd.output())
  , _probe(p)
  , _tls_pm(std::move(tls_pm)) {
    _hook.push_back(*this);
    _probe.connection_established();
}

connection::~connection() noexcept { _hook.erase(_hook.iterator_to(*this)); }

void connection::shutdown_input() {
    try {
        _fd.shutdown_input();
    } catch (...) {
        _probe.connection_close_error();
        rpc::rpclog.debug(
          "Failed to shutdown connection: {}", std::current_exception());
    }
}

ss::future<> connection::shutdown() {
    _probe.connection_closed();
    return _out.stop();
}

ss::future<> connection::write(ss::scattered_message<char> msg) {
    _probe.add_bytes_sent(msg.size());
    return _out.write(std::move(msg));
}

} // namespace net
