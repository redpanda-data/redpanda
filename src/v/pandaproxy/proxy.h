/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "kafka/client/client.h"
#include "pandaproxy/server.h"
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/net/socket_defs.hh>

#include <vector>

namespace pandaproxy {

class proxy {
public:
    proxy(
      ss::socket_address listen_addr,
      std::vector<unresolved_address> broker_addrs);

    ss::future<> start();
    ss::future<> stop();

private:
    kafka::client::client _client;
    server::context_t _ctx;
    server _server;
};

} // namespace pandaproxy
