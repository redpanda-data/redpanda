#pragma once

#include "pandaproxy/client/client.h"
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
      std::vector<ss::socket_address> broker_addrs);

    ss::future<> start();
    ss::future<> stop();

private:
    client::client _client;
    server::context_t _ctx;
    server _server;
};

} // namespace pandaproxy
