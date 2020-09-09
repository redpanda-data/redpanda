#include "pandaproxy/client/client.h"

#include "seastarx.h"

namespace pandaproxy::client {

client::client(std::vector<ss::socket_address> broker_addrs)
  : _client(rpc::base_transport::configuration{
    .server_addr = broker_addrs[0], // TODO: Support multiple brokers
  }) {}

ss::future<> client::connect() { return _client.connect(); }

ss::future<> client::stop() { return _client.stop(); }

} // namespace pandaproxy::client
