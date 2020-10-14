#include "pandaproxy/client/client.h"

#include "pandaproxy/client/broker.h"
#include "pandaproxy/client/configuration.h"
#include "pandaproxy/client/error.h"
#include "pandaproxy/client/logger.h"
#include "pandaproxy/client/retry_with_mitigation.h"
#include "seastarx.h"
#include "ssx/future-util.h"
#include "utils/unresolved_address.h"

#include <seastar/core/gate.hh>

namespace pandaproxy::client {

client::client(std::vector<unresolved_address> broker_addrs)
  : _seeds{std::move(broker_addrs)}
  , _brokers{} {}

ss::future<> client::do_connect(unresolved_address addr) {
    return make_broker(unknown_node_id, addr)
      .then([this](shared_broker_t broker) {
          return broker
            ->dispatch(kafka::metadata_request{.list_all_topics = true})
            .then([this, broker](kafka::metadata_response res) {
                return _brokers.apply(std::move(res));
            });
      });
}

ss::future<> client::connect() {
    return ss::do_with(size_t{0}, [this](size_t& retries) {
        return retry_with_mitigation(
          shard_local_cfg().retries(),
          shard_local_cfg().retry_base_backoff(),
          [this, retries]() {
              return do_connect(_seeds[retries % _seeds.size()]);
          },
          [&retries](std::exception_ptr) {
              ++retries;
              return ss::now();
          });
    });
}

ss::future<> client::stop() { return _brokers.stop(); }

} // namespace pandaproxy::client
