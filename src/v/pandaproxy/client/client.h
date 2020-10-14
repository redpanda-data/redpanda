#pragma once

#include "kafka/client.h"
#include "pandaproxy/client/broker.h"
#include "pandaproxy/client/brokers.h"
#include "pandaproxy/client/configuration.h"
#include "utils/retry.h"
#include "utils/unresolved_address.h"

#include <seastar/core/condition-variable.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

namespace pandaproxy::client {

class client {
public:
    explicit client(std::vector<unresolved_address> broker_addrs);

    /// \brief Connect to all brokers.
    ss::future<> connect();
    /// \brief Disconnect from all brokers.
    ss::future<> stop();

    /// \brief Dispatch a request to any broker.
    template<typename T>
    CONCEPT(requires(KafkaRequest<typename T::api_type>))
    ss::future<typename T::api_type::response_type> dispatch(T r) {
        return _brokers.any().then(
          [r{std::move(r)}](shared_broker_t broker) mutable {
              return broker->dispatch(std::move(r));
          });
    }

private:
    /// \brief Connect and update metdata.
    ss::future<> do_connect(unresolved_address addr);

    /// \brief Seeds are used when no brokers are connected.
    std::vector<unresolved_address> _seeds;
    /// \brief Broker lookup from topic_partition.
    brokers _brokers;
};

} // namespace pandaproxy::client
