#pragma once

#include "kafka/client.h"

namespace pandaproxy::client {

class client {
public:
    explicit client(std::vector<ss::socket_address> broker_addrs);

    /// \brief Connect to all brokers.
    ss::future<> connect();
    /// \brief Disconnect from all brokers.
    ss::future<> stop();

    /// \brief Dispatch a request to any broker.
    template<typename T>
    CONCEPT(requires(KafkaRequest<typename T::api_type>))
    ss::future<typename T::api_type::response_type> dispatch(T r) {
        return _client.dispatch(std::move(r));
    }

private:
    kafka::client _client;
};

} // namespace pandaproxy::client
