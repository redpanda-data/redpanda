#pragma once

#include "redpanda/kafka/requests/request_context.h"

#include <cstdint>

namespace kafka::transport {

class probe {
public:
    void connection_established() {
        ++_connects;
        ++_connections;
    }

    void connection_closed() {
        --_connections;
    }

    void waiting_for_available_memory() {
        ++_requests_blocked_memory;
    }

    void serving_request(requests::request_context&) {
        ++_requests_served;
        ++_requests_serving;
    }

private:
    uint64_t _connects = 0;
    uint64_t _connections = 0;
    uint64_t _requests_blocked_memory = 0;
    uint64_t _requests_served = 0;
    uint64_t _requests_serving = 0;
};

} // namespace kafka::transport
