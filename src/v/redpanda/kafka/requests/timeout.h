#pragma once
#include "model/timeout_clock.h"
namespace kafka::requests {

// converts Kafka timeout expressed in milliseconds to
// model::timeout_clock::time_point used in redpanda internal apis.
// In kafka when timeout has negative value it means that request has no timeout

inline model::timeout_clock::time_point to_timeout(int64_t timeout_ms) {
    if (timeout_ms <= 0) {
        return model::no_timeout;
    }
    return model::timeout_clock::now() + std::chrono::milliseconds(timeout_ms);
}
}; // namespace kafka::requests
