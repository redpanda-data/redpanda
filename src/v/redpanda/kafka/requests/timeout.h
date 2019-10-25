#pragma once
#include "model/timeout_clock.h"
namespace kafka {

// converts Kafka timeout expressed in milliseconds to
// model::timeout_clock::time_point used in redpanda internal apis.
// In kafka when timeout has negative value it means that request has no timeout

inline model::timeout_clock::time_point
to_timeout(std::chrono::milliseconds timeout) {
    if (timeout.count() <= 0) {
        return model::no_timeout;
    }
    return model::timeout_clock::now() + timeout;
}
}; // namespace kafka
