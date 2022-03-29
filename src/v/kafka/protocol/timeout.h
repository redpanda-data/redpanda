/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

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
