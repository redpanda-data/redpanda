/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <cstdint>
namespace constants {

class balancer {
public:
    // The partition balancer will declare a node unresponsive if it misses this
    // many node statuses in a row
    static constexpr uint8_t missed_statuses_until_unresponsive = 7u;
};

} // namespace constants
