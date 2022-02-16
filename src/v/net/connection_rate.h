/*
 * Copyright 2022 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "seastarx.h"

#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/net.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>

#include <chrono>

using namespace std::chrono_literals;

namespace net {

struct connection_rate_info {
    int64_t max_connection_rate;
    std::vector<ss::sstring> overrides;
};

} // namespace net