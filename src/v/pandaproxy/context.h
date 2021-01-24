/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "kafka/client/client.h"
#include "seastarx.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/semaphore.hh>

namespace pandaproxy {

struct context_t {
    ss::semaphore mem_sem;
    ss::abort_source as;
    kafka::client::client& client;
};

} // namespace pandaproxy
