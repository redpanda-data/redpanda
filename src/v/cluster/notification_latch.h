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

#include "cluster/errc.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "utils/expiring_promise.h"

#include <absl/container/flat_hash_map.h>

namespace cluster {
/// Cache notifications
class notification_latch {
private:
    using promise_t = expiring_promise<errc, model::timeout_clock>;
    using promise_ptr = std::unique_ptr<promise_t>;
    using underlying_t = absl::flat_hash_map<model::offset, promise_ptr>;

public:
    ss::future<errc> wait_for(model::offset, model::timeout_clock::time_point);
    void notify(model::offset);
    void stop();

private:
    underlying_t _promises;
};
} // namespace cluster
