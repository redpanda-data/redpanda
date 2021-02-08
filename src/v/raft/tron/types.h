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

#include <seastar/core/sstring.hh>

#include <seastarx.h>

namespace raft::tron {
struct stats_request {};
struct stats_reply {};
struct put_reply {
    bool success;
    ss::sstring failure_reason;
};
} // namespace raft::tron
