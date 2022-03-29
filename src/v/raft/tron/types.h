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

#include "bytes/iobuf.h"
#include "model/async_adl_serde.h"
#include "raft/types.h"

namespace raft::tron {
struct stats_request {};
struct stats_reply {};
struct put_reply {
    bool success;
    ss::sstring failure_reason;
};
} // namespace raft::tron
