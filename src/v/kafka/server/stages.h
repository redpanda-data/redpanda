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
#include "base/seastarx.h"
#include "kafka/protocol/fwd.h"

#include <seastar/core/future.hh>

namespace kafka {

/// \brief The two-stage result of a group request that replicates a batch.
///
/// \c dispatched resolves once the batch is enqueued in raft, which fixes its
/// position relative to other requests. \c result resolves once the batch is
/// committed and its state change applied, and carries the client response.
template<typename Result>
struct stages {
    using value_type = Result;

    explicit stages(Result res)
      : dispatched(ss::now())
      , result(ss::make_ready_future<Result>(std::move(res))) {}

    explicit stages(ss::future<Result> res)
      : dispatched(ss::now())
      , result(std::move(res)) {}

    stages(ss::future<> dispatched, ss::future<Result> res)
      : dispatched(std::move(dispatched))
      , result(std::move(res)) {}

    ss::future<> dispatched;
    ss::future<Result> result;
};

using offset_commit_stages = stages<offset_commit_response>;
using join_group_stages = stages<join_group_response>;
using sync_group_stages = stages<sync_group_response>;

} // namespace kafka
