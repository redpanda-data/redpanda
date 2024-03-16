/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/outcome.h"
#include "model/fundamental.h"
#include "raft/errc.h"

#include <chrono>
#include <optional>

namespace raft {

enum class consistency_level { quorum_ack, leader_ack, no_ack };

struct replicate_options {
    explicit replicate_options(consistency_level l)
      : consistency(l)
      , timeout(std::nullopt) {}

    replicate_options(consistency_level l, std::chrono::milliseconds timeout)
      : consistency(l)
      , timeout(timeout) {}

    consistency_level consistency;
    std::optional<std::chrono::milliseconds> timeout;
};

struct replicate_result {
    /// used by the kafka API to produce a kafka reply to produce request.
    /// see produce_request.cc
    model::offset last_offset;
};

struct replicate_stages {
    replicate_stages(ss::future<>, ss::future<result<replicate_result>>);
    explicit replicate_stages(raft::errc);
    // after this future is ready, request in enqueued in raft and it will not
    // be reorderd
    ss::future<> request_enqueued;
    // after this future is ready, request was successfully replicated with
    // requested consistency level
    ss::future<result<replicate_result>> replicate_finished;
};

} // namespace raft
