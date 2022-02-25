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

#include "kafka/server/group_stm.h"
#include "kafka/types.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <absl/container/node_hash_map.h>

namespace kafka {

/**
 * the key type for group membership log records.
 *
 * the opaque key field is decoded based on the actual type.
 *
 * TODO: The `noop` type indicates a control structure used to synchronize raft
 * state in a transition to leader state so that a consistent read is made. this
 * is a temporary work-around until we fully address consistency semantics in
 * raft.
 */
struct group_recovery_consumer_state {
    absl::node_hash_map<kafka::group_id, group_stm> groups;
};

class group_recovery_consumer {
public:
    /*
     * This batch consumer is used during partition recovery to read, index, and
     * deduplicate both group and commit metadata snapshots.
     */

    explicit group_recovery_consumer(ss::abort_source& as)
      : _as(as) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch batch);

    group_recovery_consumer_state end_of_stream() { return std::move(_state); }

private:
    ss::future<> handle_record(model::record);
    ss::future<> handle_group_metadata(iobuf, std::optional<iobuf>);
    ss::future<> handle_offset_metadata(iobuf, std::optional<iobuf>);
    group_recovery_consumer_state _state;
    model::offset _batch_base_offset;

    ss::abort_source& _as;
};
} // namespace kafka
