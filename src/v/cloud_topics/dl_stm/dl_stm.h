// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cloud_topics/dl_stm/dl_stm_state.h"
#include "raft/persisted_stm.h"

namespace experimental::cloud_topics {

// Avoid leaking the api class details to the header.
class dl_stm_api;

class dl_stm final : public raft::persisted_stm<> {
    // Allow the api class to access the private state of the stm.
    friend class dl_stm_api;

public:
    static constexpr const char* name = "dl_stm";

    dl_stm(ss::logger&, raft::consensus*);

private:
    ss::future<> do_apply(const model::record_batch& batch) override;

    ss::future<>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&&) override;
    ss::future<raft::stm_snapshot>
    take_local_snapshot(ssx::semaphore_units u) override;

    ss::future<> apply_raft_snapshot(const iobuf&) override;
    ss::future<iobuf> take_snapshot(model::offset) override;

private:
    dl_stm_state _state;
};

} // namespace experimental::cloud_topics
