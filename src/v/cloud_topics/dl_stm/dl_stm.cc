// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_topics/dl_stm/dl_stm.h"

namespace experimental::cloud_topics {

dl_stm::dl_stm(ss::logger& logger, raft::consensus* raft)
  : raft::persisted_stm<>(name, logger, raft) {}

ss::future<> dl_stm::do_apply(const model::record_batch&) { co_return; }

ss::future<> dl_stm::apply_local_snapshot(raft::stm_snapshot_header, iobuf&&) {
    co_return;
}

ss::future<raft::stm_snapshot>
dl_stm::take_local_snapshot(ssx::semaphore_units) {
    co_return raft::stm_snapshot{};
}

ss::future<> dl_stm::apply_raft_snapshot(const iobuf&) { co_return; }

ss::future<iobuf> dl_stm::take_snapshot(model::offset) { co_return iobuf(); }

}; // namespace experimental::cloud_topics
