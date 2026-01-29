/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "raft/tests/raft_fixture.h"

#include "test_utils/async.h"

namespace raft {

seastar::future<> raft_fixture::TearDownAsync() { return stop(); }
seastar::future<> raft_fixture::SetUpAsync() { return start(); }

ss::future<> raft_fixture::wait_for_committed_offset(
  model::offset offset, std::chrono::milliseconds timeout) {
    return tests::cooperative_spin_wait_with_timeout(timeout, [this, offset] {
        return std::all_of(
          nodes().begin(), nodes().end(), [offset](auto& pair) {
              return pair.second->raft()->committed_offset() >= offset;
          });
    });
}
ss::future<> raft_fixture::wait_for_visible_offset(
  model::offset offset, std::chrono::milliseconds timeout) {
    return tests::cooperative_spin_wait_with_timeout(timeout, [this, offset] {
        return std::all_of(
          nodes().begin(), nodes().end(), [offset](auto& pair) {
              return pair.second->raft()->last_visible_index() >= offset;
          });
    });
}
} // namespace raft
