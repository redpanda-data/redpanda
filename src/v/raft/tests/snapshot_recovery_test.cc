// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "raft/tests/raft_fixture.h"
#include "raft/tests/raft_fixture_retry_policy.h"
#include "test_utils/test.h"

using namespace raft;

TEST_F_CORO(raft_fixture, test_snapshot_recovery) {
    auto& n0 = add_node(model::node_id(0), model::revision_id(0));
    auto& n1 = add_node(model::node_id(2), model::revision_id(0));
    auto& n2 = add_node(model::node_id(3), model::revision_id(0));

    // seed one of the nodes with snapshot
    auto base_dir = n1.base_directory();
    auto ntp = n1.ntp();

    snapshot_metadata md{
      .last_included_index = model::offset(128),
      .last_included_term = model::term_id(64),
      .latest_configuration = raft::group_configuration(
        all_vnodes(), model::revision_id(0)),
      .cluster_time = clock_type::time_point::min(),
      .log_start_delta = offset_translator_delta(10),
    };
    co_await ss::recursive_touch_directory(n1.work_directory());
    storage::simple_snapshot_manager snapshot_manager(
      std::filesystem::path(n1.work_directory()),
      storage::simple_snapshot_manager::default_snapshot_filename,
      ss::default_priority_class());

    co_await raft::details::persist_snapshot(snapshot_manager, md, iobuf{});

    co_await n0.init_and_start(all_vnodes());
    co_await n1.init_and_start(all_vnodes());
    co_await n2.init_and_start(all_vnodes());

    auto leader_id = co_await wait_for_leader(30s);
    auto& leader_node = node(leader_id);
    ASSERT_GT_CORO(leader_node.raft()->term(), md.last_included_term);
    ASSERT_EQ_CORO(
      leader_node.raft()->start_offset(),
      model::next_offset(md.last_included_index));
}
