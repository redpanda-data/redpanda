// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/outcome.h"
#include "jumbo_log/metadata.h"
#include "jumbo_log_stm.h"
#include "model/fundamental.h"
#include "raft/tests/raft_fixture.h"
#include "random/generators.h"
#include "state_machine_manager.h"
#include "test_utils/test.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/log.hh>

#include <tuple>

static ss::logger test_logger{"jumbo_log_stm-test"};

using namespace raft;

using stm_t = cluster::jumbo_log_stm;
using stm_cssshptrr_t = const ss::shared_ptr<stm_t>&;

struct jumbo_log_stm_fixture : stm_raft_fixture<stm_t> {
    static constexpr auto TIMEOUT = 30s;
    //
    stm_shptrs_t create_stms(
      state_machine_manager_builder& builder, raft_node_instance& node) {
        return builder.create_stm<stm_t>(test_logger, node.raft().get());
    }

    ss::future<result<jumbo_log::write_intent_id_t>>
    create_write_intent(jumbo_log::segment_object object) {
        auto res = co_await stm_retry_with_leader<0>(
          TIMEOUT, [object = std::move(object)](stm_cssshptrr_t stm) {
              return stm->create_write_intent(object, TIMEOUT);
          });
        co_return res;
    }
};

TEST_F_CORO(jumbo_log_stm_fixture, test_stm_basic) {
    co_await initialize_state_machines();

    auto create_res1 = co_await create_write_intent(
      {.id = random_generators::get_uuid(), .size_bytes = 100});

    auto object_uuid_2 = random_generators::get_uuid();
    auto create_res2 = co_await create_write_intent(
      {.id = object_uuid_2, .size_bytes = 100});

    auto create_res3 = co_await create_write_intent(
      {.id = object_uuid_2, .size_bytes = 100});

    ASSERT_NE_CORO(create_res1, create_res2);
    ASSERT_EQ_CORO(create_res2, create_res3);
}

TEST_F_CORO(jumbo_log_stm_fixture, test_stm_snapshots) {
    co_await initialize_state_machines();

    auto create_res1 = co_await create_write_intent(
      {.id = random_generators::get_uuid(), .size_bytes = 100});

    ASSERT_EQ_CORO(create_res1.value(), jumbo_log::write_intent_id_t(1));

    auto object_uuid_2 = random_generators::get_uuid();
    auto create_res2 = co_await create_write_intent(
      {.id = object_uuid_2, .size_bytes = 100});

    ASSERT_EQ_CORO(create_res2.value(), jumbo_log::write_intent_id_t(2));

    auto create_res3 = co_await create_write_intent(
      {.id = object_uuid_2, .size_bytes = 100});

    ASSERT_NE_CORO(create_res1, create_res2);
    ASSERT_EQ_CORO(create_res2, create_res3);

    auto offset_result = co_await stm_retry_with_leader<0>(
      TIMEOUT, [](stm_cssshptrr_t stm) {
          return ss::make_ready_future<result<model::offset>>(
            stm->last_applied_offset());
      });
    auto offset = offset_result.value();

    std::ignore = offset;

    // See take snapshot example in src/v/raft/tests/persisted_stm_test.cc for
    // how to implement snapshotting.
    // co_await retry_with_leader(
    //   model::timeout_clock::now() + TIMEOUT,
    //   [this, offset](raft_node_instance& node) -> ss::future<bool> {
    //       auto snap = co_await get_stm<0>(node)->take_snapshot(offset);

    //       co_return co_await node.raft()
    //         ->write_snapshot(raft::write_snapshot_cfg(offset,
    //         std::move(snap))) .then([]() { return
    //         ss::make_ready_future<bool>(true); });
    //   });

    // // restart raft after trunaction, ensure snapshot is loaded
    // // correctly.
    // co_await restart_nodes();

    // auto start_offset_result = co_await retry_with_leader(
    //   model::timeout_clock::now() + TIMEOUT, [this](raft_node_instance& node)
    //   {
    //       return ss::make_ready_future<result<model::offset>>(
    //         node.raft()->start_offset());
    //   });
    // ASSERT_RESULT_EQ_CORO(start_offset_result, model::next_offset(offset));

    // auto create_res4 = co_await create_write_intent(
    //   {.id = object_uuid_2, .size_bytes = 100});

    // // Still same result on retry. Snapshot should have retained this
    // // information and still provide idempotency.
    // ASSERT_EQ_CORO(create_res3, create_res4);

    // auto create_res5 = co_await create_write_intent(
    //   {.id = random_generators::get_uuid(), .size_bytes = 100});

    // // An id not yet seen.
    // ASSERT_EQ_CORO(create_res5.value(), jumbo_log::write_intent_id_t(3));
}
