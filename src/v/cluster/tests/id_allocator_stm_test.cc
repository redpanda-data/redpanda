// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "base/outcome.h"
#include "cluster/id_allocator_stm.h"
#include "cluster/tests/raft_fixture_retry_policy.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "raft/fundamental.h"
#include "raft/tests/raft_fixture.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "storage/record_batch_builder.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"
#include "test_utils/scoped_config.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>

#include <thread>

using namespace std::chrono_literals;
using namespace raft;

namespace cluster {
namespace {

ss::logger idstmlog{"idstm-test"};
using stm_ssshptr_t = const ss::shared_ptr<cluster::id_allocator_stm>&;

struct id_allocator_stm_fixture : stm_raft_fixture<cluster::id_allocator_stm> {
    //
    id_allocator_stm_fixture() {
        test_local_cfg.get("id_allocator_batch_size").set_value(int16_t(1));
        test_local_cfg.get("id_allocator_log_capacity").set_value(int16_t(2));
    }

    stm_shptrs_t create_stms(
      state_machine_manager_builder& builder, raft_node_instance& node) {
        return builder.create_stm<cluster::id_allocator_stm>(
          idstmlog, node.raft().get(), config::shard_local_cfg());
    }

    ss::future<> reset(int64_t id) {
        auto result = co_await stm_retry_with_leader<0>(
          30s, [id](stm_ssshptr_t stm) { return stm->reset_next_id(id, 1s); });
        ASSERT_RESULT_EQ_CORO(result, id);
    }

    ss::future<> allocate1(int64_t& cur_last_id) {
        auto result = co_await stm_retry_with_leader<0>(
          30s, [](stm_ssshptr_t stm) { return stm->allocate_id(1s); });
        ASSERT_RESULT_GT_CORO(result, cur_last_id);
        cur_last_id = result.assume_value();
    }

    // Allocates n IDs, ensuring that each one new ID is greater than the
    // previous one, starting with 'cur_last_id'.
    // Returns the last allocated ID.
    // One by one to be able to inject raft quirks in future.
    ss::future<int64_t> allocate_n(int64_t cur_last_id, int n) {
        for (int i = 0; i < n; i++) {
            co_await allocate1(cur_last_id);
        }
        co_return cur_last_id;
    }

    scoped_config test_local_cfg;
};

} // namespace

TEST_F_CORO(id_allocator_stm_fixture, stm_monotonicity_test) {
    co_await initialize_state_machines();

    int64_t last_id = -1;
    co_await allocate_n(last_id, 5);
}

TEST_F_CORO(id_allocator_stm_fixture, stm_restart_test) {
    co_await initialize_state_machines();

    int64_t last_id = -1;

    last_id = co_await allocate_n(last_id, 5);
    co_await restart_nodes();
    co_await allocate_n(last_id, 5);
}

TEST_F_CORO(id_allocator_stm_fixture, stm_reset_id_test) {
    co_await initialize_state_machines();

    int64_t last_id = -1;
    co_await allocate_n(last_id, 5);

    // Reset to 100.
    last_id = 100;
    co_await reset(last_id + 1);
    last_id = co_await allocate_n(last_id, 5);
    ASSERT_EQ_CORO(last_id, 105);

    // Even after restarting, the starting point should be where we left off.
    co_await restart_nodes();

    last_id = co_await allocate_n(last_id, 5);
    ASSERT_EQ_CORO(last_id, 110);
}

TEST_F_CORO(id_allocator_stm_fixture, stm_reset_batch_test) {
    co_await initialize_state_machines();
    int64_t last_id = -1;
    co_await allocate_n(last_id, 5);

    last_id = 100;
    co_await reset(last_id + 1);

    // After a leadership change, the reset should still take effect. However,
    // it should be offset by one batch.
    co_await with_leader(1s, [](raft_node_instance& leader_node) {
        return leader_node.raft()->step_down("test");
    });

    last_id = co_await allocate_n(last_id, 1);
    ASSERT_EQ_CORO(last_id, 102);
}

} // namespace cluster
