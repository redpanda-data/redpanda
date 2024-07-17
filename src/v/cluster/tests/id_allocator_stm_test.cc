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
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "raft/tests/simple_raft_fixture.h"
#include "raft/tests/stm_test_fixture.h"
#include "raft/types.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "storage/record_batch_builder.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"
#include "test_utils/scoped_config.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>

#include <thread>

using namespace std::chrono_literals;

namespace cluster {
namespace {

ss::logger idstmlog{"idstm-test"};

struct id_allocator_stm_fixture : state_machine_fixture {
    ss::future<> initialize_state_machines() {
        // set configuration parameters
        test_local_cfg.get("id_allocator_batch_size").set_value(int16_t(1));
        test_local_cfg.get("id_allocator_log_capacity").set_value(int16_t(2));

        create_nodes();
        co_await start_nodes();
    }

    ss::future<> start_node(raft_node_instance& node) {
        co_await node.initialise(all_vnodes());
        raft::state_machine_manager_builder builder;
        auto stm = builder.create_stm<cluster::id_allocator_stm>(
          idstmlog, node.raft().get(), config::shard_local_cfg());
        co_await node.start(std::move(builder));
        node_stms.emplace(node.get_vnode(), std::move(stm));
    }

    ss::future<> start_nodes() {
        co_await parallel_for_each_node(
          [this](raft_node_instance& node) { return start_node(node); });
    }

    ss::future<> stop_and_recreate_nodes() {
        absl::flat_hash_map<model::node_id, ss::sstring> data_directories;
        for (auto& [id, node] : nodes()) {
            data_directories[id]
              = node->raft()->log()->config().base_directory();
            node_stms.erase(node->get_vnode());
        }

        co_await ss::parallel_for_each(
          std::views::keys(data_directories),
          [this](model::node_id id) { return stop_node(id); });

        for (auto& [id, data_dir] : data_directories) {
            add_node(id, model::revision_id(0), std::move(data_dir));
        }
    }

    ss::future<> reset(int64_t id) {
        auto result = co_await retry_with_leader(
          model::timeout_clock::now() + 30s,
          [this, id](raft_node_instance& leader_node) {
              auto stm = node_stms[leader_node.get_vnode()];
              return stm->reset_next_id(id, 1s);
          });
        ASSERT_RESULT_EQ_CORO(result, id);
    }

    ss::future<> allocate1(int64_t& cur_last_id) {
        auto result = co_await retry_with_leader(
          model::timeout_clock::now() + 30s,
          [this](raft_node_instance& leader_node) {
              auto stm = node_stms[leader_node.get_vnode()];
              return stm->allocate_id(1s);
          });
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

    absl::flat_hash_map<raft::vnode, ss::shared_ptr<cluster::id_allocator_stm>>
      node_stms;
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
    co_await stop_and_recreate_nodes();
    co_await start_nodes();
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
    co_await stop_and_recreate_nodes();
    co_await start_nodes();

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
