// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "cluster/distributed_kv_stm.h"
#include "cluster/tests/raft_fixture_retry_policy.h"
#include "errc.h"
#include "raft/tests/raft_fixture.h"
#include "raft/tests/raft_group_fixture.h"
#include "serde/envelope.h"
#include "test_utils/test.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include <memory>
#include <tuple>

static ss::logger test_logger{"kv_stm-test"};

using namespace raft;

using test_key = int;
using test_value = int;
using stm_t = cluster::distributed_kv_stm<test_key, test_value>;
using stm_cssshptrr_t = const ss::shared_ptr<stm_t>&;

struct kv_stm_fixture : stm_raft_fixture<stm_t> {
    static constexpr auto TIMEOUT = 30s;
    //
    stm_shptrs_t create_stms(
      state_machine_manager_builder& builder, raft_node_instance& node) {
        return builder.create_stm<stm_t>(1, test_logger, node.raft().get());
    }

    ss::future<result<model::partition_id, cluster::errc>>
    get_coordinator(test_key k) {
        return stm_retry_with_leader<0>(
          TIMEOUT, [k = std::move(k)](stm_cssshptrr_t stm) {
              return stm->coordinator(k);
          });
    }

    ss::future<> put(stm_t::kv_data_t kvs) {
        auto res = co_await stm_retry_with_leader<0>(
          TIMEOUT, [kvs = std::move(kvs)](stm_cssshptrr_t stm) {
              return stm->put(kvs);
          });
        ASSERT_EQ_CORO(res, cluster::errc::success);
    }

    ss::future<result<std::optional<test_value>, cluster::errc>>
    get(test_key k) {
        return stm_retry_with_leader<0>(
          TIMEOUT,
          [k = std::move(k)](stm_cssshptrr_t stm) { return stm->get(k); });
    }

    ss::future<result<stm_t::kv_data_t, cluster::errc>> list() {
        return stm_retry_with_leader<0>(
          TIMEOUT, [](stm_cssshptrr_t stm) { return stm->list(); });
    }

    ss::future<result<size_t, cluster::errc>>
    repartition(size_t new_partition_count) {
        return stm_retry_with_leader<0>(
          TIMEOUT, [new_partition_count](stm_cssshptrr_t stm) {
              return stm->repartition(new_partition_count);
          });
    }
};

TEST_F_CORO(kv_stm_fixture, test_stm_basic) {
    co_await initialize_state_machines();

    ASSERT_RESULT_EQ_CORO(co_await get_coordinator(0), 0);
    ASSERT_RESULT_EQ_CORO(co_await get_coordinator(0), 0);

    stm_t::kv_data_t kvs;
    kvs[0] = 99;
    kvs[1] = 100;
    co_await put(kvs);

    ASSERT_RESULT_EQ_CORO(co_await get(0), test_value{99});
    ASSERT_RESULT_EQ_CORO(co_await get(1), test_value{100});

    auto list_result = co_await list();
    ASSERT_TRUE_CORO(list_result);
    ASSERT_EQ_CORO(list_result.assume_value().size(), 2);

    // delete key
    auto del_result = co_await stm_retry_with_leader<0>(
      TIMEOUT, [](stm_cssshptrr_t stm) { return stm->remove(0); });
    ASSERT_EQ_CORO(del_result, cluster::errc::success);
    // mapping should be gone.
    ASSERT_RESULT_EQ_CORO(co_await get(0), std::nullopt);
    // other mapping should be retained.
    ASSERT_RESULT_EQ_CORO(co_await get(1), test_value{100});
    kvs.erase(0);
    ASSERT_RESULT_EQ_CORO(co_await list(), kvs)
}

TEST_F_CORO(kv_stm_fixture, test_stm_list) {
    co_await initialize_state_machines();

    ASSERT_RESULT_EQ_CORO(co_await get_coordinator(0), 0);

    stm_t::kv_data_t kvs;
    for (int i = 0; i < 100; ++i) {
        kvs[i] = i * i;
    }
    // List and put can be called concurrently. We don't make any guarentees
    // (it's eventually consistent), but it shouldn't crash.
    for (int i = 0; i < 1000; ++i) {
        kvs[i] = i;
        co_await put(kvs);
        ASSERT_TRUE_CORO(co_await list());
    }
}

TEST_F_CORO(kv_stm_fixture, test_batched_actions) {
    constexpr static auto N_ENTRIES = 30;
    co_await initialize_state_machines();

    for (int i = 0; i < N_ENTRIES; i++) {
        ASSERT_RESULT_EQ_CORO(co_await get_coordinator(i), 0);
    }

    absl::btree_map<test_key, test_value> kvs;
    for (int i = 0; i < N_ENTRIES; i++) {
        kvs[i] = i;
    }
    co_await put(std::move(kvs));

    for (int i = 0; i < N_ENTRIES; i++) {
        ASSERT_RESULT_EQ_CORO(co_await get(i), test_value{i});
    }

    // Delete the even keys
    auto predicate = [](test_key key) { return key % 2 == 0; };
    auto res = co_await stm_retry_with_leader<0>(
      TIMEOUT,
      [&predicate](stm_cssshptrr_t stm) { return stm->remove_all(predicate); });
    ASSERT_EQ_CORO(res, cluster::errc::success);
    for (int i = 0; i < N_ENTRIES; i++) {
        if (predicate(i)) {
            ASSERT_RESULT_EQ_CORO(co_await get(i), std::nullopt);
        } else {
            ASSERT_RESULT_EQ_CORO(co_await get(i), test_value{i});
        }
    }
}

TEST_F_CORO(kv_stm_fixture, test_stm_repartitioning) {
    co_await initialize_state_machines();
    ASSERT_RESULT_EQ_CORO(co_await get_coordinator(0), 0);
    // load up some keys, should all hash to 0 partition.
    for (int i = 0; i < 99; i++) {
        ASSERT_RESULT_EQ_CORO(co_await get_coordinator(i), 0);
    }
    // repartition to bump the partition count.
    ASSERT_RESULT_EQ_CORO(co_await repartition(3), 3);

    // load up more keys that hash to all partitions
    bool part1_has_entries = false, part2_has_entries = false;
    for (int i = 100; !part1_has_entries || !part2_has_entries; i++) {
        auto res = co_await get_coordinator(i);
        ASSERT_RESULT_GE_CORO(res, 0);
        ASSERT_RESULT_LE_CORO(res, 2);
        part1_has_entries = part1_has_entries || res.assume_value() == 1;
        part2_has_entries = part2_has_entries || res.assume_value() == 2;
    }
    // ensure the original set of keys are still with partition 0;
    for (int i = 0; i < 99; i++) {
        ASSERT_RESULT_EQ_CORO(co_await get_coordinator(i), 0);
    }
    // downsizing prohibited
    auto repartition_result = co_await repartition(1);
    ASSERT_FALSE_CORO(repartition_result);
    ASSERT_EQ_CORO(repartition_result.error(), cluster::errc::invalid_request);
}

TEST_F_CORO(kv_stm_fixture, test_stm_snapshots) {
    co_await initialize_state_machines();

    // load some data into the stm
    for (int i = 0; i < 99; i++) {
        ASSERT_TRUE_CORO(co_await get_coordinator(i));
    }

    for (int i = 0; i < 99; i++) {
        for (int j = 0; j < 10; j++) {
            absl::btree_map<test_key, test_value> kvs;
            kvs[i] = test_value{j};
            co_await put(kvs);
        }
    }

    auto offset_result = co_await stm_retry_with_leader<0>(
      TIMEOUT, [](stm_cssshptrr_t stm) {
          return ss::make_ready_future<result<model::offset>>(
            stm->last_applied_offset());
      });
    auto offset = offset_result.value();

    co_await stm_retry_with_leader<0>(TIMEOUT, [](stm_cssshptrr_t stm) {
        return stm->write_local_snapshot().then(
          []() { return ss::make_ready_future<bool>(true); });
    });

    co_await retry_with_leader(
      model::timeout_clock::now() + TIMEOUT,
      [offset](raft_node_instance& node) {
          return node.raft()
            ->write_snapshot(raft::write_snapshot_cfg(offset, iobuf()))
            .then([]() { return ss::make_ready_future<bool>(true); });
      });

    // restart raft after trunaction, ensure snapshot is loaded
    // correctly.
    co_await restart_nodes();

    auto start_offset_result = co_await retry_with_leader(
      model::timeout_clock::now() + TIMEOUT, [](raft_node_instance& node) {
          return ss::make_ready_future<result<model::offset>>(
            node.raft()->start_offset());
      });
    ASSERT_RESULT_EQ_CORO(start_offset_result, model::next_offset(offset));

    for (int i = 0; i < 99; i++) {
        ASSERT_RESULT_EQ_CORO(co_await get(i), test_value{9});
    }
}
