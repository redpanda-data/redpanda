// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/distributed_kv_stm.h"
#include "raft/tests/raft_group_fixture.h"
#include "raft/tests/simple_raft_fixture.h"
#include "serde/envelope.h"

static ss::logger logger{"kv_stm-test"};

using test_key = int;
using test_value = int;
using stm_t = cluster::distributed_kv_stm<test_key, test_value>;

struct stm_test_fixture : simple_raft_fixture {
    void create_stm_and_start_raft(
      int num_partitions,
      storage::ntp_config::default_overrides overrides = {}) {
        create_raft(overrides);
        raft::state_machine_manager_builder stm_m_builder;

        _stm = stm_m_builder.create_stm<stm_t>(
          num_partitions, logger, _raft.get());

        _raft->start(std::move(stm_m_builder)).get();
        _started = true;
    }

    ss::shared_ptr<stm_t> _stm;
};

FIXTURE_TEST(test_stm_basic, stm_test_fixture) {
    create_stm_and_start_raft(1);
    auto& stm = *_stm;
    stm.start().get0();
    wait_for_confirmed_leader();

    // simple query, should initialize state.
    auto result = stm.coordinator(0).get0();
    BOOST_REQUIRE(result);
    BOOST_REQUIRE_EQUAL(result.value(), 0);

    // same query, should return the same result.
    auto result2 = stm.coordinator(0).get0();
    BOOST_REQUIRE(result2);
    BOOST_REQUIRE_EQUAL(result.value(), result2.value());

    absl::btree_map<test_key, test_value> kvs;
    kvs[0] = 99;
    kvs[1] = 100;

    auto result3 = stm.put(kvs).get0();
    BOOST_REQUIRE_EQUAL(result3, cluster::errc::success);

    BOOST_REQUIRE_EQUAL(stm.get(0).get0().value(), test_value{99});
    BOOST_REQUIRE_EQUAL(stm.get(1).get0().value(), test_value{100});
    BOOST_REQUIRE_EQUAL(stm.list().get().value().size(), 2);

    // delete key
    BOOST_REQUIRE(stm.remove(0).get0() == cluster::errc::success);
    // mapping should be gone.
    BOOST_REQUIRE(!stm.get(0).get0().value());
    // other mapping should be retained.
    BOOST_REQUIRE_EQUAL(stm.get(1).get0().value(), test_value{100});
    kvs.erase(0);
    BOOST_REQUIRE_EQUAL(stm.list().get().value().size(), 1);
}

FIXTURE_TEST(test_stm_list, stm_test_fixture) {
    create_stm_and_start_raft(1);
    auto& stm = *_stm;
    stm.start().get0();
    wait_for_confirmed_leader();

    auto result = stm.coordinator(0).get0();
    BOOST_REQUIRE(result);
    BOOST_REQUIRE_EQUAL(result.value(), 0);

    absl::btree_map<test_key, test_value> kvs;
    for (int i = 0; i < 100; ++i) {
        kvs[i] = i * i;
    }
    // List and put can be called concurrently. We don't make any guarentees
    // (it's eventually consistent), but it shouldn't crash.
    for (int i = 0; i < 1000; ++i) {
        kvs[i] = i;
        auto put_fut = stm.put(kvs);
        auto list_fut = stm.list();
        BOOST_REQUIRE_EQUAL(put_fut.get(), cluster::errc::success);
        BOOST_REQUIRE(list_fut.get().has_value());
    }
}

FIXTURE_TEST(test_batched_actions, stm_test_fixture) {
    create_stm_and_start_raft(1);
    auto& stm = *_stm;
    stm.start().get0();
    wait_for_confirmed_leader();

    for (int i = 0; i < 30; i++) {
        auto result = stm.coordinator(i).get0();
        BOOST_REQUIRE(result);
        BOOST_REQUIRE_EQUAL(result.value(), model::partition_id{0});
    }

    absl::btree_map<test_key, test_value> kvs;
    for (int i = 0; i < 30; i++) {
        kvs[i] = test_value{i};
    }
    stm.put(std::move(kvs)).get();

    for (int i = 0; i < 30; i++) {
        BOOST_REQUIRE_EQUAL(stm.get(i).get0().value(), test_value{i});
    }

    // Delete the even keys
    auto result = stm.remove_all([](int key) { return key % 2 == 0; }).get();
    BOOST_REQUIRE_EQUAL(result, cluster::errc::success);
    for (int i = 0; i < 30; i++) {
        if (i % 2 == 0) {
            BOOST_REQUIRE(!stm.get(i).get().value().has_value());
        } else {
            BOOST_REQUIRE_EQUAL(stm.get(i).get().value(), test_value{i});
        }
    }
}

FIXTURE_TEST(test_stm_repartitioning, stm_test_fixture) {
    create_stm_and_start_raft(1);
    auto& stm = *_stm;
    stm.start().get0();
    wait_for_confirmed_leader();

    auto result = stm.coordinator(0).get0();
    BOOST_REQUIRE(result);
    BOOST_REQUIRE_EQUAL(result.value(), 0);

    // load up some keys, should all hash to 0 partition.
    for (int i = 0; i < 99; i++) {
        auto result = stm.coordinator(i).get0();
        BOOST_REQUIRE(result);
        BOOST_REQUIRE_EQUAL(result.value(), model::partition_id{0});
    }

    // repartition to bump the partition count.
    auto repartition_result = stm.repartition(3).get0();
    BOOST_REQUIRE(repartition_result);
    BOOST_REQUIRE_EQUAL(repartition_result.value(), 3);

    // load up more keys that hash to all partitions
    bool partition_1 = false, partition_2 = false;
    for (int i = 100; partition_1 && partition_2; i++) {
        auto result = stm.coordinator(i).get0();
        BOOST_REQUIRE(result);
        BOOST_REQUIRE_GE(result.value(), 0);
        BOOST_REQUIRE_LE(result.value(), 2);
        partition_1 = partition_1 || result.value() == 1;
        partition_2 = partition_2 || result.value() == 2;
    }

    // ensure the original set of keys are still with partition 0;
    for (int i = 0; i < 99; i++) {
        auto result = stm.coordinator(i).get0();
        BOOST_REQUIRE(result);
        BOOST_REQUIRE_EQUAL(result.value(), model::partition_id{0});
    }

    repartition_result = stm.repartition(1).get0();
    BOOST_REQUIRE_EQUAL(
      repartition_result.error(), cluster::errc::invalid_request);
}

FIXTURE_TEST(test_stm_snapshots, stm_test_fixture) {
    create_stm_and_start_raft(1);
    auto& stm = *_stm;
    stm.start().get0();
    wait_for_confirmed_leader();

    // load some data into the stm
    for (int i = 0; i < 99; i++) {
        auto result = stm.coordinator(i).get0();
        BOOST_REQUIRE(result);
    }

    for (int i = 0; i < 99; i++) {
        for (int j = 0; j < 10; j++) {
            absl::btree_map<test_key, test_value> kvs;
            kvs[i] = test_value{j};
            auto result = stm.put(kvs).get0();
            BOOST_REQUIRE(result == cluster::errc::success);
        }
    }

    auto offset = stm.last_applied_offset();
    stm.write_local_snapshot().get0();
    _raft->write_snapshot(raft::write_snapshot_cfg(offset, iobuf())).get0();

    // restart raft after trunaction, ensure snapshot is loaded
    // correctly.
    stop_all();
    create_stm_and_start_raft(1);
    wait_for_confirmed_leader();
    BOOST_REQUIRE_EQUAL(_raft->start_offset(), model::next_offset(offset));
    auto& new_stm = *_stm;
    for (int i = 0; i < 99; i++) {
        auto result = new_stm.get(i).get0();
        BOOST_REQUIRE(result);
        BOOST_REQUIRE_EQUAL(result.value().value(), test_value{9});
    }
}
