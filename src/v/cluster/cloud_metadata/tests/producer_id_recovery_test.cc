/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_storage/tests/manual_fixture.h"
#include "cluster/archival/archival_metadata_stm.h"
#include "cluster/cloud_metadata/producer_id_recovery_manager.h"
#include "cluster/id_allocator_frontend.h"
#include "cluster/partition.h"
#include "cluster/tests/tx_compaction_utils.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

static ss::logger test_log("pid_recovery");

using namespace cluster::cloud_metadata;

class ProducerIdRecoveryTest
  : public cloud_storage_manual_multinode_test_base
  , public seastar_test {
public:
    ss::future<> SetUpAsync() override {
        cluster::topic_properties props;
        props.cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::compaction;
        co_await add_topic({model::kafka_namespace, topic_name}, 1, props);
        co_await wait_for_leader(ntp);

        partition = app.partition_manager.local().get(ntp).get();
        log = partition->log();
    }

    void generate_txn_data(int num_txns) {
        cluster::tx_executor::spec spec{
          ._num_txes = num_txns,
          ._num_rolls = 3,
          ._types = cluster::tx_executor::mixed,
          ._interleave = true,
          ._compact = false};
        cluster::tx_executor{}.run_random_workload(
          spec, partition->raft()->term(), partition->rm_stm(), log);
    }

protected:
    const model::topic topic_name{"pid_recovery_test_topic"};
    const model::ntp ntp{model::kafka_namespace, topic_name, 0};
    cluster::partition* partition;
    ss::shared_ptr<storage::log> log;
};

class ProducerIdRecoveryParamTest
  : public ProducerIdRecoveryTest
  , public ::testing::WithParamInterface<bool> {};

TEST_P(ProducerIdRecoveryParamTest, TestBasicRecovery) {
    bool init_id_allocator_tp = GetParam();
    auto num_txns = 10;
    auto last_pid = model::producer_id{num_txns - 1};
    generate_txn_data(num_txns);
    ASSERT_EQ(last_pid, partition->rm_stm()->highest_producer_id());

    // Because our workload is fake, we shouldn't have used any id_allocator
    // IDs. We can validate this by checking the existence of the id_allocator.
    auto& topics_table = app.controller->get_topics_state().local();
    ASSERT_FALSE(
      topics_table.get_topic_metadata(model::id_allocator_nt).has_value());

    if (init_id_allocator_tp) {
        auto id_reply = app.id_allocator_frontend.local().allocate_id(1s).get();
        ASSERT_EQ(1, id_reply.id);
        ASSERT_TRUE(
          topics_table.get_topic_metadata(model::id_allocator_nt).has_value());
    }

    // Regardless of what whether we previously initialized the id_allocator,
    // we should see it after recovery.
    producer_id_recovery_manager recovery_mgr(
      app.controller->get_members_table(),
      app._connection_cache,
      app.id_allocator_frontend);
    ASSERT_EQ(error_outcome::success, recovery_mgr.recover().get());
    ASSERT_TRUE(
      topics_table.get_topic_metadata(model::id_allocator_nt).has_value());

    // We should start we our data left off.
    auto id_reply = app.id_allocator_frontend.local().allocate_id(1s).get();
    ASSERT_EQ(last_pid() + 1, id_reply.id);

    id_reply = app.id_allocator_frontend.local().allocate_id(1s).get();
    ASSERT_EQ(last_pid() + 2, id_reply.id);

    // In the event we recover while the id_allocator has state, the state
    // should never go lower.
    ASSERT_EQ(error_outcome::success, recovery_mgr.recover().get());
    id_reply = app.id_allocator_frontend.local().allocate_id(1s).get();
    ASSERT_EQ(last_pid() + 3, id_reply.id);
}

INSTANTIATE_TEST_SUITE_P(
  ShouldInitIdAllocator, ProducerIdRecoveryParamTest, ::testing::Bool());

TEST_F(ProducerIdRecoveryTest, TestRecoveryDoesntLower) {
    auto num_txns = 10;
    auto last_pid = model::producer_id{num_txns - 1};
    generate_txn_data(num_txns);
    ASSERT_EQ(last_pid, partition->rm_stm()->highest_producer_id());

    producer_id_recovery_manager recovery_mgr(
      app.controller->get_members_table(),
      app._connection_cache,
      app.id_allocator_frontend);
    ASSERT_EQ(error_outcome::success, recovery_mgr.recover().get());
    for (int i = 0; i < 3; i++) {
        auto id_reply = app.id_allocator_frontend.local().allocate_id(1s).get();
        ASSERT_EQ(last_pid() + 1 + i, id_reply.id);
    }
    ASSERT_EQ(error_outcome::success, recovery_mgr.recover().get());
    auto id_reply = app.id_allocator_frontend.local().allocate_id(1s).get();

    // The next pid shouldn't go down.
    ASSERT_EQ(last_pid() + 4, id_reply.id);
}
