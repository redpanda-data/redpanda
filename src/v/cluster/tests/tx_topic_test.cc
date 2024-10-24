// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tx_gateway_frontend.h"
#include "config/configuration.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "redpanda/application.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/async.h"

#include <seastar/core/io_priority_class.hh>

#include <boost/test/tools/old/interface.hpp>

#include <chrono>

FIXTURE_TEST(test_tm_stm_new_tx, redpanda_thread_fixture) {
    // call find coordinator to initialize the topic
    auto coordinator = app.tx_gateway_frontend.local()
                         .find_coordinator(
                           kafka::transactional_id{"test-tx-id"})
                         .get();

    auto cfg = app.controller->get_topics_state().local().get_topic_cfg(
      model::tx_manager_nt);

    BOOST_REQUIRE(
      cfg->properties.retention_duration.value()
      == config::shard_local_cfg()
           .transaction_coordinator_delete_retention_ms());
    BOOST_REQUIRE(
      cfg->properties.segment_size.value()
      == config::shard_local_cfg().transaction_coordinator_log_segment_size());

    /**
     * Change property
     */
    static size_t new_segment_size = 1024 * 1024;
    config::shard_local_cfg()
      .transaction_coordinator_log_segment_size.set_value(new_segment_size);

    RPTEST_REQUIRE_EVENTUALLY(10s, [this] {
        auto cfg = app.controller->get_topics_state().local().get_topic_cfg(
          model::tx_manager_nt);

        return new_segment_size == cfg->properties.segment_size.value();
    });

    static std::chrono::milliseconds new_retention_ms(100000);
    config::shard_local_cfg()
      .transaction_coordinator_delete_retention_ms.set_value(new_retention_ms);

    RPTEST_REQUIRE_EVENTUALLY(10s, [this] {
        auto cfg = app.controller->get_topics_state().local().get_topic_cfg(
          model::tx_manager_nt);

        return new_retention_ms == cfg->properties.retention_duration.value();
    });

    cfg = app.controller->get_topics_state().local().get_topic_cfg(
      model::tx_manager_nt);
    // segment size should state the same
    BOOST_REQUIRE_EQUAL(new_segment_size, cfg->properties.segment_size.value());
    /**
     * Change both properties at once
     */
    static size_t newer_segment_size = 20000000;
    static std::chrono::milliseconds newer_retention_ms(500000);
    config::shard_local_cfg()
      .transaction_coordinator_log_segment_size.set_value(newer_segment_size);

    config::shard_local_cfg()
      .transaction_coordinator_delete_retention_ms.set_value(
        newer_retention_ms);
    RPTEST_REQUIRE_EVENTUALLY(10s, [this] {
        auto cfg = app.controller->get_topics_state().local().get_topic_cfg(
          model::tx_manager_nt);

        return newer_retention_ms == cfg->properties.retention_duration.value()
               && cfg->properties.segment_size.value() == newer_segment_size;
    });
}

FIXTURE_TEST(test_tm_stm_eviction, redpanda_thread_fixture) {
    // call find coordinator to initialize the topic
    auto coordinator = app.tx_gateway_frontend.local()
                         .find_coordinator(
                           kafka::transactional_id{"test-tx-id"})
                         .get();
    auto tx_mgr_prts = app.partition_manager.local().get_topic_partition_table(
      model::tx_manager_nt);
    BOOST_REQUIRE(!tx_mgr_prts.empty());
    auto tx_mgr_prt = tx_mgr_prts[model::ntp{
      model::tx_manager_nt.ns,
      model::tx_manager_topic,
      model::partition_id{0}}];
    BOOST_REQUIRE(tx_mgr_prt != nullptr);

    // Start a tx and roll to the next segment.
    auto tx_stm = tx_mgr_prt->raft()->stm_manager()->get<cluster::tm_stm>();
    auto pid = model::producer_identity{1, 0};
    auto op_code = tx_stm
                     ->register_new_producer(
                       tx_mgr_prt->raft()->term(),
                       kafka::transactional_id{"tx-1"},
                       std::chrono::milliseconds(0),
                       pid)
                     .get();
    BOOST_REQUIRE_EQUAL(op_code, cluster::tm_stm::op_status::success);
    auto tx_mgr_log = tx_mgr_prt->log();
    tx_mgr_log->flush().get();
    tx_mgr_log->force_roll(ss::default_priority_class()).get();

    // Start another tx and roll to the next segment.
    op_code = tx_stm
                ->register_new_producer(
                  tx_mgr_prt->raft()->term(),
                  kafka::transactional_id{"tx-2"},
                  std::chrono::milliseconds(0),
                  pid)
                .get();
    BOOST_REQUIRE_EQUAL(op_code, cluster::tm_stm::op_status::success);
    BOOST_REQUIRE_EQUAL(2, tx_mgr_log->segment_count());

    tx_mgr_log->flush().get();
    tx_mgr_log->force_roll(ss::default_priority_class()).get();
    BOOST_REQUIRE_EQUAL(op_code, cluster::tm_stm::op_status::success);
    BOOST_REQUIRE_EQUAL(3, tx_mgr_log->segment_count());

    // Run GC such that we remove the first segment.
    auto size_to_keep = tx_mgr_log->size_bytes()
                        - tx_mgr_log->segments()[0]->size_bytes();
    tx_mgr_log->gc(storage::gc_config{model::timestamp::max(), size_to_keep})
      .get();
    RPTEST_REQUIRE_EVENTUALLY(
      5s, [&] { return tx_mgr_log->segment_count() == 2; });
}
