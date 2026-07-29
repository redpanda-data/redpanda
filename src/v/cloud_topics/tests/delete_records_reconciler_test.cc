/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/tests/s3_imposter.h"
#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "cloud_topics/level_zero/stm/ctp_stm_api.h"
#include "cloud_topics/state_accessors.h"
#include "kafka/data/partition_proxy.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "redpanda/tests/fixture.h"
#include "ssx/sformat.h"
#include "test_utils/async.h"
#include "test_utils/scoped_config.h"

#include <gtest/gtest.h>

using tests::kafka_produce_transport;
using tests::kv_t;

namespace {
ss::logger test_log("delete_records_reconciler_test");
} // namespace

// Exercises the interaction between DeleteRecords at the high watermark
// (`rpk topic trim-prefix --offset -1`) and the L0 -> L1 reconciler on a
// storage.mode=cloud topic.
class delete_records_reconciler_fixture
  : public s3_imposter_fixture
  , public redpanda_thread_fixture
  , public ::testing::Test {
public:
    delete_records_reconciler_fixture()
      : redpanda_thread_fixture(init_cloud_topics_tag{}, httpd_port_number()) {
        set_expectations_and_listen({});
        wait_for_controller_leadership().get();
    }

    void SetUp() override {
        cluster::topic_properties props;
        props.storage_mode = model::redpanda_storage_mode::cloud;
        props.shadow_indexing = model::shadow_indexing_mode::disabled;
        props.cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::deletion;
        add_topic({model::kafka_namespace, topic_name}, 1, props).get();
        wait_for_leader(ntp).get();
        _partition = app.partition_manager.local().get(ntp);
        _stm = _partition->raft()->stm_manager()->get<cloud_topics::ctp_stm>();
    }

    void TearDown() override {
        for (auto& fn : std::views::reverse(cleanup)) {
            fn();
        }
    }

    kafka_produce_transport* make_producer() {
        auto producer = std::make_unique<kafka_produce_transport>(
          make_kafka_client().get());
        producer->start().get();
        auto* p = producer.get();
        cleanup.emplace_back([p = std::move(producer)] { p->stop().get(); });
        return p;
    }

    void produce_records(kafka_produce_transport* producer, int begin, int n) {
        std::vector<kv_t> batch;
        batch.reserve(n);
        for (int i = begin; i < begin + n; i++) {
            batch.emplace_back(
              ssx::sformat("key{}", i), ssx::sformat("val{}", i));
        }
        producer
          ->produce_to_partition(topic_name, model::partition_id(0), batch)
          .get();
    }

    kafka::offset lro() const {
        return cloud_topics::ctp_stm_api(_stm).get_last_reconciled_offset();
    }

    model::offset lrlo() const {
        return cloud_topics::ctp_stm_api(_stm).get_last_reconciled_log_offset();
    }

    kafka::offset l0_start_offset() const {
        return cloud_topics::ctp_stm_api(_stm).get_start_offset();
    }

    // The L1 metastore's next offset for the partition, i.e. the first offset
    // that has not been reconciled into L1. nullopt if the partition is not
    // tracked in L1 at all yet.
    std::optional<kafka::offset> l1_next_offset() const {
        auto state = _partition->get_cloud_topics_state();
        auto topic_id = _partition->get_topic_config()->get().tp_id;
        if (state == nullptr || !topic_id.has_value()) {
            return std::nullopt;
        }
        auto res = state->local()
                     .get_l1_metastore()
                     ->get_offsets({*topic_id, ntp.tp.partition})
                     .get();
        if (!res.has_value()) {
            return std::nullopt;
        }
        return res.value().next_offset;
    }

    void set_reconciliation_enabled(bool enabled) {
        test_local_cfg.get("cloud_topics_disable_reconciliation_loop")
          .set_value(!enabled);
    }

    void log_state(std::string_view tag) {
        vlog(
          test_log.info,
          "[{}] lro={} lrlo={} l0_start={} l1_next={} local_log_start={} "
          "hwm={} inactive_epoch={}",
          tag,
          lro(),
          lrlo(),
          l0_start_offset(),
          l1_next_offset(),
          _partition->log()->offsets().start_offset,
          _partition->high_watermark(),
          _stm->estimate_inactive_epoch());
    }

    // Drives the partition into the state the finding describes:
    //   * offsets 0..49 reconciled into L1 (LRO == 49),
    //   * offsets 50..99 produced but not reconciled,
    //   * DeleteRecords at the high watermark (100) applied,
    //   * the reconciliation loop running again.
    void wedge_partition(kafka_produce_transport* producer) {
        // Keep the reconciler off so that we control where the LRO sits.
        set_reconciliation_enabled(false);
        for (int i = 0; i < 50; i += 10) {
            produce_records(producer, i, 10);
        }

        // Let the reconciler commit offsets 0..49 into L1, then freeze it.
        set_reconciliation_enabled(true);
        RPTEST_REQUIRE_EVENTUALLY(
          30s, [this] { return lro() >= kafka::offset{49}; });
        set_reconciliation_enabled(false);
        ASSERT_EQ(lro(), kafka::offset{49});
        log_state("reconciled 0..49");

        // Offsets 50..99 are produced while reconciliation is stopped, so they
        // stay above the LRO.
        for (int i = 50; i < 100; i += 10) {
            produce_records(producer, i, 10);
        }
        log_state("produced 50..99, unreconciled");

        // DeleteRecords with offset -1. The Kafka handler resolves -1 to the
        // partition's current high watermark and calls prefix_truncate with it
        // (see kafka/server/handlers/delete_records.cc).
        auto proxy = kafka::make_partition_proxy(_partition);
        auto hwm = proxy.high_watermark();
        ASSERT_EQ(hwm, model::offset{100});
        auto ec
          = proxy.prefix_truncate(hwm, ss::lowres_clock::now() + 30s).get();
        ASSERT_EQ(ec, kafka::error_code::none);
        ASSERT_EQ(proxy.start_offset(), model::offset{100});
        log_state("after trim-prefix to hwm");

        set_reconciliation_enabled(true);
    }

    std::vector<ss::noncopyable_function<void()>> cleanup;
    scoped_config test_local_cfg;
    const model::topic topic_name{"tapioca"};
    model::ntp ntp{model::kafka_namespace, topic_name, 0};
    ss::lw_shared_ptr<cluster::partition> _partition;
    ss::shared_ptr<cloud_topics::ctp_stm> _stm;
};

// After DeleteRecords deletes every record in the partition, no record is
// left that still needs to be handed to L1: the partition must reach a
// caught-up state. Concretely, the last-reconciled offset must cover the
// deleted range, because the LRO is what caps local log truncation
// (ctp_stm::max_collectible_offset) and what unfreezes the partition's
// contribution to the cluster-wide L0 GC watermark.
TEST_F(delete_records_reconciler_fixture, trim_to_hwm_lro_catches_up) {
    auto* producer = make_producer();
    wedge_partition(producer);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());

    log_state("waiting for the LRO to catch up");
    RPTEST_REQUIRE_EVENTUALLY(
      30s, [this] { return lro() >= kafka::offset{99}; });
    log_state("caught up");
}

// Is the stall permanent, or does the next write to the partition release it?
// After the same wedge, produce one more batch and check whether the LRO
// advances over it.
TEST_F(delete_records_reconciler_fixture, trim_to_hwm_then_produce) {
    auto* producer = make_producer();
    wedge_partition(producer);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());

    // Confirm the LRO really is stuck while the partition is idle.
    ss::sleep(5s).get();
    EXPECT_EQ(lro(), kafka::offset{49});
    log_state("idle after trim");

    // Now write offsets 100..109.
    produce_records(producer, 100, 10);
    log_state("produced 100..109");
    RPTEST_REQUIRE_EVENTUALLY(
      30s, [this] { return lro() >= kafka::offset{109}; });
    log_state("after produce");
}
