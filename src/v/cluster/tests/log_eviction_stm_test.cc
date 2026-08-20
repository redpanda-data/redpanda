// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/log_eviction_stm.h"
#include "raft/tests/raft_fixture.h"
#include "test_utils/async.h"

ss::logger logger("eviction_stm_test");

class test_log_eviction_stm : public cluster::log_eviction_stm {
public:
    test_log_eviction_stm(
      raft::consensus* c, ss::logger& logger, storage::kvstore& kvs)
      : cluster::log_eviction_stm(c, logger, kvs) {}

    /**
     * The two following methods can be used to drive the eviction stms
     * storage eviction event processing loop. It works by overriding a method
     * called storage_eviction_event() and replacing this with a manually
     * controlled promise that can be driven by the test writer.
     *
     * Ensure to call \ref drive_eviction_loop once its expected that the effect
     * it should complete has been performed. By that time it can be guaranteed
     * that the event loop will call storage_eviction_event() for the next
     * future.
     */
    ss::future<model::offset>
    storage_eviction_event(ss::abort_source& as) override {
        logger.info("eviction_stm waiting on storage event");
        vassert(!p.has_value(), "Cannot have value");
        if (as.abort_requested()) {
            return ss::make_exception_future<model::offset>(
              ss::abort_requested_exception());
        }
        p = ss::promise<model::offset>();
        /// Use passed abort source so worker's stop() wakes a parked fiber
        /// without test involvement.
        auto sub = as.subscribe([this]() noexcept {
            if (p.has_value()) {
                p->set_exception(ss::abort_requested_exception());
                p.reset();
            }
        });
        return p->get_future().finally([sub = std::move(sub)] {});
    }

    void drive_eviction_loop(model::offset o) {
        logger.info("Driving eviction_stm loop with value: {}", o);
        /// Its possible the test wants to drive the next iteration of the loop
        /// but the loop hasn't yet called storage_eviction_event(), wait a max
        /// of 5s for this to occur
        tests::cooperative_spin_wait_with_timeout(5s, [this] {
            return p.has_value();
        }).get();
        p->set_value(o);
        p.reset();
    }

    std::optional<ss::promise<model::offset>> p;
};

struct eviction_stm_fixture : public raft::raft_fixture {};

TEST_F(eviction_stm_fixture, test_eviction_stm_deadlock) {
    for (int i = 0; i < 3; ++i) {
        add_node(model::node_id(0), model::revision_id(0));
    }

    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder stm_mgr_builder;
        node->initialise(all_vnodes()).get();
        stm_mgr_builder.create_stm<test_log_eviction_stm>(
          node->raft().get(), logger(), node->get_kvstore());

        node->start(std::move(stm_mgr_builder)).get();
        node->raft()
          ->stm_manager()
          ->get<test_log_eviction_stm>()
          ->sync_bg_fiber_to_mode()
          .get();
    }
    std::vector<storage::offset_stats> offsets;
    for (auto i = 0; i < 5; ++i) {
        auto leader_id = wait_for_leader(10s).get();
        auto& leader = node(leader_id);

        leader.raft()
          ->replicate(
            make_batches(20, 1, 128),
            raft::replicate_options(raft::consistency_level::quorum_ack))
          .get();

        auto log = leader.raft()->log();
        offsets.push_back(log->offsets());
        log->force_roll().get();
    }
    auto leader_id = wait_for_leader(10s).get();
    auto& leader = node(leader_id);
    /// Fufills the promise causing the monitor_log_eviction loop to continue
    const auto highest_term0_offset = offsets[0].dirty_offset;
    auto eviction_stm
      = leader.raft()->stm_manager()->get<test_log_eviction_stm>();
    eviction_stm->drive_eviction_loop(highest_term0_offset);
    auto next_start_offset = highest_term0_offset + model::offset(1);
    /// Wait until the effect has proceeded, i.e. a snapshot has been taken
    tests::cooperative_spin_wait_with_timeout(
      5s,
      [&leader, highest_term0_offset] {
          return leader.raft()->last_snapshot_index() == highest_term0_offset;
      })
      .get();
    /// eviction_stm should report the correct new start offset
    ASSERT_EQ(next_start_offset, eviction_stm->effective_start_offset());
    ss::sleep(5s).get();
    /// Test deadlock, attempt to truncate at offset that will be translated
    // to be below the raft last snapshot, this should have no effect
    eviction_stm->drive_eviction_loop(next_start_offset);

    /// Then try to evict some more data, if this step fails, previous call
    /// const had initiated the deadlock
    auto highest_term1_offset = offsets[1].dirty_offset;
    eviction_stm->drive_eviction_loop(highest_term1_offset);
    /// Wait until the effect has proceeded, i.e. a snapshot has been taken,
    /// if this does not timeout, this means no deadlock has occurred
    tests::cooperative_spin_wait_with_timeout(
      5s,
      [&leader, highest_term1_offset] {
          return leader.raft()->last_snapshot_index() >= highest_term1_offset;
      })
      .get();

    next_start_offset = highest_term1_offset + model::offset(1);
    ASSERT_EQ(next_start_offset, eviction_stm->effective_start_offset());
}

TEST_F(eviction_stm_fixture, test_bg_worker_stops_on_cloud_transition) {
    add_node(model::node_id(0), model::revision_id(0));
    for (auto& [id, node] : nodes()) {
        raft::state_machine_manager_builder stm_mgr_builder;
        node->initialise(all_vnodes()).get();
        stm_mgr_builder.create_stm<cluster::log_eviction_stm>(
          node->raft().get(), logger(), node->get_kvstore());
        node->start(std::move(stm_mgr_builder)).get();
    }
    auto& n = node(model::node_id(0));
    auto stm = n.raft()->stm_manager()->get<cluster::log_eviction_stm>();
    using accessor = cluster::testing::log_eviction_stm_accessor;

    /// The fibers do not start with the stm; the partition starts them once
    /// the durable storage mode has been fed into ntp_config.
    ASSERT_FALSE(accessor::bg_fiber_running(*stm));

    /// A partition that is not a cloud topic runs the eviction fibers.
    stm->sync_bg_fiber_to_mode().get();
    ASSERT_TRUE(accessor::bg_fiber_running(*stm));

    /// Flipping the partition's storage mode to cloud stops the fibers on the
    /// next sync, as the partition-mode-change path will do.
    n.raft()->log()->set_partition_storage_mode(
      model::redpanda_storage_mode::cloud);
    stm->sync_bg_fiber_to_mode().get();
    ASSERT_FALSE(accessor::bg_fiber_running(*stm));
}
