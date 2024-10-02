// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/log_eviction_stm.h"
#include "raft/tests/raft_group_fixture.h"
#include "redpanda/tests/fixture.h"
#include "storage/disk_log_impl.h"

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

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
    ss::future<model::offset> storage_eviction_event() override {
        logger.info("eviction_stm waiting on storage event");
        vassert(!p.has_value(), "Cannot have value");
        p = ss::promise<model::offset>();
        return p->get_future();
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

FIXTURE_TEST(test_eviction_stm_deadlock, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    auto leader_id = wait_for_group_leader(gr);
    auto leader_raft = gr.get_member(leader_id).consensus;

    /// Publish records and open new segments
    auto impl = leader_raft->log();
    std::vector<storage::offset_stats> offsets;
    for (auto i = 0; i < 5; ++i) {
        replicate_random_batches(gr, 20).get();
        offsets.push_back(impl->offsets());
        impl->force_roll(ss::default_priority_class()).get();
    }

    /// Create an instance of the log eviction stm, however using the subclass
    /// above which allows this test to drive the storage eviction loop
    ss::abort_source as;
    test_log_eviction_stm eviction_stm(
      leader_raft.get(),
      logger,
      gr.get_member(leader_id).storage.local().kvs());
    eviction_stm.start().get();
    auto cleanup = ss::defer([&] {
        // NOTE: this it the natural order of shutdown.
        leader_raft->stop().get();
        as.request_abort();
        eviction_stm.stop().get();
    });

    /// Fufills the promise causing the monitor_log_eviction loop to continue
    const auto highest_term0_offset = offsets[0].dirty_offset;
    eviction_stm.drive_eviction_loop(highest_term0_offset);
    auto next_start_offset = highest_term0_offset + model::offset(1);
    /// Wait until the effect has proceeded, i.e. a snapshot has been taken
    tests::cooperative_spin_wait_with_timeout(
      5s,
      [leader_raft, highest_term0_offset] {
          return leader_raft->last_snapshot_index() == highest_term0_offset;
      })
      .get();
    /// eviction_stm should report the correct new start offset
    BOOST_CHECK_EQUAL(next_start_offset, eviction_stm.effective_start_offset());

    /// Test deadlock, attempt to truncate at offset that will be translated to
    /// be below the raft last snapshot, this should have no effect
    eviction_stm.drive_eviction_loop(next_start_offset);

    /// Then try to evict some more data, if this step fails, previous call had
    /// initiated the deadlock
    const auto highest_term1_offset = offsets[1].dirty_offset;
    eviction_stm.drive_eviction_loop(highest_term1_offset);
    /// Wait until the effect has proceeded, i.e. a snapshot has been taken,
    /// if this does not timeout, this means no deadlock has occurred
    tests::cooperative_spin_wait_with_timeout(
      5s,
      [leader_raft, highest_term1_offset] {
          return leader_raft->last_snapshot_index() >= highest_term1_offset;
      })
      .get();

    next_start_offset = highest_term1_offset + model::offset(1);
    BOOST_CHECK_EQUAL(next_start_offset, eviction_stm.effective_start_offset());
    cleanup.cancel();

    /// Shutdown the eviction stm, the abort source is externally managed
    as.request_abort();
    if (eviction_stm.p) {
        /// Not really an error, allowing any existing futures waited on to
        /// complete will allow the loop to be safely shutdown.
        eviction_stm.p->set_exception(ss::abort_requested_exception());
    }
    eviction_stm.stop().get();
}
