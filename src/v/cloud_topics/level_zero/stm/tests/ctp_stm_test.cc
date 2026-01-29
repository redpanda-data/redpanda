/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/stm/ctp_stm.h"
#include "cloud_topics/level_zero/stm/ctp_stm_api.h"
#include "cloud_topics/level_zero/stm/placeholder.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/types.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "raft/tests/raft_fixture.h"
#include "ssx/when_all.h"
#include "test_utils/async.h"

#include <optional>

namespace ct = cloud_topics;
using namespace std::chrono_literals;

struct ctp_stm_api_accessor {
    ss::future<std::expected<model::offset, ct::ctp_stm_api_errc>>
    replicated_apply(model::record_batch rb, ss::abort_source& as) {
        // This function is used to access the private method of ctp_stm_api
        return api.replicated_apply(std::move(rb), model::no_timeout, as);
    }
    ct::ctp_stm_api api;
};

namespace cloud_topics {
struct ctp_stm_accessor {
    auto take_snapshot(ctp_stm& stm) { return stm.take_local_snapshot({}); }

    auto install_snapshot(ctp_stm& stm, raft::stm_snapshot snapshot) {
        return stm.apply_local_snapshot(
          snapshot.header, std::move(snapshot.data));
    }

    bool epoch_cv_has_waiters(ctp_stm& stm) {
        return stm._epoch_updated_cv.has_waiters();
    }
};
} // namespace cloud_topics

class ctp_stm_fixture : public raft::stm_raft_fixture<ct::ctp_stm> {
public:
    ss::future<> start() {
        enable_offset_translation();
        co_await initialize_state_machines();
    }

    stm_shptrs_t create_stms(
      raft::state_machine_manager_builder& builder,
      raft::raft_node_instance& node) override {
        return builder.create_stm<ct::ctp_stm>(ct::cd_log, node.raft().get());
    }

    ct::ctp_stm_api api(raft::raft_node_instance& node) {
        return ct::ctp_stm_api(get_stm<0>(node));
    }

    model::record_batch make_record_batch(
      ct::cluster_epoch e,
      model::offset base_offset,
      int32_t seq,
      std::optional<int> size = std::nullopt) {
        ct::object_id id = ct::object_id::create(e);
        ct::ctp_placeholder placeholder{
          .id = id,
          .offset = ct::first_byte_offset_t{0},
          .size_bytes = ct::byte_range_size_t{0},
        };

        storage::record_batch_builder builder(
          model::record_batch_type::ctp_placeholder, base_offset);

        auto first_value = serde::to_iobuf(placeholder);

        builder.add_raw_kv(std::nullopt, std::move(first_value));
        if (size.has_value()) {
            for (int i = 1; i < size.value(); ++i) {
                builder.add_raw_kv(std::nullopt, std::nullopt);
            }
        }

        auto ph = std::move(builder).build();
        ph.header().first_timestamp = model::timestamp::now();
        ph.header().max_timestamp = model::timestamp::now();
        ph.header().base_sequence = seq;
        ph.header().header_crc = model::internal_header_only_crc(ph.header());
        return ph;
    }

    ss::future<std::expected<model::offset, ct::ctp_stm_api_errc>>
    replicate_record_batch(
      raft::raft_node_instance& node, model::record_batch rb) {
        ctp_stm_api_accessor accessor{.api = api(node)};
        co_return co_await accessor.replicated_apply(std::move(rb), as);
    }

    /// Helper method that follows the producer pattern: fence → replicate
    /// Fences the given epoch, and if successful, replicates a batch with that
    /// epoch. Returns true if both operations succeeded.
    ss::future<bool> replicate_with_epoch(
      raft::raft_node_instance& node,
      ct::cluster_epoch epoch,
      model::offset base_offset,
      int32_t seq = 0) {
        // Fence the epoch first (like producer does)
        auto fence_result = co_await api(node).fence_epoch(epoch);
        if (!fence_result.has_value()) {
            co_return false;
        }

        // Keep the fence guard alive during replication
        auto fence_guard = std::move(fence_result.value());

        // Then replicate with that epoch
        auto batch = make_record_batch(epoch, base_offset, seq);
        auto res = co_await replicate_record_batch(node, std::move(batch));

        // fence_guard released here when it goes out of scope
        co_return res.has_value();
    }

    ss::abort_source as;
};

TEST_F_CORO(ctp_stm_fixture, test_basic) {
    // Test replicates L0 metadata batch and checks that the epoch is updated
    co_await start();

    co_await wait_for_leader(raft::default_timeout());

    auto gc_epoch = co_await api(node(*get_leader())).get_inactive_epoch();

    ASSERT_TRUE_CORO(gc_epoch);
    ASSERT_FALSE_CORO(gc_epoch.value().has_value());

    auto b = make_record_batch(ct::cluster_epoch{1}, model::offset{0}, 0);
    auto res = co_await replicate_record_batch(
      node(*get_leader()), std::move(b));
    ASSERT_TRUE_CORO(res.has_value());

    auto max_epoch = api(node(*get_leader())).get_max_epoch();
    auto max_seen_epoch = api(node(*get_leader())).get_max_seen_epoch();
    ASSERT_TRUE_CORO(max_epoch.has_value());
    ASSERT_TRUE_CORO(max_seen_epoch.has_value());
    ASSERT_EQ_CORO(max_epoch.value(), ct::cluster_epoch{1});
    ASSERT_EQ_CORO(max_seen_epoch.value(), ct::cluster_epoch{1});

    gc_epoch = co_await api(node(*get_leader())).get_inactive_epoch();
    ASSERT_TRUE_CORO(gc_epoch);
    ASSERT_TRUE_CORO(gc_epoch.value().has_value());
    ASSERT_EQ_CORO(gc_epoch.value().value(), ct::cluster_epoch{0});
}

TEST_F_CORO(ctp_stm_fixture, test_fencing) {
    co_await start();

    co_await wait_for_leader(raft::default_timeout());

    auto gc_epoch = co_await api(node(*get_leader())).get_inactive_epoch();

    ASSERT_TRUE_CORO(gc_epoch);
    ASSERT_FALSE_CORO(gc_epoch->has_value());

    auto b1 = make_record_batch(ct::cluster_epoch{2}, model::offset{0}, 0);
    auto res = co_await replicate_record_batch(
      node(*get_leader()), std::move(b1));
    ASSERT_TRUE_CORO(res.has_value());

    auto max_epoch = api(node(*get_leader())).get_max_epoch();
    auto max_seen_epoch = api(node(*get_leader())).get_max_seen_epoch();
    ASSERT_TRUE_CORO(max_epoch.has_value());
    ASSERT_TRUE_CORO(max_seen_epoch.has_value());
    ASSERT_EQ_CORO(max_epoch.value(), ct::cluster_epoch{2});
    ASSERT_EQ_CORO(max_seen_epoch.value(), ct::cluster_epoch{2});

    // Acquire the fence for epoch 2 (should succeed)
    {
        auto fence
          = co_await api(node(*get_leader())).fence_epoch(ct::cluster_epoch{2});
        ASSERT_TRUE_CORO(fence.has_value());
        // Read fence
        ASSERT_EQ_CORO(fence.value().unit.count(), 1);
    }

    // Acquire the fence for epoch 1 (should fail)
    {
        auto fence
          = co_await api(node(*get_leader())).fence_epoch(ct::cluster_epoch{1});
        ASSERT_FALSE_CORO(fence.has_value());
    }

    // Advance max_seen_epoch to 3.
    auto write_fence
      = co_await api(node(*get_leader())).fence_epoch(ct::cluster_epoch{3});
    ASSERT_TRUE_CORO(write_fence.has_value());

    // Out of order fence for epoch 1 (should be waiting for the fence to be
    // released)
    auto leader_api = api(node(*get_leader()));
    auto fut = leader_api.fence_epoch(ct::cluster_epoch{1});
    co_await ss::sleep(100ms);

    write_fence = {};

    auto read_fence = co_await std::move(fut);
    ASSERT_FALSE_CORO(read_fence.has_value());
}

TEST_F_CORO(ctp_stm_fixture, test_last_reconciled_offset) {
    // This test checks reconciliation in the ctp_stm in case if
    // epoch spans a single offset.
    co_await start();

    co_await wait_for_leader(raft::default_timeout());

    auto gc_epoch = co_await api(node(*get_leader())).get_inactive_epoch();

    ASSERT_TRUE_CORO(gc_epoch);
    ASSERT_FALSE_CORO(gc_epoch->has_value());

    auto b1 = make_record_batch(ct::cluster_epoch{1}, model::offset{0}, 0);
    auto res1 = co_await replicate_record_batch(
      node(*get_leader()), std::move(b1));
    ASSERT_TRUE_CORO(res1.has_value());

    auto b2 = make_record_batch(ct::cluster_epoch{2}, model::offset{1}, 1);
    auto res2 = co_await replicate_record_batch(
      node(*get_leader()), std::move(b2));
    ASSERT_TRUE_CORO(res2.has_value());

    auto max_epoch = api(node(*get_leader())).get_max_epoch();
    auto max_seen_epoch = api(node(*get_leader())).get_max_seen_epoch();
    ASSERT_TRUE_CORO(max_epoch.has_value());
    ASSERT_TRUE_CORO(max_seen_epoch.has_value());
    ASSERT_EQ_CORO(max_epoch.value(), ct::cluster_epoch{2});
    ASSERT_EQ_CORO(max_seen_epoch.value(), ct::cluster_epoch{2});

    auto gc_epoch_before
      = co_await api(node(*get_leader())).get_inactive_epoch();
    ASSERT_TRUE_CORO(gc_epoch_before);
    ASSERT_TRUE_CORO(gc_epoch_before->has_value());
    ASSERT_EQ_CORO(gc_epoch_before->value(), ct::cluster_epoch{0});

    // Advance reconciled offset to the first batch (b1),
    // now b1 is reconciled and can be removed alongside its epoch (1).
    // First referenced epoch is now 2.
    co_await api(node(*get_leader()))
      .advance_reconciled_offset(kafka::offset{0}, model::no_timeout, as);

    // Check that max and max_seen_epochs remain the same
    auto max_epoch_after = api(node(*get_leader())).get_max_epoch();
    auto max_seen_epoch_after = api(node(*get_leader())).get_max_seen_epoch();
    ASSERT_TRUE_CORO(max_epoch_after.has_value());
    ASSERT_TRUE_CORO(max_seen_epoch_after.has_value());
    ASSERT_EQ_CORO(max_epoch_after.value(), ct::cluster_epoch{2});
    ASSERT_EQ_CORO(max_seen_epoch_after.value(), ct::cluster_epoch{2});

    // Check that first epoch to remove has moved forward
    auto gc_epoch_after
      = co_await api(node(*get_leader())).get_inactive_epoch();
    ASSERT_TRUE_CORO(gc_epoch_after.has_value());
    ASSERT_TRUE_CORO(gc_epoch_after->has_value());
    ASSERT_EQ_CORO(gc_epoch_after->value(), ct::cluster_epoch{1});

    // Advance reconciled offset to the b2 batch.
    // Now all epochs can be discarded.
    co_await api(node(*get_leader()))
      .advance_reconciled_offset(kafka::offset{1}, model::no_timeout, as);

    max_epoch_after = api(node(*get_leader())).get_max_epoch();
    max_seen_epoch_after = api(node(*get_leader())).get_max_seen_epoch();
    ASSERT_TRUE_CORO(max_epoch_after.has_value());
    ASSERT_TRUE_CORO(max_seen_epoch_after.has_value());

    // We know that b2 started epoch 2 but we don't yet know where it ends
    gc_epoch_after = co_await api(node(*get_leader())).get_inactive_epoch();
    ASSERT_TRUE_CORO(gc_epoch_after.has_value());
    ASSERT_FALSE_CORO(gc_epoch_after->has_value());
}

TEST_F_CORO(ctp_stm_fixture, test_truncate_all_epochs) {
    // This test gradually adds epochs and removes them by advancing the
    // reconciled offset. It checks that the epochs are removed correctly and
    // that the state is consistent. Then it adds more epochs and checks that
    // the state is still consistent.
    co_await start();
    co_await wait_for_leader(raft::default_timeout());

    auto gc_epoch = co_await api(node(*get_leader())).get_inactive_epoch();

    ASSERT_TRUE_CORO(gc_epoch);
    ASSERT_FALSE_CORO(gc_epoch->has_value());

    model::offset last_offset = model::offset{0};
    ct::cluster_epoch last_epoch = ct::cluster_epoch{0};
    for (int i = 0; i < 100; i += 10) {
        last_offset = model::offset(i);
        last_epoch = ct::cluster_epoch(i / 2);
        auto b = make_record_batch(last_epoch, last_offset, i, 10);
        auto res = co_await replicate_record_batch(
          node(*get_leader()), std::move(b));
        ASSERT_TRUE_CORO(res.has_value());
    }

    auto max_epoch = api(node(*get_leader())).get_max_epoch();
    auto max_seen_epoch = api(node(*get_leader())).get_max_seen_epoch();
    ASSERT_TRUE_CORO(max_epoch.has_value());
    ASSERT_TRUE_CORO(max_seen_epoch.has_value());
    ASSERT_EQ_CORO(max_epoch.value(), last_epoch);
    ASSERT_EQ_CORO(max_seen_epoch.value(), last_epoch);
    // Nothing yet reconciled
    auto inactive_epoch
      = co_await api(node(*get_leader())).get_inactive_epoch();
    ASSERT_TRUE_CORO(inactive_epoch);
    ASSERT_FALSE_CORO(inactive_epoch->has_value());

    // Advance reconciled offset to the middle of the first epoch
    co_await api(node(*get_leader()))
      .advance_reconciled_offset(kafka::offset(50), model::no_timeout, as);
    ss::abort_source as;
    co_await api(node(*get_leader())).sync_in_term(model::no_timeout, as);
    inactive_epoch = co_await api(node(*get_leader())).get_inactive_epoch();
    ASSERT_TRUE_CORO(inactive_epoch);
    ASSERT_TRUE_CORO(inactive_epoch->has_value());
    ASSERT_EQ_CORO(inactive_epoch->value(), ct::cluster_epoch{24});

    // Advance reconciled offset exactly to the end of the first epoch
    co_await api(node(*get_leader()))
      .advance_reconciled_offset(kafka::offset(99), model::no_timeout, as);
    inactive_epoch = co_await api(node(*get_leader())).get_inactive_epoch();
    max_epoch = api(node(*get_leader())).get_max_epoch();
    ASSERT_TRUE_CORO(inactive_epoch);
    ASSERT_TRUE_CORO(max_epoch);
    ASSERT_FALSE_CORO(inactive_epoch->has_value());
    ASSERT_EQ_CORO(max_epoch.value(), last_epoch);
}

TEST_F_CORO(ctp_stm_fixture, test_start_offset) {
    co_await start();
    co_await wait_for_leader(raft::default_timeout());
    auto& leader = node(*get_leader());
    auto leader_api = api(leader);
    auto b1 = make_record_batch(ct::cluster_epoch{1}, model::offset{0}, 0);
    auto res1 = co_await replicate_record_batch(leader, std::move(b1));
    ASSERT_TRUE_CORO(res1.has_value());
    auto b2 = make_record_batch(ct::cluster_epoch{2}, model::offset{1}, 1);
    auto res2 = co_await replicate_record_batch(leader, std::move(b2));
    ASSERT_TRUE_CORO(res2.has_value());

    auto start_offset = leader_api.get_start_offset();
    ASSERT_EQ_CORO(start_offset, kafka::offset{0});

    co_await leader_api.set_start_offset(
      kafka::offset{1}, model::no_timeout, as);

    start_offset = leader_api.get_start_offset();
    ASSERT_EQ_CORO(start_offset, kafka::offset{1});

    co_await leader_api.set_start_offset(
      kafka::offset{2}, model::no_timeout, as);
    start_offset = leader_api.get_start_offset();
    ASSERT_EQ_CORO(start_offset, kafka::offset{2});

    co_await leader_api.set_start_offset(
      kafka::offset{1}, model::no_timeout, as);
    start_offset = leader_api.get_start_offset();
    ASSERT_EQ_CORO(start_offset, kafka::offset{2});
}

TEST_F_CORO(ctp_stm_fixture, truncates_below_lro) {
    co_await start();
    co_await wait_for_leader(raft::default_timeout());
    auto& leader = node(*get_leader());
    EXPECT_EQ(leader.raft()->last_snapshot_index(), model::offset::min());
    auto leader_api = api(leader);
    // Write some data
    for (int o = 0; o < 1024; ++o) {
        co_await replicate_record_batch(
          leader, make_record_batch(ct::cluster_epoch{1}, model::offset{o}, 0));
    }
    // Wait for all nodes to replicate up to offset 1023 before rolling.
    co_await wait_for_committed_offset(model::offset{1023}, 10s);
    // Segment roll on all the nodes so we can take a snapshot.
    for (auto& vnode : all_vnodes()) {
        co_await node(vnode.id()).raft()->log()->force_roll();
    }
    // Write some more data
    for (int o = 0; o < 1024; ++o) {
        co_await replicate_record_batch(
          leader, make_record_batch(ct::cluster_epoch{1}, model::offset{o}, 0));
    }
    // Advance the LRO
    co_await leader_api.advance_reconciled_offset(
      kafka::offset{2000}, model::no_timeout, as);
    // Wait for the snapshot to be created
    for (auto& vnode : all_vnodes()) {
        RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [this, &vnode]() {
            return node(vnode.id()).raft()->last_snapshot_index()
                   == model::offset{1024};
        });
    }
}

TEST_F_CORO(ctp_stm_fixture, can_replay_truncated_log) {
    co_await start();
    co_await wait_for_leader(raft::default_timeout());
    auto& leader = node(get_leader().value());
    EXPECT_EQ(leader.raft()->last_snapshot_index(), model::offset::min());
    auto leader_api = api(leader);
    // Write some data
    for (int o = 0; o < 1024; ++o) {
        co_await replicate_record_batch(
          leader, make_record_batch(ct::cluster_epoch{1}, model::offset{o}, 0));
    }
    // Wait for all nodes to replicate up to offset 1023 before rolling.
    co_await wait_for_committed_offset(model::offset{1023}, 10s);
    // Segment roll on all the nodes so we can take a snapshot.
    for (auto& vnode : all_vnodes()) {
        co_await node(vnode.id()).raft()->log()->force_roll();
    }
    // Write some more data
    for (int o = 0; o < 1024; ++o) {
        co_await replicate_record_batch(
          leader, make_record_batch(ct::cluster_epoch{1}, model::offset{o}, 0));
    }
    // Advance the LRO to a low value that will be truncated away
    co_await leader_api.advance_reconciled_offset(
      kafka::offset{1}, model::no_timeout, as);
    // Advance the LRO to truncate what the previous batch pointed too
    co_await leader_api.advance_reconciled_offset(
      kafka::offset{2000}, model::no_timeout, as);
    // Wait for the snapshot to be created
    for (auto& vnode : all_vnodes()) {
        RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [this, &vnode]() {
            return node(vnode.id()).raft()->last_snapshot_index()
                   == model::offset{1024};
        });
    }
    auto follower_id = random_follower_id().value();
    vlog(ct::cd_log.info, "restarting node {}", follower_id);
    auto dirty_offset = leader.raft()->dirty_offset();
    co_await restart_node_and_delete_data(follower_id);
    co_await wait_for_committed_offset(dirty_offset, 10s);
    auto follower_stm = get_stm<0>(node(follower_id));
    co_await follower_stm->wait(dirty_offset, model::no_timeout);
    vlog(ct::cd_log.info, "recovery done: {}", follower_id);
}

TEST_F_CORO(ctp_stm_fixture, test_snapshot) {
    co_await start();

    co_await wait_for_leader(raft::default_timeout());

    auto gc_epoch = co_await api(node(*get_leader())).get_inactive_epoch();

    ASSERT_TRUE_CORO(gc_epoch);
    ASSERT_FALSE_CORO(gc_epoch->has_value());

    auto b1 = make_record_batch(ct::cluster_epoch{2}, model::offset{0}, 0);
    auto res = co_await replicate_record_batch(
      node(*get_leader()), std::move(b1));
    ASSERT_TRUE_CORO(res.has_value());

    auto max_epoch = api(node(*get_leader())).get_max_epoch();
    auto max_seen_epoch = api(node(*get_leader())).get_max_seen_epoch();
    ASSERT_TRUE_CORO(max_epoch.has_value());
    ASSERT_TRUE_CORO(max_seen_epoch.has_value());
    ASSERT_EQ_CORO(max_epoch.value(), ct::cluster_epoch{2});
    ASSERT_EQ_CORO(max_seen_epoch.value(), ct::cluster_epoch{2});

    auto& leader = node(*get_leader());
    auto stm = get_stm<0>(leader);
    ct::ctp_stm_accessor a;
    auto snapshot = co_await a.take_snapshot(*stm);

    co_await a.install_snapshot(*stm, std::move(snapshot));

    // Acquire the fence for epoch 1 (should fail)
    {
        auto fence = co_await api(leader).fence_epoch(ct::cluster_epoch{1});
        ASSERT_FALSE_CORO(fence.has_value());
    }
}

TEST_F_CORO(ctp_stm_fixture, test_fence_epoch_concurrent_new_epoch) {
    // This test verifies the optimization in fence_epoch() where multiple
    // concurrent requests for a new epoch only require one write lock.
    // The first request acquires the epoch_update_lock and write lock, updates
    // the epoch, then signals waiters. The remaining requests wake up and take
    // the read-lock path since the epoch has been updated.
    co_await start();
    co_await wait_for_leader(raft::default_timeout());

    auto& leader = node(*get_leader());
    auto stm = get_stm<0>(leader);
    ct::ctp_stm_accessor accessor;

    // First, establish epoch 1 by replicating a batch
    auto b1 = make_record_batch(ct::cluster_epoch{1}, model::offset{0}, 0);
    auto res = co_await replicate_record_batch(leader, std::move(b1));
    ASSERT_TRUE_CORO(res.has_value());

    // Verify epoch 1 is established
    auto max_epoch = api(leader).get_max_epoch();
    ASSERT_TRUE_CORO(max_epoch.has_value());
    ASSERT_EQ_CORO(max_epoch.value(), ct::cluster_epoch{1});

    // Launch multiple concurrent fence_epoch calls for epoch 2 (a new epoch).
    // All of these will initially see max_seen_epoch=1 and need to bump to 2.
    // With the optimization:
    // - One request acquires _epoch_update_lock, gets write lock, updates epoch
    // - Others wait on condition variable, then take read-lock path
    constexpr size_t num_concurrent_requests = 10;
    using expected_t
      = std::expected<ct::cluster_epoch_fence, ct::stale_cluster_epoch>;
    std::vector<ss::future<expected_t>> futures;
    futures.reserve(num_concurrent_requests);

    {
        auto leader_api = api(leader);
        // Make a single request which fences the current epoch (thus holding a
        // read lock)
        auto initial_fence = co_await leader_api.fence_epoch(
          ct::cluster_epoch{1});
        ASSERT_TRUE_CORO(initial_fence.has_value());
        // Push back a number of futures which will not yet be able to be
        // resolved since a read lock is outstanding, forcing a number of
        // requests to become waiters while a single request waits for a
        // write lock- previously, this would have resulted in all requests
        // waiting on a write lock sequentially.
        for (size_t i = 0; i < num_concurrent_requests; ++i) {
            futures.push_back(leader_api.fence_epoch(ct::cluster_epoch{2}));
        }
        // All requests but one should be waiting on cv.
        RPTEST_REQUIRE_EVENTUALLY_CORO(
          10s, [&] { return accessor.epoch_cv_has_waiters(*stm); });
        // Let `initial_fence` go out of scope.
    }

    // Wait for all fences to be acquired - they should all be able to succeed
    // without hanging because only one request (the first request) had to
    // obtain a write lock, which then downgraded to a read lock, and the rest
    // of the requests could obtain read locks for the current epoch.
    auto fences = co_await ssx::when_all_succeed<std::vector<expected_t>>(
      std::move(futures));
    for (auto& fence : fences) {
        ASSERT_TRUE_CORO(fence.has_value())
          << "All fence requests should succeed";
        ASSERT_EQ_CORO(fence->unit.count(), 1);
    }

    ASSERT_FALSE_CORO(accessor.epoch_cv_has_waiters(*stm));

    // Verify epoch 2 is now established
    auto max_seen = api(leader).get_max_seen_epoch();
    ASSERT_TRUE_CORO(max_seen.has_value());
    ASSERT_EQ_CORO(max_seen.value(), ct::cluster_epoch{2});
}

TEST_F_CORO(ctp_stm_fixture, test_previous_epoch_fencing_with_lro) {
    // This test verifies that:
    // 1. The previous epoch is correctly tracked through epoch application and
    // fencing
    // 2. Out-of-order epochs can only be fenced if they are >= previous epoch
    // 3. When LRO is propagated, the inactive epoch computation respects the
    //    previous epoch invariant (inactive_epoch < previous_epoch)
    co_await start();
    co_await wait_for_leader(raft::default_timeout());

    auto& leader = node(*get_leader());
    auto leader_api = api(leader);

    // Establish epoch 1
    auto b1 = make_record_batch(ct::cluster_epoch{1}, model::offset{0}, 0, 10);
    auto res1 = co_await replicate_record_batch(leader, std::move(b1));
    ASSERT_TRUE_CORO(res1.has_value());

    // Establish epoch 2
    auto b2 = make_record_batch(
      ct::cluster_epoch{2}, model::offset{10}, 10, 10);
    auto res2 = co_await replicate_record_batch(leader, std::move(b2));
    ASSERT_TRUE_CORO(res2.has_value());

    // Establish epoch 3
    auto b3 = make_record_batch(
      ct::cluster_epoch{3}, model::offset{20}, 20, 10);
    auto res3 = co_await replicate_record_batch(leader, std::move(b3));
    ASSERT_TRUE_CORO(res3.has_value());

    // Verify epochs are established
    auto max_epoch = leader_api.get_max_epoch();
    auto max_seen_epoch = leader_api.get_max_seen_epoch();
    ASSERT_TRUE_CORO(max_epoch.has_value());
    ASSERT_TRUE_CORO(max_seen_epoch.has_value());
    ASSERT_EQ_CORO(max_epoch.value(), ct::cluster_epoch{3});
    ASSERT_EQ_CORO(max_seen_epoch.value(), ct::cluster_epoch{3});

    // Get the previous epoch (should be epoch 2, the previous
    // max_applied_epoch)
    auto stm = get_stm<0>(leader);
    auto previous_epoch = stm->state().get_previous_applied_epoch();
    ASSERT_TRUE_CORO(previous_epoch.has_value());
    ASSERT_EQ_CORO(previous_epoch.value(), ct::cluster_epoch{2});

    // Check inactive epoch before any LRO advance
    // Since we have epochs 1, 2, 3, epoch 0 is already inactive
    auto inactive_epoch_before = co_await leader_api.get_inactive_epoch();
    ASSERT_TRUE_CORO(inactive_epoch_before);
    ASSERT_TRUE_CORO(inactive_epoch_before->has_value());
    ASSERT_EQ_CORO(inactive_epoch_before->value(), ct::cluster_epoch{0});

    // Check the estimate as well
    auto estimated_inactive_before = leader_api.estimate_inactive_epoch();
    ASSERT_TRUE_CORO(estimated_inactive_before.has_value());
    ASSERT_EQ_CORO(estimated_inactive_before.value(), ct::cluster_epoch{0});

    // Fence epoch 4 to advance max_seen_epoch
    {
        auto fence_4 = co_await leader_api.fence_epoch(ct::cluster_epoch{4});
        ASSERT_TRUE_CORO(fence_4.has_value());

        // get_previous_epoch() returns the committed _previous_epoch, which
        // is still 2 because no batch with epoch 4 has been applied yet.
        // The transient _previous_seen_epoch is 3 (used by epoch_in_window).
        previous_epoch = stm->state().get_previous_applied_epoch();
        ASSERT_TRUE_CORO(previous_epoch.has_value());
        ASSERT_EQ_CORO(previous_epoch.value(), ct::cluster_epoch{2});
    }

    // Try to fence an out-of-order epoch (epoch 2) that is <
    // _previous_seen_epoch (3) and < max_seen_epoch (4) - should fail because
    // epoch_in_window checks against the transient _previous_seen_epoch
    {
        auto fence_prev = co_await leader_api.fence_epoch(ct::cluster_epoch{2});
        ASSERT_FALSE_CORO(fence_prev.has_value())
          << "Should not be able to fence an out-of-order epoch < "
             "_previous_seen_epoch";
    }

    // Advance LRO to the middle of epoch 1 (kafka offset 5)
    // This should make epochs before epoch 1 inactive, but not epoch 1 itself
    co_await leader_api.advance_reconciled_offset(
      kafka::offset{5}, model::no_timeout, as);

    // Check that inactive epoch is computed correctly
    // Even though LRO is in the middle of epoch 1, epoch 0 is still inactive
    // because the minimum epoch referenced is 1
    auto inactive_after_lro1 = co_await leader_api.get_inactive_epoch();
    ASSERT_TRUE_CORO(inactive_after_lro1);
    ASSERT_TRUE_CORO(inactive_after_lro1->has_value());
    ASSERT_EQ_CORO(inactive_after_lro1->value(), ct::cluster_epoch{0});

    // Check the estimate
    auto estimated_inactive_lro1 = leader_api.estimate_inactive_epoch();
    ASSERT_TRUE_CORO(estimated_inactive_lro1.has_value());
    ASSERT_EQ_CORO(estimated_inactive_lro1.value(), ct::cluster_epoch{0});

    // Advance LRO to the end of epoch 1 (kafka offset 9)
    co_await leader_api.advance_reconciled_offset(
      kafka::offset{9}, model::no_timeout, as);

    // Now epochs 0 and 1 are inactive (we've reconciled all of epoch 1)
    // The minimum epoch referenced is now 2
    auto inactive_after_lro2 = co_await leader_api.get_inactive_epoch();
    ASSERT_TRUE_CORO(inactive_after_lro2);
    ASSERT_TRUE_CORO(inactive_after_lro2->has_value());
    ASSERT_EQ_CORO(inactive_after_lro2->value(), ct::cluster_epoch{1});

    // Check the estimate (should be lower in this case)
    auto estimated_inactive_lro2 = leader_api.estimate_inactive_epoch();
    ASSERT_TRUE_CORO(estimated_inactive_lro2.has_value());
    ASSERT_EQ_CORO(estimated_inactive_lro2.value(), ct::cluster_epoch{0});

    // Get the previous epoch (lower bound of active epochs)
    // The previous epoch is still 2 (no new epochs have been applied)
    previous_epoch = stm->state().get_previous_applied_epoch();
    ASSERT_TRUE_CORO(previous_epoch.has_value());
    ASSERT_EQ_CORO(previous_epoch.value(), ct::cluster_epoch{2});

    // Advance LRO past epoch 2 (kafka offset 19)
    // This is before _max_applied_epoch_offset (20), so _min_epoch_lower_bound
    // won't be updated yet (it will remain stale)
    co_await leader_api.advance_reconciled_offset(
      kafka::offset{19}, model::no_timeout, as);

    // Check inactive epoch after advancing past epoch 2
    // The inactive epoch is computed by scanning the log, so it should be 2
    auto inactive_after_lro3 = co_await leader_api.get_inactive_epoch();
    ASSERT_TRUE_CORO(inactive_after_lro3);
    ASSERT_TRUE_CORO(inactive_after_lro3->has_value());
    ASSERT_EQ_CORO(inactive_after_lro3->value(), ct::cluster_epoch{2});

    // Check the estimate - it may be stale at this point since we're before
    // _max_applied_epoch_offset, but it should still be <= the precise value
    auto estimated_inactive_lro3 = leader_api.estimate_inactive_epoch();
    if (estimated_inactive_lro3.has_value()) {
        ASSERT_LE_CORO(estimated_inactive_lro3.value(), ct::cluster_epoch{2})
          << "Estimated inactive epoch should be <= precise value";
    }

    // The previous epoch is still 2 (no new epochs have been applied)
    previous_epoch = stm->state().get_previous_applied_epoch();
    ASSERT_TRUE_CORO(previous_epoch.has_value());
    ASSERT_EQ_CORO(previous_epoch.value(), ct::cluster_epoch{2});

    // Advance LRO past epoch 3 (kafka offset 29)
    // This advances past _max_applied_epoch_offset (20), so
    // _min_epoch_lower_bound should be updated to _max_applied_epoch (3)
    // The previous epoch remains 3 (no new epochs have been applied)
    co_await leader_api.advance_reconciled_offset(
      kafka::offset{29}, model::no_timeout, as);

    // Check inactive epoch after advancing past epoch 3
    // Since we've advanced LRO past all epochs, get_inactive_epoch() may return
    // nullopt (no epochs left in the log after LRO)
    auto inactive_after_lro4 = co_await leader_api.get_inactive_epoch();
    ASSERT_TRUE_CORO(inactive_after_lro4);

    // Previous epoch should still be 2 (unchanged, no new epochs applied)
    previous_epoch = stm->state().get_previous_applied_epoch();
    ASSERT_TRUE_CORO(previous_epoch.has_value());
    ASSERT_EQ_CORO(previous_epoch.value(), ct::cluster_epoch{2});

    // Check the estimate after advancing past _max_applied_epoch_offset
    // Now _min_epoch_lower_bound should be updated to _max_applied_epoch (3)
    // estimate_inactive_epoch returns min(_min_epoch_lower_bound - 1,
    // _previous_epoch - 1) = min(3 - 1, 2 - 1) = min(2, 1) = 1
    auto estimated_inactive_lro4 = leader_api.estimate_inactive_epoch();
    ASSERT_EQ_CORO(estimated_inactive_lro4.value(), ct::cluster_epoch{1});
}

TEST_F_CORO(
  ctp_stm_fixture, test_stale_in_memory_window_after_leadership_change) {
    // This test verifies a bug where a node that becomes leader again
    // can have a stale in-memory _max_seen_epoch window and incorrectly
    // accept stale epochs.
    //
    // Scenario:
    // 1. Node 0 is leader with in-memory window [11, 12]
    // 2. Leadership changes to Node 1
    // 3. Node 1 advances the window to [13, 15]
    // 4. Leadership changes back to Node 0
    // 5. Node 0 still has stale in-memory window [11, 12]
    // 6. Node 0 incorrectly accepts epoch 11 (which is now stale)

    co_await start();
    co_await wait_for_leader(raft::default_timeout());

    // Step 1: Node 0 becomes leader and advances the window
    auto initial_leader_id = get_leader().value();
    vlog(ct::cd_log.info, "Initial leader: {}", initial_leader_id);

    auto& node0 = node(initial_leader_id);

    // Fence and replicate batches to establish window [11, 12]
    for (int i = 0; i < 13; i++) {
        auto epoch = ct::cluster_epoch{i};
        bool success = co_await replicate_with_epoch(
          node0, epoch, model::offset{i}, i);
        ASSERT_TRUE_CORO(success);
    }

    // Verify Node 0's window
    auto max_seen_0 = api(node0).get_max_seen_epoch();
    ASSERT_TRUE_CORO(max_seen_0.has_value());
    vlog(
      ct::cd_log.info,
      "Node {} max_seen_epoch before failover: {}",
      initial_leader_id,
      max_seen_0.value());
    ASSERT_EQ_CORO(max_seen_0.value(), ct::cluster_epoch{12});

    // Step 2: Transfer leadership to a different node
    // Block the current leader from immediately becoming leader again
    node0.raft()->block_new_leadership();
    vlog(
      ct::cd_log.info,
      "Triggering leadership change from Node {}",
      initial_leader_id);
    co_await node0.raft()->step_down("test_induced_failover");

    // Wait for a different node to become leader
    co_await wait_for_leader(10s);
    auto new_leader_id = get_leader().value();
    ASSERT_NE_CORO(new_leader_id, initial_leader_id)
      << "New leader should be different";
    vlog(ct::cd_log.info, "New leader: {}", new_leader_id);

    auto& node1 = node(new_leader_id);

    // Step 3: On new leader, advance the window to [14, 15]
    for (int i = 13; i < 16; i++) {
        auto epoch = ct::cluster_epoch{i};
        bool success = co_await replicate_with_epoch(
          node1, epoch, model::offset{i}, i);
        ASSERT_TRUE_CORO(success);
    }

    auto max_seen_1 = api(node1).get_max_seen_epoch();
    ASSERT_TRUE_CORO(max_seen_1.has_value());
    vlog(
      ct::cd_log.info,
      "Node {} max_seen_epoch after advancement: {}",
      new_leader_id,
      max_seen_1.value());
    ASSERT_EQ_CORO(max_seen_1.value(), ct::cluster_epoch{15});

    // Step 4: Transfer leadership back to the original leader
    // This is where the bug manifests: Node 0 has stale in-memory window [11,
    // 12]
    node0.raft()->unblock_new_leadership();
    vlog(
      ct::cd_log.info,
      "Transferring leadership back to Node {}",
      initial_leader_id);
    co_await node1.raft()->transfer_leadership(
      raft::transfer_leadership_request{
        .group = node1.raft()->group(),
        .target = initial_leader_id,
        .timeout = 10s});

    co_await wait_for_leader(10s);
    auto final_leader_id = *get_leader();
    vlog(ct::cd_log.info, "Final leader: {}", final_leader_id);
    ASSERT_EQ_CORO(final_leader_id, initial_leader_id)
      << "Leadership should have transferred back to original leader";

    auto& final_leader = node(final_leader_id);

    // Step 5: Try to fence epoch 11 on the current leader
    // This epoch is now stale (window is [14, 15] or higher after new leader
    // replicated through epoch 15)
    // but if the bug exists and the leader is the original node with
    // stale in-memory state [11, 12], it might incorrectly accept it
    vlog(
      ct::cd_log.info,
      "Attempting to fence stale epoch 11 on Node {}",
      final_leader_id);

    auto stale_fence_result
      = co_await api(final_leader).fence_epoch(ct::cluster_epoch{11});

    // This should FAIL because epoch 11 is now stale
    // If the bug exists and final_leader == initial_leader_id, this will
    // incorrectly succeed because the leader has stale in-memory window [11,
    // 12]
    ASSERT_FALSE_CORO(stale_fence_result.has_value())
      << "Leader " << final_leader_id
      << " incorrectly accepted stale epoch 11. "
      << "This indicates the bug where in-memory _max_seen_epoch "
      << "is stale after regaining leadership.";
}
