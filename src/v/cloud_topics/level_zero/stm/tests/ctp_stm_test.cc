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
        ASSERT_TRUE_CORO(fence.unit.has_value());
    }

    // Acquire the fence for epoch 1 (should fail)
    {
        auto fence
          = co_await api(node(*get_leader())).fence_epoch(ct::cluster_epoch{1});
        ASSERT_FALSE_CORO(fence.unit.has_value());
    }

    // Advance max_seen_epoch to 3.
    auto write_fence
      = co_await api(node(*get_leader())).fence_epoch(ct::cluster_epoch{3});
    ASSERT_TRUE_CORO(write_fence.unit.has_value());

    // Out of order fence for epoch 2 (should be waiting for the fence to be
    // released)
    auto fut = api(node(*get_leader())).fence_epoch(ct::cluster_epoch{2});
    co_await ss::sleep(100ms);

    write_fence = {};

    auto read_fence = co_await std::move(fut);
    ASSERT_FALSE_CORO(read_fence.unit.has_value());
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

TEST(test_ewma, smoke_test) {
    cloud_topics::detail::simple_ewma ewma;

    ewma.update(10, 1);
    ASSERT_EQ(ewma.size_estimate(1), 0);

    for (int i = 0; i < 20; i++) {
        ewma.update(10, 1);
    }

    // The estimate is expected to be slightly
    // below 10 but the 'size_estimate()' rounds up.
    ASSERT_EQ(ewma.size_estimate(1), 10);
    ASSERT_LT(ewma.size_estimate(100), 1000);
    ASSERT_GT(ewma.size_estimate(100), 990);
}
