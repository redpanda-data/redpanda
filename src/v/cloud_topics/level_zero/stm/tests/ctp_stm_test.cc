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
#include "cloud_topics/level_zero/stm/ctp_stm_factory.h"
#include "cloud_topics/level_zero/stm/placeholder.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/types.h"
#include "cluster/state_machine_registry.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "raft/tests/raft_fixture.h"
#include "test_utils/test.h"

#include <optional>

namespace ct = cloud_topics;
using namespace std::chrono_literals;

struct ctp_stm_api_accessor {
    ss::future<std::expected<model::offset, ct::ctp_stm_api_errc>>
    replicated_apply(model::record_batch rb) {
        // This function is used to access the private method of ctp_stm_api
        return api->replicated_apply(std::move(rb));
    }
    ct::ctp_stm_api* api;
};

class ctp_stm_fixture : public raft::raft_fixture {
public:
    static constexpr auto node_count = 3;

    ctp_stm_fixture()
      : rtc(as) {}

    ss::future<> start() {
        enable_offset_translation();
        for (auto i = 0; i < node_count; ++i) {
            add_node(model::node_id(i), model::revision_id(0));
        }

        for (auto& [id, node] : nodes()) {
            co_await node->initialise(all_vnodes());

            raft::state_machine_manager_builder builder;

            cloud_topics::l0::ctp_stm_factory stm_factory;
            stm_factory.create(
              builder, &*node->raft(), cluster::stm_instance_config{nullptr});

            vlog(ct::cd_log.info, "Starting node {}", id);

            co_await node->start(std::move(builder));

            stm_by_vnode[node->get_vnode()]
              = node->raft()->stm_manager()->get<ct::ctp_stm>();

            api_by_vnode.emplace(
              node->get_vnode(),
              ss::make_shared<ct::ctp_stm_api>(
                rtc, node->raft()->stm_manager()->get<ct::ctp_stm>()));
        }
    }

    ct::ctp_stm_api& api(raft::raft_node_instance& node) {
        return *api_by_vnode[node.get_vnode()];
    }

    model::record_batch make_record_batch(
      ct::cluster_epoch e,
      model::offset base_offset,
      int32_t seq,
      std::optional<int> size = std::nullopt) {
        ct::object_id id = ct::object_id::create(e);
        ct::dl_placeholder placeholder{
          .id = id,
          .offset = ct::first_byte_offset_t{0},
          .size_bytes = ct::byte_range_size_t{0},
        };

        storage::record_batch_builder builder(
          model::record_batch_type::dl_placeholder, base_offset);

        auto first_key = serde::to_iobuf(
          cloud_topics::dl_placeholder_record_key::payload);

        auto first_value = serde::to_iobuf(placeholder);

        builder.add_raw_kv(std::move(first_key), std::move(first_value));
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
        return ctp_stm_api_accessor{.api = &api(node)}.replicated_apply(
          std::move(rb));
    }

    ss::abort_source as;
    retry_chain_node rtc;
    absl::flat_hash_map<raft::vnode, ss::shared_ptr<ct::ctp_stm>> stm_by_vnode;
    absl::flat_hash_map<raft::vnode, ss::shared_ptr<ct::ctp_stm_api>>
      api_by_vnode;
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
      .advance_reconciled_offset(kafka::offset{0});

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
      .advance_reconciled_offset(kafka::offset{1});

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
      .advance_reconciled_offset(kafka::offset(50));
    ss::abort_source as;
    co_await api(node(*get_leader())).sync_in_term(as);
    inactive_epoch = co_await api(node(*get_leader())).get_inactive_epoch();
    ASSERT_TRUE_CORO(inactive_epoch);
    ASSERT_TRUE_CORO(inactive_epoch->has_value());
    ASSERT_EQ_CORO(inactive_epoch->value(), ct::cluster_epoch{24});

    // Advance reconciled offset exactly to the end of the first epoch
    co_await api(node(*get_leader()))
      .advance_reconciled_offset(kafka::offset(99));
    inactive_epoch = co_await api(node(*get_leader())).get_inactive_epoch();
    max_epoch = api(node(*get_leader())).get_max_epoch();
    ASSERT_TRUE_CORO(inactive_epoch);
    ASSERT_TRUE_CORO(max_epoch);
    ASSERT_FALSE_CORO(inactive_epoch->has_value());
    ASSERT_EQ_CORO(max_epoch.value(), last_epoch);
}
