/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "absl/container/flat_hash_set.h"
#include "bytes/iobuf.h"
#include "cluster/logger.h"
#include "cluster/partition_properties_stm.h"
#include "cluster/tests/raft_fixture_retry_policy.h"
#include "config/mock_property.h"
#include "container/chunked_circular_buffer.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/tests/random_batch.h"
#include "raft/replicate.h"
#include "raft/tests/raft_fixture.h"
#include "random/generators.h"
#include "serde/rw/rw.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/util/file.hh>

#include <gtest/gtest.h>

#include <filesystem>
#include <ranges>
namespace cluster {
static ss::logger tstlog{"test-logger"};
struct partition_properties_stm_fixture : raft::raft_fixture {
    using stm_t = cluster::partition_properties_stm;
    ss::future<> initialize_state_machines() {
        create_nodes();
        return start_all_nodes();
    }

    ss::future<> start_all_nodes() {
        for (auto& [_, node] : nodes()) {
            co_await node->initialise(all_vnodes());
            raft::state_machine_manager_builder builder;

            auto stm = builder.create_stm<stm_t>(
              node->raft().get(),
              clusterlog,
              node->get_kvstore(),
              sync_timeout.bind());

            co_await node->start(std::move(builder));
            node_stms[node->get_vnode()] = std::move(stm);
        }
    }

    void create_nodes() {
        for (auto i = 0; i < 3; ++i) {
            add_node(model::node_id(i), model::revision_id(0));
        }
    }

    ss::future<> check_res(result<model::offset> res, bool expect_success) {
        ASSERT_TRUE_CORO(expect_success == res.has_value());
        if (!expect_success) {
            ASSERT_EQ_CORO(res.error(), errc::invalid_data_migration_state);
        }
    }

    ss::future<>
    disable_writes(model::revision_id revision_id, bool expect_success = true) {
        auto res = co_await retry_with_leader(
          raft::default_timeout(),
          2s,
          [revision_id](raft::raft_node_instance& leader_node) {
              return get_stm(leader_node)->disable_writes(revision_id);
          });
        co_await check_res(res, expect_success);
    }

    ss::future<>
    enable_writes(model::revision_id revision_id, bool expect_success = true) {
        auto res = co_await retry_with_leader(
          raft::default_timeout(),
          2s,
          [revision_id](raft::raft_node_instance& leader_node) {
              return get_stm(leader_node)->enable_writes(revision_id);
          });
        co_await check_res(res, expect_success);
    }

    static ss::shared_ptr<stm_t> get_stm(raft::raft_node_instance& rni) {
        return rni.raft()->stm_manager()->get<stm_t>();
    }

    ss::shared_ptr<stm_t> get_leader_stm() {
        auto leader_id = get_leader();
        EXPECT_TRUE(leader_id.has_value());

        auto& leader_node = node(leader_id.value());

        return get_stm(leader_node);
    }

    ss::future<result<stm_t::writes_disabled>> get_writes_disabled_on_leader() {
        return get_leader_stm()->sync_writes_disabled();
    }

    ss::future<> assert_writes(stm_t::writes_disabled disabled) {
        auto r = co_await get_writes_disabled_on_leader();
        ASSERT_TRUE_CORO(r.has_value());
        ASSERT_EQ_CORO(r.value(), disabled);
    }

    ss::future<> set_partition_mode(
      model::redpanda_storage_mode mode, bool expect_success = true) {
        auto res = co_await retry_with_leader(
          raft::default_timeout(),
          2s,
          [mode](raft::raft_node_instance& leader_node) {
              return get_stm(leader_node)->set_partition_mode(mode);
          });
        ASSERT_EQ_CORO(res.has_value(), expect_success);
    }

    ss::future<> assert_partition_mode(model::redpanda_storage_mode mode) {
        auto r = co_await get_leader_stm()->sync_partition_mode();
        ASSERT_TRUE_CORO(r.has_value());
        ASSERT_EQ_CORO(r.value(), mode);
    }

    ss::future<>
    check_partition_mode_consistent(model::redpanda_storage_mode mode) {
        RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this, mode] {
            return std::ranges::all_of(
              node_stms | std::views::values,
              [mode](auto stm) { return stm->partition_mode() == mode; });
        });
    }

    ss::future<result<model::offset>> replicate_random_batches() {
        return retry_with_leader(
          raft::default_timeout(),
          2s,
          [](raft::raft_node_instance& leader_node) {
              return model::test::make_random_batches(
                       model::test::record_batch_spec{
                         .count = 10,
                         .records = 50,
                       })
                .then([&](
                        chunked_circular_buffer<model::record_batch> batches) {
                    return leader_node.raft()
                      ->replicate(
                        chunked_vector<model::record_batch>(
                          std::from_range,
                          std::move(batches) | std::views::as_rvalue),
                        raft::replicate_options(
                          raft::consistency_level::quorum_ack))
                      .then([](result<raft::replicate_result> res) {
                          if (res.has_error()) {
                              return result<model::offset>(res.error());
                          }
                          return result<model::offset>(res.value().last_offset);
                      });
                });
          });
    }

    ss::future<bool> generate_random_data() {
        bool disabled = false;
        model::revision_id revision_id{1};
        for (int i = 0; i < 200; ++i) {
            auto o = co_await replicate_random_batches();
            vlog(tstlog.info, "last batches offset offset: {}", o);

            // 1/6 chance to create a legacy record
            if (random_generators::get_int(6) == 0) {
                revision_id = model::revision_id{};
            }

            if (random_generators::random_choice({true, false})) {
                co_await enable_writes(revision_id);
                disabled = false;
            } else {
                co_await disable_writes(revision_id);
                disabled = true;
            }

            if (revision_id == model::revision_id{}) {
                revision_id = model::revision_id{1};
            } else {
                ++revision_id;
            }
        }
        co_return disabled;
    }

    ss::future<> restart_nodes(
      absl::flat_hash_set<model::node_id> nodes_to_remove_data = {}) {
        // need to preserve data directory for the node to read the same data.
        absl::flat_hash_map<model::node_id, ss::sstring> data_directories;
        for (auto& [id, node] : nodes()) {
            data_directories[id]
              = node->raft()->log()->config().base_directory();
        }

        for (auto& [id, data_dir] : data_directories) {
            co_await stop_node(id);
            if (nodes_to_remove_data.contains(id)) {
                vlog(
                  tstlog.info,
                  "removing: {}",
                  std::filesystem::path(data_directories[id]));
                co_await ss::recursive_remove_directory(
                  std::filesystem::path(data_directories[id]));
            }
            add_node(id, model::revision_id(0), std::move(data_dir));
        }
        co_await start_all_nodes();
    }

    ss::future<>
    check_state_consistent(stm_t::writes_disabled writes_disabled) {
        RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this, writes_disabled] {
            return std::ranges::all_of(
              node_stms | std::views::values, [writes_disabled](auto stm) {
                  return stm->are_writes_disabled() == writes_disabled;
              });
        });
    }

    config::mock_property<std::chrono::milliseconds> sync_timeout{10s};
    absl::flat_hash_map<raft::vnode, ss::shared_ptr<stm_t>> node_stms;
};

struct partition_properties_stm_accessor {
    using snapshot_t = partition_properties_stm::raft_snapshot;
    static snapshot_t snap_from_iobuf(iobuf buffer) {
        return serde::from_iobuf<partition_properties_stm_accessor::snapshot_t>(
          std::move(buffer));
    }
};

TEST_F_CORO(partition_properties_stm_fixture, test_basic_operations) {
    co_await initialize_state_machines();
    // disable writes and validate outcome
    co_await disable_writes(model::revision_id{1});
    co_await assert_writes(stm_t::writes_disabled::yes);
    // check idempotent with the same revision id
    co_await disable_writes(model::revision_id{1});
    co_await assert_writes(stm_t::writes_disabled::yes);
    // check idempotent with a higher revision id
    co_await disable_writes(model::revision_id{2});
    co_await assert_writes(stm_t::writes_disabled::yes);
    // the same operation is disallowed with a lower revision id
    co_await disable_writes(model::revision_id{1}, false);
    co_await assert_writes(stm_t::writes_disabled::yes);
    // the opposite operation is disallowed with a lower revision id
    co_await enable_writes(model::revision_id{1}, false);
    co_await assert_writes(stm_t::writes_disabled::yes);
    // the opposite operation is disallowed with the same revision id
    co_await enable_writes(model::revision_id{2}, false);
    co_await assert_writes(stm_t::writes_disabled::yes);

    // enable writes back, and verify that the state is updated
    co_await enable_writes(model::revision_id{3});
    co_await assert_writes(stm_t::writes_disabled::no);
    // check idempotent with the same revision id
    co_await enable_writes(model::revision_id{3});
    co_await assert_writes(stm_t::writes_disabled::no);
    // check idempotent with a higher revision id
    co_await enable_writes(model::revision_id{4});
    co_await assert_writes(stm_t::writes_disabled::no);
    // the same operation is disallowed with a lower revision id
    co_await enable_writes(model::revision_id{3}, false);
    co_await assert_writes(stm_t::writes_disabled::no);
    // the opposite operation is disallowed with a lower revision id
    co_await disable_writes(model::revision_id{3}, false);
    co_await assert_writes(stm_t::writes_disabled::no);
    // the opposite operation is disallowed with the same revision id
    co_await disable_writes(model::revision_id{4}, false);
    co_await assert_writes(stm_t::writes_disabled::no);

    // empty revision id always works and resets the internal revision counter
    // state change with empty revision id
    co_await disable_writes(model::revision_id{});
    co_await assert_writes(stm_t::writes_disabled::yes);
    // idempotent with empty revision id
    co_await disable_writes(model::revision_id{});
    co_await assert_writes(stm_t::writes_disabled::yes);
    // a legacy command flips the state even when the current revision is also
    // empty, i.e. adjacent legacy commands are not treated as out-of-order
    co_await enable_writes(model::revision_id{});
    co_await assert_writes(stm_t::writes_disabled::no);
    co_await disable_writes(model::revision_id{});
    co_await assert_writes(stm_t::writes_disabled::yes);
    // after reset, even revision id 1 works
    co_await enable_writes(model::revision_id{1});
    co_await assert_writes(stm_t::writes_disabled::no);
    // state change with empty revision id for enable
    co_await disable_writes(model::revision_id{5});
    co_await assert_writes(stm_t::writes_disabled::yes);
    co_await enable_writes(model::revision_id{});
    co_await assert_writes(stm_t::writes_disabled::no);
    // idempotent with empty revision id for enable
    co_await enable_writes(model::revision_id{});
    co_await assert_writes(stm_t::writes_disabled::no);
    // after reset, even revision id 1 works for disable
    co_await disable_writes(model::revision_id{1});
    co_await assert_writes(stm_t::writes_disabled::yes);
}

TEST_F_CORO(partition_properties_stm_fixture, test_snapshot) {
    co_await initialize_state_machines();

    auto before_disabled = co_await replicate_random_batches();
    co_await disable_writes(model::revision_id{1});
    // it may be confusing that we replicate after the writes are disabled
    // however we only going to block writes of Kafka batches while metadata
    // batches will still be writable
    auto before_enabled = co_await replicate_random_batches();
    co_await enable_writes(model::revision_id{2});
    auto after_enabled = co_await replicate_random_batches();
    auto leader_id = get_leader();
    EXPECT_TRUE(leader_id.has_value());

    auto& leader_node = node(leader_id.value());
    // take snapshot from before the disable writes
    auto o = co_await leader_node.random_batch_base_offset(
      before_disabled.value());
    tstlog.info(
      "last offset before disabled: {}, before enabled: {}, after enabled: {}",
      before_disabled.value(),
      before_enabled.value(),
      after_enabled.value());

    auto snap_before_disabled
      = partition_properties_stm_accessor::snap_from_iobuf(
        co_await get_leader_stm()->take_raft_snapshot(o));
    ASSERT_EQ_CORO(
      snap_before_disabled.writes_disabled, stm_t::writes_disabled::no);
    ASSERT_EQ_CORO(
      snap_before_disabled.writes_revision_id, model::revision_id{});
    // take snapshot at disable command offset
    auto snap_at_disabled = partition_properties_stm_accessor::snap_from_iobuf(
      co_await get_leader_stm()->take_raft_snapshot(
        model::next_offset(before_disabled.value())));
    ASSERT_EQ_CORO(
      snap_at_disabled.writes_disabled, stm_t::writes_disabled::yes);
    ASSERT_EQ_CORO(snap_at_disabled.writes_revision_id, model::revision_id{1});
    // take snapshot after disable command but before enable
    o = co_await leader_node.random_batch_base_offset(
      before_enabled.value(), before_disabled.value() + model::offset(2));
    auto snap_before_enabled
      = partition_properties_stm_accessor::snap_from_iobuf(
        co_await get_leader_stm()->take_raft_snapshot(o));
    ASSERT_EQ_CORO(
      snap_before_enabled.writes_disabled, stm_t::writes_disabled::yes);
    ASSERT_EQ_CORO(
      snap_before_enabled.writes_revision_id, model::revision_id{1});
    auto snap_at_enabled = partition_properties_stm_accessor::snap_from_iobuf(
      co_await get_leader_stm()->take_raft_snapshot(
        model::next_offset(before_enabled.value())));
    ASSERT_EQ_CORO(snap_at_enabled.writes_disabled, stm_t::writes_disabled::no);
    ASSERT_EQ_CORO(snap_at_enabled.writes_revision_id, model::revision_id{2});

    o = co_await leader_node.random_batch_base_offset(
      leader_node.raft()->dirty_offset(),
      before_enabled.value() + model::offset(2));
    auto snap_after_enabled
      = partition_properties_stm_accessor::snap_from_iobuf(
        co_await get_leader_stm()->take_raft_snapshot(
          model::next_offset(before_enabled.value())));
    ASSERT_EQ_CORO(
      snap_after_enabled.writes_disabled, stm_t::writes_disabled::no);
    ASSERT_EQ_CORO(
      snap_after_enabled.writes_revision_id, model::revision_id{2});
}

TEST_F_CORO(
  partition_properties_stm_fixture, test_recovery_from_local_snapshot) {
    co_await initialize_state_machines();
    stm_t::writes_disabled should_be_disabled{co_await generate_random_data()};
    auto writes_disabled = (co_await get_writes_disabled_on_leader()).value();
    ASSERT_EQ_CORO(writes_disabled, should_be_disabled);

    auto last_applied = get_leader_stm()->last_applied_offset();
    co_await restart_nodes();
    // test recovery
    co_await wait_for_leader(10s);
    writes_disabled = (co_await get_writes_disabled_on_leader()).value();
    ASSERT_EQ_CORO(writes_disabled, should_be_disabled);
    co_await wait_for_committed_offset(last_applied, 10s);
    co_await check_state_consistent(should_be_disabled);

    // drop one node data folder
    co_await restart_nodes({model::node_id(1)});
    // test recovery
    co_await wait_for_leader(10s);
    writes_disabled = (co_await get_writes_disabled_on_leader()).value();
    ASSERT_EQ_CORO(writes_disabled, should_be_disabled);
    co_await wait_for_committed_offset(last_applied, 10s);
    co_await check_state_consistent(should_be_disabled);
    // now take snapshot on every node and recover state
    for (auto& [_, node] : nodes()) {
        auto base_offset = co_await node->random_batch_base_offset(
          node->raft()->committed_offset(), model::offset(100));
        auto snapshot_offset = model::prev_offset(base_offset);
        auto result = co_await node->raft()->stm_manager()->take_snapshot(
          snapshot_offset);
        co_await node->raft()->write_snapshot(
          raft::write_snapshot_cfg(snapshot_offset, std::move(result.data)));
    }

    // test follower recovery with snapshot
    co_await restart_nodes({random_generators::random_choice(
      {model::node_id(0), model::node_id(1), model::node_id(2)})});
    co_await wait_for_leader(10s);
    writes_disabled = (co_await get_writes_disabled_on_leader()).value();
    ASSERT_EQ_CORO(writes_disabled, should_be_disabled);
    co_await wait_for_committed_offset(last_applied, 10s);
    co_await check_state_consistent(should_be_disabled);

    // test recovery with local snapshot

    for (auto& [_, node] : nodes()) {
        auto base_offset = co_await node->random_batch_base_offset(
          node->raft()->committed_offset());
        auto snapshot_offset = model::prev_offset(base_offset);

        co_await get_stm(*node)->ensure_local_snapshot_exists(snapshot_offset);
    }
    co_await restart_nodes({random_generators::random_choice(
      {model::node_id(0), model::node_id(1), model::node_id(2)})});
    co_await wait_for_leader(10s);
    writes_disabled = (co_await get_writes_disabled_on_leader()).value();
    ASSERT_EQ_CORO(writes_disabled, should_be_disabled);
    co_await wait_for_committed_offset(last_applied, 10s);
    co_await check_state_consistent(should_be_disabled);
}

TEST_F_CORO(partition_properties_stm_fixture, test_partition_mode_basic) {
    co_await initialize_state_machines();
    co_await wait_for_leader(10s);
    // A fresh partition has never had partition_mode written: it reads `unset`
    // (the "not bootstrapped -> fall back to topic_mode" sentinel).
    co_await assert_partition_mode(model::redpanda_storage_mode::unset);

    co_await set_partition_mode(model::redpanda_storage_mode::tiered);
    co_await assert_partition_mode(model::redpanda_storage_mode::tiered);
    // idempotent
    co_await set_partition_mode(model::redpanda_storage_mode::tiered);
    co_await assert_partition_mode(model::redpanda_storage_mode::tiered);
    // the migration cutover transition: tiered -> cloud
    co_await set_partition_mode(model::redpanda_storage_mode::cloud);
    co_await assert_partition_mode(model::redpanda_storage_mode::cloud);
    co_await check_partition_mode_consistent(
      model::redpanda_storage_mode::cloud);
}

// writes_disabled and partition_mode share one state_snapshot; updating one
// must preserve the other (the apply copy-modify).
TEST_F_CORO(
  partition_properties_stm_fixture, test_partition_mode_independent_of_writes) {
    co_await initialize_state_machines();

    co_await disable_writes(model::revision_id{1});
    co_await set_partition_mode(model::redpanda_storage_mode::tiered);
    co_await assert_writes(stm_t::writes_disabled::yes);
    co_await assert_partition_mode(model::redpanda_storage_mode::tiered);

    // a partition_mode update leaves writes_disabled untouched
    co_await set_partition_mode(model::redpanda_storage_mode::cloud);
    co_await assert_writes(stm_t::writes_disabled::yes);
    co_await assert_partition_mode(model::redpanda_storage_mode::cloud);

    // and a writes_disabled update leaves partition_mode untouched
    co_await enable_writes(model::revision_id{2});
    co_await assert_writes(stm_t::writes_disabled::no);
    co_await assert_partition_mode(model::redpanda_storage_mode::cloud);
}

TEST_F_CORO(partition_properties_stm_fixture, test_partition_mode_snapshot) {
    co_await initialize_state_machines();

    auto before_set = co_await replicate_random_batches();
    co_await set_partition_mode(model::redpanda_storage_mode::tiered);
    [[maybe_unused]] auto _ignored = co_await replicate_random_batches();

    auto leader_id = get_leader();
    EXPECT_TRUE(leader_id.has_value());
    auto& leader_node = node(leader_id.value());

    // snapshot taken before the set command -> unset
    auto o = co_await leader_node.random_batch_base_offset(before_set.value());
    auto snap_before = partition_properties_stm_accessor::snap_from_iobuf(
      co_await get_leader_stm()->take_raft_snapshot(o));
    ASSERT_EQ_CORO(
      snap_before.partition_mode, model::redpanda_storage_mode::unset);

    // snapshot at the set command offset -> tiered (and writes_disabled intact)
    auto snap_at = partition_properties_stm_accessor::snap_from_iobuf(
      co_await get_leader_stm()->take_raft_snapshot(
        model::next_offset(before_set.value())));
    ASSERT_EQ_CORO(
      snap_at.partition_mode, model::redpanda_storage_mode::tiered);
    ASSERT_EQ_CORO(snap_at.writes_disabled, stm_t::writes_disabled::no);
}

TEST_F_CORO(partition_properties_stm_fixture, test_partition_mode_recovery) {
    co_await initialize_state_machines();
    co_await set_partition_mode(model::redpanda_storage_mode::tiered);
    [[maybe_unused]] auto _ignored = co_await replicate_random_batches();
    co_await set_partition_mode(model::redpanda_storage_mode::cloud);
    co_await assert_partition_mode(model::redpanda_storage_mode::cloud);
    auto last_applied = get_leader_stm()->last_applied_offset();

    // restart preserving data: recover from local (kvstore) snapshot + replay
    co_await restart_nodes();
    co_await wait_for_leader(10s);
    co_await assert_partition_mode(model::redpanda_storage_mode::cloud);
    co_await wait_for_committed_offset(last_applied, 10s);
    co_await check_partition_mode_consistent(
      model::redpanda_storage_mode::cloud);

    // take a raft snapshot on every node, then restart with one node's data
    // dropped: that node recovers partition_mode from the raft snapshot.
    for (auto& [_, node] : nodes()) {
        auto base_offset = co_await node->random_batch_base_offset(
          node->raft()->committed_offset(), model::offset(1));
        auto snapshot_offset = model::prev_offset(base_offset);
        auto result = co_await node->raft()->stm_manager()->take_snapshot(
          snapshot_offset);
        co_await node->raft()->write_snapshot(
          raft::write_snapshot_cfg(snapshot_offset, std::move(result.data)));
    }
    co_await restart_nodes({model::node_id(1)});
    co_await wait_for_leader(10s);
    co_await assert_partition_mode(model::redpanda_storage_mode::cloud);
    co_await wait_for_committed_offset(last_applied, 10s);
    co_await check_partition_mode_consistent(
      model::redpanda_storage_mode::cloud);
}

// The change callback (used by partition to push partition_mode into
// ntp_config) must fire when partition_mode changes, on each replica that
// applies the change.
TEST_F_CORO(
  partition_properties_stm_fixture, test_partition_mode_change_callback) {
    co_await initialize_state_machines();
    co_await wait_for_leader(10s);

    auto fired = ss::make_lw_shared<int>(0);
    for (auto& [_, stm] : node_stms) {
        stm->set_partition_mode_change_callback([fired] { ++(*fired); });
    }

    co_await set_partition_mode(model::redpanda_storage_mode::tiered);
    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [fired] { return *fired >= 1; });
    co_await check_partition_mode_consistent(
      model::redpanda_storage_mode::tiered);

    // an idempotent (no-op) update does not change state, so it does not fire
    auto before = *fired;
    co_await set_partition_mode(model::redpanda_storage_mode::tiered);
    co_await check_partition_mode_consistent(
      model::redpanda_storage_mode::tiered);
    ASSERT_EQ_CORO(*fired, before);
}

} // namespace cluster
