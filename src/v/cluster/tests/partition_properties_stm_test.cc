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
#include "bytes/iobuf.h"
#include "cluster/logger.h"
#include "cluster/partition_properties_stm.h"
#include "cluster/tests/raft_fixture_retry_policy.h"
#include "config/mock_property.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "raft/replicate.h"
#include "raft/tests/raft_fixture.h"
#include "random/generators.h"
#include "serde/rw/rw.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/file.hh>

#include <absl/container/flat_hash_set.h>
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

    ss::future<> disable_writes() {
        co_await retry_with_leader(
          raft::default_timeout(),
          2s,
          [](raft::raft_node_instance& leader_node) {
              return get_stm(leader_node)->disable_writes();
          });
    }

    ss::future<> enable_writes() {
        co_await retry_with_leader(
          raft::default_timeout(),
          2s,
          [](raft::raft_node_instance& leader_node) {
              return get_stm(leader_node)->enable_writes();
          });
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
                .then([&](ss::circular_buffer<model::record_batch> batches) {
                    return leader_node.raft()
                      ->replicate(
                        model::make_memory_record_batch_reader(
                          std::move(batches)),
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
        for (int i = 0; i < 200; ++i) {
            auto o = co_await replicate_random_batches();
            vlog(tstlog.info, "last batches offset offset: {}", o);
            if (random_generators::random_choice({true, false})) {
                co_await enable_writes();
                disabled = false;
            } else {
                co_await disable_writes();
                disabled = true;
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
    co_await disable_writes();
    co_await assert_writes(stm_t::writes_disabled::yes);
    // disable writes operation is idempotent
    co_await disable_writes();
    co_await assert_writes(stm_t::writes_disabled::yes);

    // enable writes back, and verify that the state is updated
    co_await enable_writes();
    co_await assert_writes(stm_t::writes_disabled::no);
    // check idempotent
    co_await enable_writes();
    co_await assert_writes(stm_t::writes_disabled::no);
}

TEST_F_CORO(partition_properties_stm_fixture, test_snapshot) {
    co_await initialize_state_machines();

    auto before_disabled = co_await replicate_random_batches();
    co_await disable_writes();
    // it may be confusing that we replicate after the writes are disabled
    // however we only going to block writes of Kafka batches while metadata
    // batches will still be writable
    auto before_enabled = co_await replicate_random_batches();
    co_await enable_writes();
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
        co_await get_leader_stm()->take_snapshot(o));
    ASSERT_EQ_CORO(
      snap_before_disabled.writes_disabled, stm_t::writes_disabled::no);
    // take snapshot at disable command offset
    auto snap_at_disabled = partition_properties_stm_accessor::snap_from_iobuf(
      co_await get_leader_stm()->take_snapshot(
        model::next_offset(before_disabled.value())));
    ASSERT_EQ_CORO(
      snap_at_disabled.writes_disabled, stm_t::writes_disabled::yes);

    // take snapshot after disable command but before enable
    o = co_await leader_node.random_batch_base_offset(
      before_enabled.value(), before_disabled.value() + model::offset(2));
    auto snap_before_enabled
      = partition_properties_stm_accessor::snap_from_iobuf(
        co_await get_leader_stm()->take_snapshot(o));
    ASSERT_EQ_CORO(
      snap_before_enabled.writes_disabled, stm_t::writes_disabled::yes);

    auto snap_at_enabled = partition_properties_stm_accessor::snap_from_iobuf(
      co_await get_leader_stm()->take_snapshot(
        model::next_offset(before_enabled.value())));
    ASSERT_EQ_CORO(snap_at_enabled.writes_disabled, stm_t::writes_disabled::no);

    o = co_await leader_node.random_batch_base_offset(
      leader_node.raft()->dirty_offset(),
      before_enabled.value() + model::offset(2));
    auto snap_after_enabled
      = partition_properties_stm_accessor::snap_from_iobuf(
        co_await get_leader_stm()->take_snapshot(
          model::next_offset(before_enabled.value())));
    ASSERT_EQ_CORO(
      snap_after_enabled.writes_disabled, stm_t::writes_disabled::no);
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

} // namespace cluster
