/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/data_migrated_resources.h"
#include "cluster/topic_table.h"
#include "datalake/catalog_schema_manager.h"
#include "datalake/coordinator/coordinator.h"
#include "datalake/coordinator/state_machine.h"
#include "datalake/coordinator/state_update.h"
#include "datalake/coordinator/tests/state_test_utils.h"
#include "datalake/record_schema_resolver.h"
#include "model/record_batch_types.h"
#include "raft/tests/stm_test_fixture.h"
#include "serde/rw/rw.h"
#include "storage/record_batch_builder.h"
#include "test_utils/container_ostream.h" // IWYU pragma: keep

using coordinator = std::unique_ptr<datalake::coordinator::coordinator>;
using stm = datalake::coordinator::coordinator_stm;
using stm_ptr = ss::shared_ptr<stm>;

struct coordinator_stm_fixture : stm_raft_fixture<stm> {
    ss::future<> TearDownAsync() override {
        for (auto& [_, coordinator] : coordinators) {
            co_await coordinator->stop_and_wait();
        }
        co_return co_await stm_raft_fixture<stm>::TearDownAsync();
    }

    config::binding<std::chrono::milliseconds> commit_interval() const {
        return config::mock_binding(1ms);
    }

    config::binding<std::chrono::seconds> snapshot_interval() const {
        return config::mock_binding(1s);
    }

    config::binding<ss::sstring> default_partition_spec() const {
        return config::mock_binding<ss::sstring>("(hour(redpanda.timestamp))");
    }

    config::binding<bool> disable_snapshot_expiry() const {
        return config::mock_binding<bool>(false);
    }

    config::binding<size_t> max_pending_files() const {
        return config::mock_binding<size_t>(100000);
    }

    stm_shptrs_t create_stms(
      state_machine_manager_builder& builder,
      raft_node_instance& node) override {
        return builder.create_stm<stm>(
          logger(), node.raft().get(), snapshot_interval());
    }

    ss::future<> initialize() {
        co_await initialize_state_machines();
        co_await parallel_for_each_node([this](raft_node_instance& node) {
            auto stm = get_stm<0>(node);
            coordinators[node.get_vnode()]
              = std::make_unique<datalake::coordinator::coordinator>(
                get_stm<0>(node),
                topic_table,
                type_resolver,
                schema_mgr,
                [this](const model::topic& t, model::revision_id r) {
                    return remove_tombstone(t, r);
                },
                file_committer,
                snapshot_remover,
                commit_interval(),
                default_partition_spec(),
                disable_snapshot_expiry(),
                max_pending_files());
            coordinators[node.get_vnode()]->start();
            return ss::now();
        });
    }

    model::offset last_snapshot_offset() {
        model::offset result = model::offset::max();
        for (auto& [_, stms] : node_stms) {
            auto stm_snapshot_index
              = std::get<0>(stms)->raft()->last_snapshot_index();
            result = std::min(result, stm_snapshot_index);
        }
        return result;
    }

    template<class Func>
    auto retry_with_leader_coordinator(Func&& func) {
        return retry_with_leader(
          model::timeout_clock::now() + 5s,
          [this, func = std::forward<Func>(func)](
            raft_node_instance& leader) mutable {
              return func(
                coordinators[leader.get_vnode()], leader.get_vnode().id());
          });
    }

    std::vector<model::offset> last_applied_offsets() const {
        std::vector<model::offset> result;
        result.reserve(node_stms.size());
        for (auto& [_, stms] : node_stms) {
            result.push_back(std::get<0>(stms)->last_applied_offset());
        }
        return result;
    }

    /**
     * Returns the last committed offset for a given topic partition
     * from each coordinator replica (stm).
     */
    std::vector<std::optional<kafka::offset>>
    last_committed_offsets(model::topic_partition tp) {
        std::vector<std::optional<kafka::offset>> result;
        result.reserve(node_stms.size());
        for (auto& [_, stms] : node_stms) {
            const auto& topic_state = std::get<0>(stms)->state();
            auto it = topic_state.topic_to_state.find(tp.topic);
            if (it == topic_state.topic_to_state.end()) {
                result.emplace_back(std::nullopt);
                continue;
            }
            const auto& p_state = it->second.pid_to_pending_files;
            auto p_it = p_state.find(tp.partition);
            if (p_it == p_state.end()) {
                result.emplace_back(std::nullopt);
                continue;
            }
            auto& pending_files = p_it->second.pending_entries;
            if (!pending_files.empty()) {
                result.emplace_back(pending_files.back().data.last_offset);
            } else {
                result.push_back(p_it->second.last_committed);
            }
        }
        return result;
    }

    /**
     * Returns the  kafka bytes processed for a given topic
     * from each coordinator replica (stm).
     */
    std::vector<std::optional<uint64_t>>
    total_bytes_processed(model::topic topic) {
        std::vector<std::optional<uint64_t>> result;
        result.reserve(node_stms.size());
        for (auto& [_, stms] : node_stms) {
            const auto& topic_state = std::get<0>(stms)->state();
            auto it = topic_state.topic_to_state.find(topic);
            if (it == topic_state.topic_to_state.end()) {
                result.emplace_back(std::nullopt);
                continue;
            }
            result.emplace_back(it->second.total_kafka_bytes_processed);
        }
        return result;
    }

    model::topic_partition random_tp() const {
        return {
          tp_ns.tp,
          model::partition_id(
            random_generators::get_int<int32_t>(0, max_partitions - 1))};
    }

    // Replicates a lifecycle transition registering `tp`'s topic as live,
    // directly through the STM (no coordinator commit loop involved).
    ss::future<bool> register_topic(stm_ptr stm) {
        auto term = co_await stm->sync(5s);
        if (term.has_error()) {
            co_return false;
        }
        datalake::coordinator::topic_lifecycle_update upd{
          .topic = tp_ns.tp,
          .revision = rev,
          .new_state
          = datalake::coordinator::topic_state::lifecycle_state_t::live};
        storage::record_batch_builder builder(
          model::record_batch_type::datalake_coordinator, model::offset{0});
        builder.add_raw_kv(
          serde::to_iobuf(datalake::coordinator::topic_lifecycle_update::key),
          serde::to_iobuf(std::move(upd)));
        ss::abort_source as;
        auto res = co_await stm->replicate_and_wait(
          term.value(), std::move(builder).build(), as);
        co_return !res.has_error();
    }

    // Replicates a batch of pending files (with data files, so their estimated
    // memory footprint is non-zero) contiguous with `tp`'s current pending
    // state, directly through the STM. Files are never committed, so the
    // pending totals only grow.
    ss::future<bool> add_next_files(stm_ptr stm) {
        auto term = co_await stm->sync(5s);
        if (term.has_error()) {
            co_return false;
        }
        int64_t start = 0;
        const auto& st = stm->state();
        auto it = st.topic_to_state.find(tp_ns.tp);
        if (it != st.topic_to_state.end()) {
            auto pit = it->second.pid_to_pending_files.find(tp.partition);
            if (pit != it->second.pid_to_pending_files.end()) {
                const auto& entries = pit->second.pending_entries;
                if (!entries.empty()) {
                    start = entries.back().data.last_offset() + 1;
                } else if (pit->second.last_committed.has_value()) {
                    start = pit->second.last_committed.value()() + 1;
                }
            }
        }
        std::vector<std::pair<int64_t, int64_t>> bounds;
        bounds.reserve(5);
        for (int i = 0; i < 5; ++i) {
            bounds.emplace_back(start, start + 5);
            start += 6;
        }
        auto build_res = datalake::coordinator::add_files_update::build(
          st,
          tp,
          rev,
          datalake::coordinator::make_pending_files(
            bounds, /*with_file=*/true));
        if (build_res.has_error()) {
            co_return false;
        }
        storage::record_batch_builder builder(
          model::record_batch_type::datalake_coordinator, model::offset{0});
        builder.add_raw_kv(
          serde::to_iobuf(datalake::coordinator::add_files_update::key),
          serde::to_iobuf(std::move(build_res.value())));
        ss::abort_source as;
        auto res = co_await stm->replicate_and_wait(
          term.value(), std::move(builder).build(), as);
        co_return !res.has_error();
    }

    ss::future<> register_in_topic_table() {
        auto topic_cfg = cluster::topic_configuration(
          tp_ns.ns, tp_ns.tp, /*partition_count=*/1, /*replication_factor=*/1);
        topic_cfg.properties.iceberg_mode = model::iceberg_mode::key_value;
        auto tt_res = co_await topic_table.apply(
          cluster::create_topic_cmd{tp_ns, {topic_cfg, {}}},
          model::offset{rev()});
        ASSERT_EQ_CORO(tt_res, cluster::errc::success);
    }

    ss::future<
      checked<std::nullopt_t, datalake::coordinator::coordinator::errc>>
    remove_tombstone(const model::topic&, model::revision_id) {
        co_return std::nullopt;
    }

    static constexpr int32_t max_partitions = 5;
    model::topic_namespace tp_ns{model::kafka_namespace, model::topic{"test"}};
    model::topic_partition tp{tp_ns.tp, model::partition_id{0}};
    model::revision_id rev{123};
    cluster::data_migrations::migrated_resources mr;
    cluster::topic_table topic_table{mr, model::node_id{0}};
    datalake::binary_type_resolver type_resolver;
    datalake::simple_schema_manager schema_mgr;
    datalake::coordinator::simple_file_committer file_committer;
    datalake::coordinator::noop_snapshot_remover snapshot_remover;
    absl::flat_hash_map<raft::vnode, coordinator> coordinators;
};

TEST_F_CORO(coordinator_stm_fixture, test_snapshots) {
    co_await initialize();
    co_await wait_for_leader(5s);
    co_await register_in_topic_table();

    // populate some data until the state machine is snapshotted
    // a few times
    constexpr auto max_snapshots = 5;
    auto completed_snapshots = 0;
    auto prev_snapshot_offset = last_snapshot_offset();
    while (completed_snapshots != max_snapshots) {
        // mock a translator
        auto add_files_result = co_await retry_with_leader_coordinator(
          [&, this](coordinator& coordinator, model::node_id node_id) mutable {
              auto tp = random_tp();
              // manually notify leadership to ensure coordinator processing
              // loop starts. Here the test runs without a coordinator manager
              // that is supposed to notify leadership.
              coordinator->notify_leadership(node_id);
              return coordinator->sync_get_last_added_offsets(tp, rev).then(
                [&, tp](auto result) {
                    if (!result) {
                        return ss::make_ready_future<bool>(false);
                    }
                    auto last_committed_offset = kafka::offset_cast(
                      result.value().last_added_offset.value_or(
                        kafka::offset{-1}));
                    std::vector<std::pair<int64_t, int64_t>> offset_pairs;
                    offset_pairs.reserve(5);
                    auto next_offset = last_committed_offset() + 1;
                    for (int i = 0; i < 5; i++) {
                        offset_pairs.emplace_back(next_offset, next_offset + 5);
                        next_offset = next_offset + 6;
                    }
                    return coordinator
                      ->sync_ensure_table_exists(
                        tp.topic, rev, datalake::record_schema_components{})
                      .then([this, tp, offset_pairs, &coordinator](
                              auto ensure_res) {
                          if (!ensure_res) {
                              return ss::make_ready_future<bool>(false);
                          }

                          return coordinator
                            ->sync_add_files(
                              tp,
                              rev,
                              datalake::coordinator::make_pending_files(
                                offset_pairs))
                            .then([](auto result) {
                                return ss::make_ready_future<bool>(
                                  result.has_value());
                            });
                      });
                });
          });
        ASSERT_TRUE_CORO(add_files_result) << "Timed out waiting to add files";
        auto snapshot_offset = last_snapshot_offset();
        vlog(logger().debug, "last snapshot offset: {}", snapshot_offset);
        if (snapshot_offset > prev_snapshot_offset) {
            completed_snapshots++;
        }
        prev_snapshot_offset = snapshot_offset;
    }

    // Add a new raft group to hydrate from snapshot.
    auto new_node_id = model::node_id{static_cast<int32_t>(node_stms.size())};
    auto& node = add_node(new_node_id, model::revision_id{10});
    co_await start_node(node);
    co_await with_leader(
      10s, [vn = node.get_vnode()](raft_node_instance& node) {
          return node.raft()->add_group_member(vn, model::revision_id{10});
      });
    // Wait until all group members converge and there are no further updates.
    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this]() {
        auto offsets = last_applied_offsets();
        return ss::make_ready_future<bool>(
          std::equal(offsets.begin() + 1, offsets.end(), offsets.begin()));
    });

    ASSERT_GT_CORO(node.raft()->start_offset(), model::offset{0})
      << "New node not seeded with snapshot";

    for (int32_t pid = 0; pid < max_partitions; pid++) {
        auto committed_offsets = last_committed_offsets(
          {tp_ns.tp, model::partition_id{pid}});
        vlog(logger().info, "committed offsets: {}", committed_offsets);
        ASSERT_TRUE_CORO(
          std::equal(
            committed_offsets.begin() + 1,
            committed_offsets.end(),
            committed_offsets.begin()))
          << "Topic state mismatch across replicas for partition: " << pid
          << ", offsets: " << committed_offsets;
    }

    auto bytes_processed = total_bytes_processed(tp_ns.tp);
    vlog(logger().info, "bytes processed: {}", bytes_processed);
    ASSERT_TRUE_CORO(
      std::equal(
        bytes_processed.begin() + 1,
        bytes_processed.end(),
        bytes_processed.begin()))
      << "Topic state mismatch across replicas for partition: " << tp_ns.tp
      << ", bytes processed: " << bytes_processed;
}

// The pending file and byte totals are derived state left out of the
// serialized snapshot, so they must be rebuilt when a node is seeded from a
// snapshot. Adds files (never committed, so the totals stay non-zero), forces
// enough snapshots to truncate the log, then brings up a fresh node that can
// only catch up from the snapshot and checks its totals match a recompute.
TEST_F_CORO(coordinator_stm_fixture, test_snapshot_recomputes_pending) {
    co_await initialize_state_machines();
    co_await wait_for_leader(5s);

    auto registered = co_await stm_retry_with_leader<0>(
      5s, [this](stm_ptr stm) { return register_topic(stm); });
    ASSERT_TRUE_CORO(registered) << "Failed to register topic";

    // Add files until the state machine has snapshotted a few times, so the
    // leader's log prefix is truncated and a new node must hydrate from a
    // snapshot rather than replay the log.
    constexpr auto max_snapshots = 5;
    auto completed_snapshots = 0;
    auto prev_snapshot_offset = last_snapshot_offset();
    while (completed_snapshots != max_snapshots) {
        auto added = co_await stm_retry_with_leader<0>(
          5s, [this](stm_ptr stm) { return add_next_files(stm); });
        ASSERT_TRUE_CORO(added) << "Timed out waiting to add files";
        auto snapshot_offset = last_snapshot_offset();
        if (snapshot_offset > prev_snapshot_offset) {
            completed_snapshots++;
        }
        prev_snapshot_offset = snapshot_offset;
    }

    // Add a new raft group member to hydrate from snapshot.
    auto new_node_id = model::node_id{static_cast<int32_t>(node_stms.size())};
    auto& node = add_node(new_node_id, model::revision_id{10});
    co_await start_node(node);
    co_await with_leader(
      10s, [vn = node.get_vnode()](raft_node_instance& node) {
          return node.raft()->add_group_member(vn, model::revision_id{10});
      });
    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, [this]() {
        auto offsets = last_applied_offsets();
        return ss::make_ready_future<bool>(
          std::equal(offsets.begin() + 1, offsets.end(), offsets.begin()));
    });
    ASSERT_GT_CORO(node.raft()->start_offset(), model::offset{0})
      << "New node not seeded with snapshot";

    // The snapshot-hydrated node's maintained totals (rebuilt on install) must
    // equal a fresh recompute, and be non-zero since nothing was committed.
    auto new_stm = get_stm<0>(node);
    auto expected = new_stm->state().copy();
    expected.recompute_pending();
    EXPECT_EQ(new_stm->state().pending_files(), expected.pending_files());
    EXPECT_EQ(new_stm->state().pending_bytes(), expected.pending_bytes());
    EXPECT_GT(new_stm->state().pending_bytes(), 0u);
}
