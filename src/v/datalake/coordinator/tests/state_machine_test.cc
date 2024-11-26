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
#include "datalake/coordinator/coordinator.h"
#include "datalake/coordinator/state_machine.h"
#include "datalake/coordinator/tests/state_test_utils.h"
#include "raft/tests/stm_test_fixture.h"

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
        return config::mock_binding(500ms);
    }

    config::binding<std::chrono::seconds> snapshot_interval() const {
        return config::mock_binding(1s);
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
                table_creator,
                [this](const model::topic& t, model::revision_id r) {
                    return remove_tombstone(t, r);
                },
                file_committer,
                commit_interval());
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
              return func(coordinators[leader.get_vnode()]);
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

    model::topic_partition random_tp() const {
        return {
          model::topic{"test"},
          model::partition_id(
            random_generators::get_int<int32_t>(0, max_partitions - 1))};
    }

    ss::future<
      checked<std::nullopt_t, datalake::coordinator::coordinator::errc>>
    remove_tombstone(const model::topic&, model::revision_id) {
        co_return std::nullopt;
    }

    static constexpr int32_t max_partitions = 5;
    model::topic_partition tp{model::topic{"test"}, model::partition_id{0}};
    model::revision_id rev{123};
    cluster::data_migrations::migrated_resources mr;
    cluster::topic_table topic_table{mr};
    datalake::coordinator::noop_table_creator table_creator;
    datalake::coordinator::simple_file_committer file_committer;
    absl::flat_hash_map<raft::vnode, coordinator> coordinators;
};

TEST_F_CORO(coordinator_stm_fixture, test_snapshots) {
    co_await initialize();
    co_await wait_for_leader(5s);

    // populate some data until the state machine is snapshotted
    // a few times
    constexpr auto max_snapshots = 5;
    auto completed_snapshots = 0;
    auto prev_snapshot_offset = last_snapshot_offset();
    while (completed_snapshots != max_snapshots) {
        // mock a translator
        auto add_files_result = co_await retry_with_leader_coordinator(
          [&, this](coordinator& coordinator) mutable {
              auto tp = random_tp();
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
          {model::topic{"test"}, model::partition_id{pid}});
        vlog(logger().info, "committed offsets: {}", committed_offsets);
        ASSERT_TRUE_CORO(std::equal(
          committed_offsets.begin() + 1,
          committed_offsets.end(),
          committed_offsets.begin()))
          << "Topic state mismatch across replicas for partition: " << pid
          << ", offsets: " << committed_offsets;
    }
}
