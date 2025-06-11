/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cluster/producer_state_manager.h"
#include "cluster/rm_stm.h"
#include "cluster/tests/tx_compaction_utils.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "raft/fundamental.h"
#include "raft/recovery_client_protocol.h"
#include "raft/recovery_memory_quota.h"
#include "raft/recovery_stm.h"
#include "raft/state_machine_manager.h"
#include "raft/tests/common.h"
#include "raft/tests/raft_fixture.h"
#include "raft/types.h"
#include "storage/ntp_config.h"
#include "test_utils/scoped_config.h"
#include "utils/tristate.h"

#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

using namespace std::chrono_literals;

static ss::logger raft_recovery_test_log("raft_recovery_test");

namespace raft {

class RaftRecoveryFixture : public raft_fixture {
public:
    ss::future<> start(
      int node_count,
      std::optional<storage::ntp_config::default_overrides> overrides
      = std::nullopt) {
        for (int i = 0; i < node_count; ++i) {
            add_node(model::node_id(i), model::revision_id(0));
        }

        for (auto& [id, node] : nodes()) {
            raft::state_machine_manager_builder builder;
            co_await node->initialise(all_vnodes(), overrides);
            auto state = std::make_unique<rm_stm_state>();
            co_await state->start();
            auto rm_stm = builder.create_stm<cluster::rm_stm>(
              raft_recovery_test_log,
              node->raft().get(),
              state->tx_gateway_frontend,
              node->get_feature_table(),
              state->producer_state_manager,
              std::nullopt);
            state->rm_stm = rm_stm;
            _rm_stms[id] = std::move(state);
            co_await node->start(std::move(builder));
            node->raft()->log()->stm_manager()->add_stm(rm_stm);
        }
    }

    seastar::future<> TearDownAsync() override {
        co_await raft::raft_fixture::TearDownAsync();
        for (auto& [_, state] : _rm_stms) {
            co_await state->stop();
        }
    }

    raft_node_map& get_raft_node_map() { return *this; }

protected:
    struct recovery_stm_state {
        std::unique_ptr<recovery_stm> stm;
        std::unique_ptr<recovery_client_protocol> recovery_client;
        std::unique_ptr<recovery_memory_quota> memory_quota;
    };

    recovery_stm_state make_recovery_stm_state(
      consensus* p,
      vnode target_node,
      model::timestamp recovery_start_time_override = model::timestamp::now()) {
        recovery_stm_state state;
        auto scfg = scheduling_config(
          ss::default_scheduling_group(), ss::default_scheduling_group());
        state.memory_quota = std::make_unique<recovery_memory_quota>([]() {
            return raft::recovery_memory_quota::configuration{
              .max_recovery_memory
              = config::mock_binding<std::optional<size_t>>(std::nullopt),
              .default_read_buffer_size = config::mock_binding(512_KiB),
            };
        });
        state.recovery_client = std::make_unique<recovery_client_protocol>(
          ss::make_shared<in_memory_recovery_client>(
            p->self().id(), get_raft_node_map()));
        state.stm = std::make_unique<recovery_stm>(
          p,
          target_node,
          *state.recovery_client,
          std::move(scfg),
          *state.memory_quota,
          recovery_start_time_override);
        return state;
    }

    struct rm_stm_state {
        ss::future<> start() {
            co_await producer_state_manager.start(
              config::mock_binding(std::numeric_limits<uint64_t>::max()),
              config::mock_binding(
                std::numeric_limits<std::chrono::milliseconds>::max()),
              config::mock_binding(std::numeric_limits<size_t>::max()));
            co_await producer_state_manager.invoke_on_all(
              [](cluster::tx::producer_state_manager& mgr) {
                  return mgr.start();
              });
        }

        ss::future<> stop() {
            co_await producer_state_manager.stop();
            rm_stm = nullptr;
        }

        ss::sharded<cluster::tx_gateway_frontend> tx_gateway_frontend;
        ss::sharded<cluster::tx::producer_state_manager> producer_state_manager;
        ss::shared_ptr<cluster::rm_stm> rm_stm = nullptr;
    };
    scoped_config cfg;
    std::unordered_map<model::node_id, std::unique_ptr<rm_stm_state>> _rm_stms;
};

struct recovery_test_case {
    std::string_view desc;
    model::cleanup_policy_bitflags cleanup_policy;
    std::optional<model::timestamp> clean_compact_timestamp;
    std::optional<model::timestamp> self_compact_timestamp;
    bool has_tombstone_records;
    bool has_tx_batches;
    std::optional<model::offset> next_index;
    model::timestamp recovery_start_time{model::timestamp::now()};
    tristate<std::chrono::milliseconds> delete_retention_ms{24h};
    bool disable_checks{false};
    bool expect_reset;

    ss::future<> set_up(
      ss::shared_ptr<storage::log> leader_log,
      follower_index_metadata& to_recover_metadata) const {
        auto overrides = storage::ntp_config::default_overrides{
          .cleanup_policy_bitflags = cleanup_policy,
          .delete_retention_ms = delete_retention_ms};

        leader_log->set_overrides(overrides);
        leader_log->force_roll().get();

        auto seg = leader_log->segments().back();

        if (clean_compact_timestamp.has_value()) {
            ASSERT_TRUE_CORO(seg->index().maybe_set_clean_compact_timestamp(
              clean_compact_timestamp.value()));
        }

        if (self_compact_timestamp.has_value()) {
            ASSERT_TRUE_CORO(seg->index().maybe_set_self_compact_timestamp(
              self_compact_timestamp.value()));
        }

        seg->index().set_may_have_tombstone_records(has_tombstone_records);
        seg->index().set_has_transaction_batches(has_tx_batches);

        if (next_index.has_value()) {
            to_recover_metadata.next_index = next_index.value();
        }

        auto& ot = const_cast<storage::segment::offset_tracker&>(
          seg->offsets());
        ot.set_offsets(
          storage::segment::offset_tracker::committed_offset_t{100},
          storage::segment::offset_tracker::stable_offset_t{100},
          storage::segment::offset_tracker::dirty_offset_t{100});
    }
};

TEST_F(RaftRecoveryFixture, SafeRecoveryUnitTestCases) {
    cfg.get("log_disable_housekeeping_for_tests").set_value(true);
    start(2).get();

    static std::vector<recovery_test_case> test_cases = {
      recovery_test_case{
        .desc = "Compacted topic starting recovery for the first time with no "
                "currently removable tombstones or transaction batches and a "
                "reasonable delete.retention.ms default doesn't need to reset.",
        .cleanup_policy = model::cleanup_policy_bitflags::compaction,
        .clean_compact_timestamp = std::nullopt,
        .self_compact_timestamp = std::nullopt,
        .has_tombstone_records = false,
        .has_tx_batches = false,
        .next_index = std::nullopt,
        .expect_reset = false},
      recovery_test_case{
        .desc = "Non-compact topic should never be eligible for resets during "
                "recovery, even with all possible flags set and an extreme "
                "value for delete.retention.ms.",
        .cleanup_policy = model::cleanup_policy_bitflags::deletion,
        .clean_compact_timestamp = model::timestamp::min(),
        .self_compact_timestamp = model::timestamp::min(),
        .has_tombstone_records = true,
        .has_tx_batches = true,
        .next_index = std::nullopt,
        .delete_retention_ms = tristate<std::chrono::milliseconds>(1ms),
        .expect_reset = false},
      recovery_test_case{
        .desc = "Compact topic whose recovery has exceeded delete.retention.ms "
                "should reset.",
        .cleanup_policy = model::cleanup_policy_bitflags::compaction,
        .clean_compact_timestamp = std::nullopt,
        .self_compact_timestamp = std::nullopt,
        .has_tombstone_records = false,
        .has_tx_batches = false,
        .next_index = std::nullopt,
        .recovery_start_time = model::timestamp::min(),
        .expect_reset = true},
      recovery_test_case{
        .desc = "Learner who is continuing recovery below "
                "earliest_removable_timestamp() due to potential tx batch "
                "removal needs to reset its state.",
        .cleanup_policy = model::cleanup_policy_bitflags::compaction,
        .clean_compact_timestamp = std::nullopt,
        .self_compact_timestamp = model::timestamp::min(),
        .has_tombstone_records = false,
        .has_tx_batches = true,
        .next_index = model::offset(20),
        .expect_reset = true},
      recovery_test_case{
        .desc
        = "Learner who is continuing recovery below "
          "earliest_removable_timestamp() due to potential tombstone record "
          "removal needs to reset its state.",
        .cleanup_policy = model::cleanup_policy_bitflags::compaction,
        .clean_compact_timestamp = model::timestamp::min(),
        .self_compact_timestamp = std::nullopt,
        .has_tombstone_records = true,
        .has_tx_batches = false,
        .next_index = model::offset(20),
        .expect_reset = true},
      recovery_test_case{
        .desc = "Disabling checks at the cluster level prevents resets.",
        .cleanup_policy = model::cleanup_policy_bitflags::compaction,
        .clean_compact_timestamp = model::timestamp::min(),
        .self_compact_timestamp = model::timestamp::min(),
        .has_tombstone_records = true,
        .has_tx_batches = true,
        .next_index = std::nullopt,
        .recovery_start_time = model::timestamp::min(),
        .delete_retention_ms = tristate<std::chrono::milliseconds>(1ms),
        .disable_checks = true,
        .expect_reset = false},
    };

    auto leader_id = model::node_id(0);
    auto& leader = node_for(leader_id).value().get();
    auto leader_raft = leader.raft();
    auto& followers = testing_details::consensus_accessor::follower_stats(
      leader_raft);
    auto leader_log = leader_raft->log();

    auto& [to_recover_id, to_recover_metadata] = *followers.begin();
    auto& to_recover = node_for(to_recover_id.id()).value().get();

    auto prev_resets = 0;
    for (const auto& test_case : test_cases) {
        vlog(
          raft_recovery_test_log.info, "Running test case: {}", test_case.desc);
        test_case.set_up(leader_log, to_recover_metadata).get();
        cfg.get("raft_recovery_disable_compaction_safety_checks")
          .set_value(test_case.disable_checks);

        auto recovery_stm_state = make_recovery_stm_state(
          leader_raft.get(), to_recover_id, test_case.recovery_start_time);
        auto& recovery = *recovery_stm_state.stm;
        recovery.apply().get();
        auto new_resets = to_recover.raft()->get_probe().get_recovery_resets();
        if (test_case.expect_reset) {
            ASSERT_GT(new_resets, prev_resets);
        } else {
            ASSERT_EQ(new_resets, prev_resets);
        }
        prev_resets = new_resets;
    }
}

class RaftRecoveryTxWorkloadTest
  : public RaftRecoveryFixture
  , public ::testing::WithParamInterface<
      std::tuple<int, int, cluster::tx_executor::tx_types, bool>> {};

TEST_P(RaftRecoveryTxWorkloadTest, RecoveryTxWorkloads) {
    cfg.get("log_disable_housekeeping_for_tests").set_value(true);
    cfg.get("log_segment_ms_min").set_value(1ms);
    auto overrides = storage::ntp_config::default_overrides{
      .cleanup_policy_bitflags = model::cleanup_policy_bitflags::compaction,
      .segment_size = std::make_optional<size_t>(10),
      .segment_ms = tristate<std::chrono::milliseconds>(1s),
      .delete_retention_ms = tristate<std::chrono::milliseconds>(500ms),
    };

    start(1, overrides).get();
    auto scfg = scheduling_config(
      ss::default_scheduling_group(), ss::default_scheduling_group());
    auto mquota = recovery_memory_quota([]() {
        return raft::recovery_memory_quota::configuration{
          .max_recovery_memory = config::mock_binding<std::optional<size_t>>(
            std::nullopt),
          .default_read_buffer_size = config::mock_binding(512_KiB),
        };
    });

    auto leader_id = wait_for_leader(2000ms).get();
    auto& leader = node_for(leader_id).value().get();
    auto leader_raft = leader.raft();
    auto leader_log = leader_raft->log();

    auto [num_tx, num_rolls, type, interleave] = GetParam();
    cluster::tx_executor::spec spec{
      ._num_txes = num_tx,
      ._num_rolls = num_rolls,
      ._types = type,
      ._interleave = interleave,
      ._num_records = 500,
      ._num_batches = 10};
    vlog(raft_recovery_test_log.info, "Running spec: {}", spec);

    auto new_node_fut
      = ss::sleep(std::chrono::milliseconds(random_generators::get_int(1, 50)))
          .then([&] {
              // Trigger recovery by adding a new node to the raft group.
              auto& new_node = add_node(
                model::node_id(1), model::revision_id(0));
              return new_node.init_and_start({}).then([&] {
                  return leader_raft->add_group_member(
                    new_node.get_vnode(), model::revision_id(0));
              });
          });
    auto rm_stm = _rm_stms.at(leader_id)->rm_stm;
    rm_stm->testing_only_disable_auto_abort();
    cluster::tx_executor{}.run_random_workload(
      spec, leader_raft->term(), rm_stm, leader_log);
    new_node_fut.get();
    vlog(raft_recovery_test_log.info, "Finished spec: {}", spec);
}

INSTANTIATE_TEST_SUITE_P(
  RaftRecoveryTxWorkloadTestParams,
  RaftRecoveryTxWorkloadTest,
  ::testing::Combine(
    ::testing::Values(10, 20, 30),
    ::testing::Values(0, 1, 2, 3, 5),
    ::testing::Values(
      cluster::tx_executor::tx_types::commit_only,
      cluster::tx_executor::tx_types::abort_only,
      cluster::tx_executor::tx_types::mixed),
    ::testing::Bool()));

} // namespace raft
