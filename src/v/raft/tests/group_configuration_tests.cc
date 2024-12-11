// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "gmock/gmock.h"
#include "raft/group_configuration.h"
#include "test_utils/randoms.h"

#include <fmt/ostream.h>

#include <ranges>

namespace {
raft::vnode create_vnode(int32_t id) {
    return {model::node_id(id), model::revision_id(0)};
}

raft::group_configuration create_configuration(std::vector<raft::vnode> nodes) {
    return {std::move(nodes), model::revision_id(0)};
}

} // namespace

TEST(test_raft_group_configuration, test_demoting_removed_voters) {
    raft::group_configuration test_grp = create_configuration(
      {create_vnode(3)});

    // add nodes
    test_grp.add(create_vnode(1), model::revision_id{0}, std::nullopt);
    test_grp.promote_to_voter(
      raft::vnode(model::node_id{1}, model::revision_id(0)));
    test_grp.finish_configuration_transition();

    test_grp.add(create_vnode(2), model::revision_id{0}, std::nullopt);
    test_grp.promote_to_voter(
      raft::vnode(model::node_id{2}, model::revision_id(0)));
    test_grp.finish_configuration_transition();

    test_grp.finish_configuration_transition();
    // remove single broker
    test_grp.remove(create_vnode(1), model::revision_id{0});
    // finish configuration transition

    ASSERT_TRUE(test_grp.maybe_demote_removed_voters());
    ASSERT_EQ(test_grp.old_config()->voters.size(), 2);
    // node 0 was demoted since it was removed from the cluster
    ASSERT_EQ(test_grp.old_config()->learners[0], create_vnode(1));
    // assert that operation is idempotent
    ASSERT_FALSE(test_grp.maybe_demote_removed_voters());
}

TEST(test_raft_group_configuration, test_aborting_configuration_change) {
    raft::group_configuration test_grp = raft::group_configuration(
      {create_vnode(3)}, model::revision_id(0));

    auto original_voters = test_grp.current_config().voters;
    auto original_nodes = test_grp.all_nodes();
    // add brokers
    test_grp.add(create_vnode(1), model::revision_id{0}, std::nullopt);

    // abort change
    test_grp.abort_configuration_change(model::revision_id{1});

    ASSERT_EQ(test_grp.get_state(), raft::configuration_state::simple);
    ASSERT_EQ(test_grp.current_config().voters, original_voters);
    ASSERT_EQ(test_grp.all_nodes(), original_nodes);
}

TEST(
  test_raft_group_configuration,
  test_reverting_configuration_change_when_adding) {
    raft::group_configuration test_grp = raft::group_configuration(
      {create_vnode(3)}, model::revision_id(0));

    // add brokers
    test_grp.add(create_vnode(1), model::revision_id{0}, std::nullopt);

    // abort change
    test_grp.cancel_configuration_change(model::revision_id{1});

    ASSERT_EQ(test_grp.get_state(), raft::configuration_state::simple);
    ASSERT_EQ(test_grp.all_nodes().size(), 1);
    ASSERT_EQ(test_grp.current_config().voters.size(), 1);
    ASSERT_EQ(test_grp.current_config().learners.size(), 0);
}
namespace {
std::vector<raft::vnode>
diff(const std::vector<raft::vnode>& lhs, const std::vector<raft::vnode>& rhs) {
    std::vector<raft::vnode> result;
    for (auto& lhs_node : lhs) {
        auto it = std::find(rhs.begin(), rhs.end(), lhs_node);
        if (it == rhs.end()) {
            result.push_back(lhs_node);
        }
    }
    return result;
}

void transition_configuration_update(raft::group_configuration& cfg) {
    while (cfg.get_state() != raft::configuration_state::simple) {
        if (cfg.get_state() == raft::configuration_state::joint) {
            cfg.maybe_demote_removed_voters();
            cfg.discard_old_config();
        }

        if (cfg.get_state() == raft::configuration_state::transitional) {
            if (!cfg.current_config().learners.empty()) {
                auto learners = cfg.current_config().learners;
                for (const auto& vnode : learners) {
                    cfg.promote_to_voter(vnode);
                }
            }
            cfg.finish_configuration_transition();
        }
    }
}

} // namespace
using namespace ::testing;
struct configuration_cancel_test_params {
    configuration_cancel_test_params(
      std::initializer_list<int> source, std::initializer_list<int> target) {
        for (auto& s : source) {
            initial_replica_set.push_back(create_vnode(s));
        }
        for (auto t : target) {
            target_replica_set.push_back(create_vnode(t));
        }
    }
    std::vector<raft::vnode> initial_replica_set;
    std::vector<raft::vnode> target_replica_set;

    friend std::ostream&
    operator<<(std::ostream&, const configuration_cancel_test_params&);
};

std::ostream&
operator<<(std::ostream& o, const configuration_cancel_test_params& p) {
    fmt::print(
      o,
      "[{}] -> [{}]",
      fmt::join(
        std::ranges::views::transform(
          p.initial_replica_set, [](auto& v) { return v.id(); }),
        ","),
      fmt::join(
        std::ranges::views::transform(
          p.target_replica_set, [](auto& v) { return v.id(); }),
        ","));
    return o;
}

class ConfigurationCancellationTest
  : public TestWithParam<configuration_cancel_test_params> {};

/**
 * This test verifies if cancelling the configuration change twice at any point
 * will lead to the cancellation being reverted.
 * example:
 * replicas are updated from set A to set B
 *  1. A->B
 * then a reconfiguration is cancelled
 *  2. B->A
 * then the reconfiguration is cancelled again
 *  3. A->B
 * Finally the replica set must be equal to B
 */
TEST_P(ConfigurationCancellationTest, TestEvenNumberOfCancellations) {
    const auto params = GetParam();

    const std::vector<raft::vnode> original_replicas
      = params.initial_replica_set;
    const std::vector<raft::vnode> target_replicas = params.target_replica_set;
    const auto to_add = diff(target_replicas, original_replicas);
    const auto to_remove = diff(original_replicas, target_replicas);

    raft::group_configuration test_cfg = raft::group_configuration(
      original_replicas, model::revision_id(0));
    test_cfg.set_version(raft::group_configuration::v_7);

    // trigger reconfiguration
    test_cfg.replace(target_replicas, model::revision_id{0}, std::nullopt);

    ASSERT_THAT(
      test_cfg.get_configuration_update()->replicas_to_add,
      ElementsAreArray(to_add));
    ASSERT_THAT(
      test_cfg.get_configuration_update()->replicas_to_remove,
      ElementsAreArray(to_remove));

    // CASE 1. Cancel right after change was requested

    // cancel configuration change, goes straight to simple state as learner can
    // be removed immediately
    auto cfg_1 = test_cfg;
    cfg_1.cancel_configuration_change(model::revision_id{0});
    // check if reconfiguration is finished by optimizations
    if (cfg_1.get_state() == raft::configuration_state::simple) {
        ASSERT_EQ(cfg_1.current_config().voters, original_replicas);
        ASSERT_TRUE(cfg_1.current_config().learners.empty());
    } else {
        cfg_1.cancel_configuration_change(model::revision_id{0});
        transition_configuration_update(cfg_1);
        ASSERT_EQ(cfg_1.current_config().voters, target_replicas);
        ASSERT_EQ(cfg_1.get_state(), raft::configuration_state::simple);
        ASSERT_TRUE(cfg_1.current_config().learners.empty());
    }
    // CASE 2. Cancel after learners promotion
    for (size_t cancel_after = 1; cancel_after <= to_add.size();
         ++cancel_after) {
        // create a copy of the configuration for each round
        auto cfg = test_cfg;
        for (size_t i = 0; i < cancel_after; ++i) {
            cfg.promote_to_voter(to_add[i]);
        }
        // update the original configuration for the next step
        if (cancel_after == to_add.size()) {
            test_cfg = cfg;
        }
        cfg.cancel_configuration_change(model::revision_id{0});
        cfg.cancel_configuration_change(model::revision_id{0});

        transition_configuration_update(cfg);

        ASSERT_EQ(cfg.current_config().voters, target_replicas);
        ASSERT_EQ(cfg.get_state(), raft::configuration_state::simple);
        ASSERT_TRUE(cfg.current_config().learners.empty());
    }

    // now finish the configuration transition as all learners were promoted
    test_cfg.finish_configuration_transition();

    // CASE 3. Cancel after leaving transitional state

    // check if reconfiguration is finished by optimizations
    if (test_cfg.get_state() == raft::configuration_state::simple) {
        ASSERT_EQ(test_cfg.current_config().voters, target_replicas);
        ASSERT_TRUE(cfg_1.current_config().learners.empty());
        return;
    }
    // at every step create a copy of test_cfg and execute cancellations against
    // the copy
    auto cfg_2 = test_cfg;

    cfg_2.cancel_configuration_change(model::revision_id{0});
    if (cfg_2.get_state() == raft::configuration_state::simple) {
        ASSERT_EQ(cfg_2.current_config().voters, original_replicas);
        ASSERT_TRUE(cfg_2.current_config().learners.empty());
    } else {
        cfg_2.cancel_configuration_change(model::revision_id{0});

        transition_configuration_update(cfg_2);

        ASSERT_EQ(cfg_2.get_state(), raft::configuration_state::simple);
        ASSERT_EQ(cfg_2.current_config().voters, target_replicas);
        ASSERT_TRUE(cfg_2.current_config().learners.empty());
    }

    test_cfg.maybe_demote_removed_voters();

    ASSERT_EQ(test_cfg.get_state(), raft::configuration_state::joint);

    // CASE 4. Cancel after demoting removed voters

    test_cfg.cancel_configuration_change(model::revision_id{0});
    // check if reconfiguration is finished by optimizations
    if (test_cfg.get_state() == raft::configuration_state::simple) {
        ASSERT_EQ(test_cfg.current_config().voters, original_replicas);
        ASSERT_TRUE(test_cfg.current_config().learners.empty());
    } else {
        test_cfg.cancel_configuration_change(model::revision_id{0});
        transition_configuration_update(test_cfg);
        ASSERT_EQ(test_cfg.get_state(), raft::configuration_state::simple);
        ASSERT_EQ(test_cfg.current_config().voters, target_replicas);
        ASSERT_TRUE(test_cfg.current_config().learners.empty());
    }
}

/**
 * This test verifies if cancelling the configuration change twice at any point
 * will lead to the cancellation being reverted.
 * example:
 * replicas are updated from set A to set B
 *  1. A->B
 * then a reconfiguration is cancelled
 *  2. B->A
 * then the reconfiguration is cancelled again
 *  3. A->B
 *  finally after last cancellation
 *  4. B->A
 *
 */
TEST_P(ConfigurationCancellationTest, TestOddNumberOfCancellations) {
    const auto params = GetParam();

    const std::vector<raft::vnode> original_replicas
      = params.initial_replica_set;
    const std::vector<raft::vnode> target_replicas = params.target_replica_set;

    const auto to_add = diff(target_replicas, original_replicas);
    const auto to_remove = diff(original_replicas, target_replicas);

    raft::group_configuration test_cfg = raft::group_configuration(
      original_replicas, model::revision_id(0));
    test_cfg.set_version(raft::group_configuration::v_7);

    // trigger reconfiguration
    test_cfg.replace(target_replicas, model::revision_id{0}, std::nullopt);

    ASSERT_THAT(
      test_cfg.get_configuration_update()->replicas_to_add,
      ElementsAreArray(to_add));
    ASSERT_THAT(
      test_cfg.get_configuration_update()->replicas_to_remove,
      ElementsAreArray(to_remove));

    // CASE 1. Cancel right after change was requested

    // cancel configuration change, goes straight to simple state as learner can
    // be removed immediately
    auto cfg_1 = test_cfg;
    cfg_1.cancel_configuration_change(model::revision_id{0});
    // check if reconfiguration is finished by optimizations
    if (cfg_1.get_state() == raft::configuration_state::simple) {
        ASSERT_EQ(cfg_1.current_config().voters, original_replicas);
    } else {
        cfg_1.cancel_configuration_change(model::revision_id{0});
        cfg_1.cancel_configuration_change(model::revision_id{0});

        transition_configuration_update(cfg_1);
        ASSERT_EQ(cfg_1.current_config().voters, original_replicas);
        ASSERT_EQ(cfg_1.get_state(), raft::configuration_state::simple);
        ASSERT_TRUE(cfg_1.current_config().learners.empty());
    }
    // CASE 2. Cancel after learners promotion
    for (size_t cancel_after = 1; cancel_after <= to_add.size();
         ++cancel_after) {
        // create a copy of the configuration for each round
        auto cfg = test_cfg;
        for (size_t i = 0; i < cancel_after; ++i) {
            cfg.promote_to_voter(to_add[i]);
        }
        // update the original configuration for the next step
        if (cancel_after == to_add.size()) {
            test_cfg = cfg;
        }
        cfg.cancel_configuration_change(model::revision_id{0});
        cfg.cancel_configuration_change(model::revision_id{0});
        cfg.cancel_configuration_change(model::revision_id{0});

        transition_configuration_update(cfg);

        ASSERT_EQ(cfg.current_config().voters, original_replicas);
        ASSERT_EQ(cfg.get_state(), raft::configuration_state::simple);
        ASSERT_TRUE(cfg.current_config().learners.empty());
    }

    // now finish the configuration transition as all learners were promoted
    test_cfg.finish_configuration_transition();

    // CASE 3. Cancel after leaving transitional state

    // check if reconfiguration is finished by optimizations
    if (test_cfg.get_state() == raft::configuration_state::simple) {
        ASSERT_EQ(test_cfg.current_config().voters, target_replicas);
        return;
    }
    // at every step create a copy of test_cfg and execute cancellations against
    // the copy
    auto cfg_2 = test_cfg;

    cfg_2.cancel_configuration_change(model::revision_id{0});
    if (cfg_2.get_state() == raft::configuration_state::simple) {
        ASSERT_EQ(cfg_2.current_config().voters, original_replicas);
        ASSERT_TRUE(cfg_2.current_config().learners.empty());
    } else {
        cfg_2.cancel_configuration_change(model::revision_id{0});
        cfg_2.cancel_configuration_change(model::revision_id{0});

        transition_configuration_update(cfg_2);

        ASSERT_EQ(cfg_2.get_state(), raft::configuration_state::simple);
        ASSERT_EQ(cfg_2.current_config().voters, original_replicas);
        ASSERT_TRUE(cfg_2.current_config().learners.empty());
    }

    test_cfg.maybe_demote_removed_voters();

    ASSERT_EQ(test_cfg.get_state(), raft::configuration_state::joint);

    // CASE 4. Cancel after demoting removed voters

    test_cfg.cancel_configuration_change(model::revision_id{0});
    // check if reconfiguration is finished by optimizations
    if (test_cfg.get_state() == raft::configuration_state::simple) {
        ASSERT_EQ(test_cfg.current_config().voters, original_replicas);
        ASSERT_TRUE(test_cfg.current_config().learners.empty());
        return;
    }
    test_cfg.cancel_configuration_change(model::revision_id{0});
    if (test_cfg.get_state() == raft::configuration_state::simple) {
        ASSERT_EQ(test_cfg.current_config().voters, target_replicas);
        ASSERT_TRUE(test_cfg.current_config().learners.empty());
        return;
    }
    test_cfg.cancel_configuration_change(model::revision_id{0});
    transition_configuration_update(test_cfg);
    ASSERT_EQ(test_cfg.get_state(), raft::configuration_state::simple);
    ASSERT_EQ(test_cfg.current_config().voters, original_replicas);
    ASSERT_TRUE(test_cfg.current_config().learners.empty());
}

// Simple helper class to handle raft group configuration advancement
struct configuration_advancement_state_machine {
    explicit configuration_advancement_state_machine(
      raft::group_configuration& cfg)
      : cfg(cfg) {
        reconcile_state();
    }

    enum class state {
        done,
        learners_promotion,
        exiting_transitional_state,
        demoting_voters,
        exiting_joint_state,
    };

    enum class direction {
        original_to_target,
        target_to_original,
    };

    friend std::ostream& operator<<(std::ostream& o, state s) {
        switch (s) {
        case state::done:
            return o << "done";
        case state::learners_promotion:
            return o << "learners_promotion";
        case state::demoting_voters:
            return o << "demoting_voters";
        case state::exiting_transitional_state:
            return o << "exiting_transitional_state";
        case state::exiting_joint_state:
            return o << "exiting_joint_state";
        }
    }

    void advance_state() {
        switch (current_state) {
        case state::done:
            break;
        case state::learners_promotion:
            cfg.promote_to_voter(cfg.current_config().learners[0]);
            reconcile_state();
            break;
        case state::exiting_transitional_state:
            cfg.finish_configuration_transition();
            reconcile_state();
            break;
        case state::demoting_voters:
            cfg.maybe_demote_removed_voters();
            reconcile_state();
            break;
        case state::exiting_joint_state:
            cfg.discard_old_config();
            reconcile_state();
            break;
        }
    }

    void cancel_reconfiguration() {
        vlog(
          logger.info,
          "Cancelling reconfiguration in state: {}",
          current_state);
        cfg.cancel_configuration_change(model::revision_id{0});
        if (dir == direction::original_to_target) {
            dir = direction::target_to_original;
        } else {
            dir = direction::original_to_target;
        }
        reconcile_state();
    }
    void abort_reconfiguration() {
        vlog(
          logger.info, "Aborting reconfiguration in state: {}", current_state);
        cfg.abort_configuration_change(model::revision_id{0});

        if (dir == direction::original_to_target) {
            dir = direction::target_to_original;
        } else {
            dir = direction::original_to_target;
        }
        reconcile_state();
    }
    void reconcile_state() {
        vlog(logger.info, "C: {}", cfg);
        if (cfg.get_state() == raft::configuration_state::simple) {
            current_state = state::done;
        } else if (cfg.get_state() == raft::configuration_state::transitional) {
            if (!cfg.current_config().learners.empty()) {
                current_state = state::learners_promotion;
            } else {
                current_state = state::exiting_transitional_state;
            }
        } else if (cfg.get_state() == raft::configuration_state::joint) {
            if (cfg.old_config()->learners.empty()) {
                current_state = state::demoting_voters;
            } else {
                current_state = state::exiting_joint_state;
            }
        }
    }
    ss::logger logger = ss::logger("test-config-stm");
    direction dir = direction::original_to_target;
    state current_state = state::done;
    raft::group_configuration& cfg;
};

TEST_P(ConfigurationCancellationTest, TestCancellationAfterAdvancement) {
    const auto params = GetParam();

    const std::vector<raft::vnode> original_replicas
      = params.initial_replica_set;
    const std::vector<raft::vnode> target_replicas = params.target_replica_set;

    const auto to_add = diff(target_replicas, original_replicas);
    const auto to_remove = diff(original_replicas, target_replicas);
    // execute the test multiple times
    for (int i = 0; i < 5000; ++i) {
        raft::group_configuration test_cfg = raft::group_configuration(
          original_replicas, model::revision_id(0));
        test_cfg.set_version(raft::group_configuration::v_7);

        // trigger reconfiguration
        test_cfg.replace(target_replicas, model::revision_id{0}, std::nullopt);

        configuration_advancement_state_machine stm(test_cfg);
        /**
         * Try advancing state until the reconfiguration is done, cancel in
         * random points
         */
        while (stm.current_state
               != configuration_advancement_state_machine::state::done) {
            if (tests::random_bool()) {
                if (tests::random_bool()) {
                    stm.cancel_reconfiguration();
                } else {
                    stm.abort_reconfiguration();
                }
            }
            stm.advance_state();
        }
        // at some point the configuration will end up in simple state, the
        // direction defines if the expected replica set is the original or the
        // target
        if (
          stm.dir
          == configuration_advancement_state_machine::direction::
            original_to_target) {
            ASSERT_THAT(
              test_cfg.current_config().voters,
              UnorderedElementsAreArray(target_replicas));
        } else {
            ASSERT_THAT(
              test_cfg.current_config().voters,
              UnorderedElementsAreArray(original_replicas));
        }
        ASSERT_TRUE(test_cfg.current_config().learners.empty());
    }
}

auto params = Values(
  configuration_cancel_test_params({0}, {1}),
  configuration_cancel_test_params({0, 1, 2}, {0, 1, 3}),
  configuration_cancel_test_params({0, 1, 2}, {10, 11, 12}),
  configuration_cancel_test_params({0, 1, 2}, {0}),
  configuration_cancel_test_params({0}, {0, 1, 3}),
  configuration_cancel_test_params({10}, {0, 1, 3}),
  configuration_cancel_test_params({0, 1, 2}, {10}),
  configuration_cancel_test_params({0, 1, 2, 3, 4}, {3, 10, 11}),
  configuration_cancel_test_params({0, 1, 2}, {0, 1, 2, 3, 4}),
  configuration_cancel_test_params({0, 1}, {10, 11}));

INSTANTIATE_TEST_SUITE_P(
  TestEvenNumberOfCancellations, ConfigurationCancellationTest, params);

INSTANTIATE_TEST_SUITE_P(
  TestOddNumberOfCancellations, ConfigurationCancellationTest, params);

INSTANTIATE_TEST_SUITE_P(
  TestCancellationAfterAdvancement, ConfigurationCancellationTest, params);
