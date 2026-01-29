// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/health_monitor_types.h"
#include "cluster/partition_balancer_planner.h"
#include "model/fundamental.h"
#include "utils/to_string.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <ranges>

namespace cluster {

constexpr auto auto_decom_timeout = std::chrono::seconds(100);
constexpr auto not_yet_timed_out
  = std::chrono::duration_cast<rpc::clock_type::duration>(
    std::chrono::seconds(50));
constexpr auto timed_out
  = std::chrono::duration_cast<rpc::clock_type::duration>(
    std::chrono::seconds(150));
constexpr auto last_seen_now
  = std::chrono::duration_cast<rpc::clock_type::duration>(
    std::chrono::seconds(0));

constexpr auto can_vote_uptime
  = std::chrono::duration_cast<std::chrono::milliseconds>(timed_out);
[[maybe_unused]] constexpr auto cannot_vote_uptime
  = std::chrono::duration_cast<std::chrono::milliseconds>(not_yet_timed_out);
[[maybe_unused]] constexpr auto uptime_irrelevant = std::chrono::milliseconds(
  0);

// testing a private static function in partition_balancer_planner, we're using
// a friend class accessor to reach in
class partition_balancer_planner_accessor {
    using params = cluster::partition_balancer_planner::
      do_get_auto_decommission_actions_params;

    // convenience representation of node state
    // up_down: represents whether a has been down past the auto decom timeout
    // node_state: relevant special node statuses that should prevent a decom
    // node_and_status: packaged node state so a test scenario can be a list of
    //                  node_id and node state
    enum class up_down : uint8_t { up, down };
    enum class node_state : uint8_t { normal, maintenance, decommissioning };
    struct node_and_status {
        up_down up_or_down;
        node_state node_state;
        model::node_id node_id;
        std::chrono::milliseconds uptime;
    };

    using report_memory_holder_t
      = std::vector<std::unique_ptr<node_liveness_report>>;

    // package a set of a params for determinining auto decommission along side
    // a holder which will keep referenced memory alive
    struct params_and_holders {
        params params;
        report_memory_holder_t report_memory_holder;
    };

    // translate the convenience representation to a parameters pack, and a list
    // of memory holders to keep the packaged references alive
    static params_and_holders
    make_params(const std::vector<node_and_status>& nodes) {
        absl::flat_hash_set<model::node_id> all_nodes{};
        absl::flat_hash_set<model::node_id> downed_nodes{};
        absl::flat_hash_set<model::node_id> maintenance_nodes{};
        absl::flat_hash_set<model::node_id> decommissioning_nodes{};

        for (const auto& node_status : nodes) {
            if (node_status.up_or_down == up_down::down) {
                downed_nodes.insert(node_status.node_id);
            }
            all_nodes.insert(node_status.node_id);

            if (node_status.node_state == node_state::maintenance) {
                maintenance_nodes.insert(node_status.node_id);
            }
            if (node_status.node_state == node_state::decommissioning) {
                decommissioning_nodes.insert(node_status.node_id);
            }
            // nothing for normal
        }

        // keeps the memory of a given auto decom report alive
        report_memory_holder_t memory_holder{};

        // index map upon the living reports
        partition_balancer_planner::auto_decom_report_map decom_report_map{};

        for (const auto& node_status : nodes) {
            if (downed_nodes.contains(node_status.node_id)) {
                // skip downed nodes for report generation
                continue;
            }

            // for living nodes, gather the bearing memory and reference list on
            // their auto decom reports
            auto report_holder = std::make_unique<node_liveness_report>();
            auto& report_ref = *report_holder;

            // add all downed nodes as timed out
            std::ranges::for_each(
              downed_nodes, [&report_ref](model::node_id node_id) mutable {
                  report_ref.node_id_to_last_seen.emplace(node_id, timed_out);
              });

            // add all not-downed nodes as current
            auto not_downed_range = all_nodes
                                    | std::ranges::views::filter(
                                      [&downed_nodes](model::node_id node_id) {
                                          return !downed_nodes.contains(
                                            node_id);
                                      });
            std::ranges::for_each(
              not_downed_range, [&report_ref](model::node_id node_id) mutable {
                  report_ref.node_id_to_last_seen.emplace(
                    node_id, last_seen_now);
              });

            partition_balancer_planner::auto_decom_node_report decom_report{
              .uptime = node_status.uptime, .liveness_report = report_ref};
            // add the report to the map
            decom_report_map.emplace(node_status.node_id, decom_report);
            memory_holder.emplace_back(std::move(report_holder));
        }

        // clang keeps putting the entire list of initializers on one line
        // clang-format off
        return params_and_holders{
          .params
          = params{
            .node_autodecommission_time = auto_decom_timeout,
            .auto_decom_report_map = std::move(decom_report_map),
            .cluster_members = std::move(all_nodes),
            .decommissioning_nodes = std::move(decommissioning_nodes),
            .maintenance_mode_nodes = std::move(maintenance_nodes),
          },
          .report_memory_holder = std::move(memory_holder)};
        // clang-format on
    }

public:
    static void smoke_test() {
        auto [params, holders] = make_params({
          node_and_status{
            .up_or_down = up_down::up,
            .node_state = node_state::normal,
            .node_id = model::node_id{0},
            .uptime = can_vote_uptime},
          node_and_status{
            .up_or_down = up_down::up,
            .node_state = node_state::normal,
            .node_id = model::node_id{1},
            .uptime = can_vote_uptime},
          node_and_status{
            .up_or_down = up_down::up,
            .node_state = node_state::normal,
            .node_id = model::node_id{2},
            .uptime = can_vote_uptime},
        });

        auto result
          = partition_balancer_planner::do_get_auto_decommission_actions(
            params);
        ASSERT_TRUE(result.empty())
          << "expected decom actions to be empty, was instead: " << result;
    }

    static void test_normal_removal() {
        // check that we can decommission a node when a majority votes that its
        // past timeout
        auto [params, holder] = make_params(
          {// node 0 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{0},
             .uptime = can_vote_uptime},

           // node 1 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{1},
             .uptime = can_vote_uptime},

           // node 3 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{3},
             .uptime = can_vote_uptime},

           // node 2 down
           node_and_status{
             .up_or_down = up_down::down,
             .node_state = node_state::normal,
             .node_id = model::node_id{2},
             .uptime = uptime_irrelevant}});

        auto result
          = partition_balancer_planner::do_get_auto_decommission_actions(
            params);
        ASSERT_EQ(result.size(), 1);

        ASSERT_EQ(result.begin().operator*(), model::node_id{2});
    }

    static void test_votes_no_quorum() {
        // check that we don't decommission if we dont have a quorum of votes
        auto [params, holder] = make_params(
          {// node 0 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{0},
             .uptime = can_vote_uptime},

           // node 1 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{1},
             .uptime = can_vote_uptime},

           // node 3 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{3},
             .uptime = can_vote_uptime},

           // node 2 down
           node_and_status{
             .up_or_down = up_down::down,
             .node_state = node_state::normal,
             .node_id = model::node_id{2},
             .uptime = uptime_irrelevant}});

        // pluck node 3's report from the reports map as though the report
        // wasn't received
        params.auto_decom_report_map.erase(model::node_id{3});
        // now there are two reports that 2 is dead, from 0 and 1

        auto result
          = partition_balancer_planner::do_get_auto_decommission_actions(
            params);
        ASSERT_EQ(result.size(), 0);
    }

    static void test_votes_insufficient_uptime() {
        // check that we don't decommission if a node has insufficient uptime to
        // vote
        auto [params, holder] = make_params(
          {// node 0 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{0},
             .uptime = can_vote_uptime},

           // node 1 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{1},
             .uptime = can_vote_uptime},

           // node 3 up but not long enough to vote for removal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{3},
             .uptime = cannot_vote_uptime},

           // node 2 down
           node_and_status{
             .up_or_down = up_down::down,
             .node_state = node_state::normal,
             .node_id = model::node_id{2},
             .uptime = uptime_irrelevant}});

        auto result
          = partition_balancer_planner::do_get_auto_decommission_actions(
            params);
        ASSERT_EQ(result.size(), 0);
    }

    static void test_dont_decom_twice() {
        // make sure a decommissioning node doesn't get another decommissioning
        // command
        auto [params, holder] = make_params(
          {// node 0 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{0},
             .uptime = can_vote_uptime},

           // node 1 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{1},
             .uptime = can_vote_uptime},

           // node 3 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{3},
             .uptime = can_vote_uptime},

           // node 2 down
           node_and_status{
             .up_or_down = up_down::down,
             .node_state = node_state::decommissioning,
             .node_id = model::node_id{2},
             .uptime = uptime_irrelevant}});

        auto result
          = partition_balancer_planner::do_get_auto_decommission_actions(
            params);
        ASSERT_EQ(result.size(), 0);
    }

    static void test_dont_decom_maintenance() {
        // if a node is in maintenance mode, dont decommission it
        auto [params, holder] = make_params(
          {// node 0 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{0},
             .uptime = can_vote_uptime},

           // node 1 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{1},
             .uptime = can_vote_uptime},

           // node 3 up and normal
           node_and_status{
             .up_or_down = up_down::up,
             .node_state = node_state::normal,
             .node_id = model::node_id{3},
             .uptime = can_vote_uptime},

           // node 2 down
           node_and_status{
             .up_or_down = up_down::down,
             .node_state = node_state::maintenance,
             .node_id = model::node_id{2},
             .uptime = uptime_irrelevant}});

        auto result
          = partition_balancer_planner::do_get_auto_decommission_actions(
            params);
        ASSERT_EQ(result.size(), 0);
    }

    static void test_ignore_non_members() {
        // make sure that we ignore reports from nodes that are no longer
        // cluster members
        auto [params, holder] = make_params({
          // node 0 up and normal
          node_and_status{
            .up_or_down = up_down::up,
            .node_state = node_state::normal,
            .node_id = model::node_id{0},
            .uptime = can_vote_uptime},

          // node 1 up and normal
          node_and_status{
            .up_or_down = up_down::up,
            .node_state = node_state::normal,
            .node_id = model::node_id{1},
            .uptime = can_vote_uptime},

          // node 3 up and normal
          node_and_status{
            .up_or_down = up_down::up,
            .node_state = node_state::normal,
            .node_id = model::node_id{3},
            .uptime = can_vote_uptime},

          // node 2 down
          node_and_status{
            .up_or_down = up_down::down,
            .node_state = node_state::maintenance,
            .node_id = model::node_id{2},
            .uptime = can_vote_uptime},

          // node will be a non-member
          node_and_status{
            .up_or_down = up_down::up,
            .node_state = node_state::normal,
            .node_id = model::node_id{4},
            .uptime = can_vote_uptime},

          // node 5 will be a non-member
          node_and_status{
            .up_or_down = up_down::up,
            .node_state = node_state::normal,
            .node_id = model::node_id{5},
            .uptime = can_vote_uptime},

        });

        // drop 4 and 5, this should leave not enough votes to remove 2
        params.cluster_members.erase(model::node_id{4});
        params.cluster_members.erase(model::node_id{5});

        auto result
          = partition_balancer_planner::do_get_auto_decommission_actions(
            params);
        ASSERT_EQ(result.size(), 0);
    }

    static void test_multi_decom() {
        // checks that given a large enough cluster, we can recommend multiple
        // nodes to decom at the same time
        // for the time being, this isn't actually used as a preprocessing step
        // cut the list to only one
        std::vector<node_and_status> nodes_and_statuses{};

        for (auto node_number : std::ranges::iota_view(0, 5)) {
            auto node_id = model::node_id{node_number};
            nodes_and_statuses.emplace_back(
              node_and_status{
                .up_or_down = up_down::up,
                .node_state = node_state::normal,
                .node_id = node_id,
                .uptime = can_vote_uptime});
        }
        nodes_and_statuses[0].up_or_down = up_down::down;
        nodes_and_statuses[1].up_or_down = up_down::down;
        auto [params, holder] = make_params(nodes_and_statuses);

        auto result
          = partition_balancer_planner::do_get_auto_decommission_actions(
            params);
        ASSERT_EQ(result.size(), 2);
        ASSERT_TRUE(result.contains(model::node_id{0}));
        ASSERT_TRUE(result.contains(model::node_id{1}));
    }

    static void test_quorum_limits() {
        // we should only decom a node if a quorum agrees that the node is past
        // timeout this test checks the limits for even & odd cluster numbers
        std::vector<node_and_status> nodes_and_statuses{};
        int dead_node_number{0};
        model::node_id dead_node{dead_node_number};

        {
            // case: 5
            for (auto node_number : std::ranges::iota_view(0, 5)) {
                auto node_id = model::node_id{node_number};
                nodes_and_statuses.emplace_back(
                  node_and_status{
                    .up_or_down = up_down::up,
                    .node_state = node_state::normal,
                    .node_id = node_id,
                    .uptime = can_vote_uptime});
            }
            nodes_and_statuses[dead_node_number].up_or_down = up_down::down;
            auto [params, holder] = make_params(nodes_and_statuses);
            { // all agree
                auto result = partition_balancer_planner::
                  do_get_auto_decommission_actions(params);
                ASSERT_EQ(result.size(), 1);
                ASSERT_TRUE(result.contains(dead_node));
            }
            { // 3 agree 1 does not
                params.auto_decom_report_map.erase(model::node_id{1});
                auto result = partition_balancer_planner::
                  do_get_auto_decommission_actions(params);
                ASSERT_EQ(result.size(), 1);
                ASSERT_TRUE(result.contains(dead_node));
            }
            { // 2 agree 2 do not
                params.auto_decom_report_map.erase(model::node_id{2});
                auto result = partition_balancer_planner::
                  do_get_auto_decommission_actions(params);
                ASSERT_EQ(result.size(), 0);
            }
        }
        {
            // case: 6
            for (auto node_number : std::ranges::iota_view(0, 6)) {
                auto node_id = model::node_id{node_number};
                nodes_and_statuses.emplace_back(
                  node_and_status{
                    .up_or_down = up_down::up,
                    .node_state = node_state::normal,
                    .node_id = node_id,
                    .uptime = can_vote_uptime});
            }
            nodes_and_statuses[dead_node_number].up_or_down = up_down::down;
            auto [params, holder] = make_params(nodes_and_statuses);
            { // all agree
                auto result = partition_balancer_planner::
                  do_get_auto_decommission_actions(params);
                ASSERT_EQ(result.size(), 1);
                ASSERT_TRUE(result.contains(dead_node));
            }
            { // 4 agree 1 does not
                params.auto_decom_report_map.erase(model::node_id{1});
                auto result = partition_balancer_planner::
                  do_get_auto_decommission_actions(params);
                ASSERT_EQ(result.size(), 1);
                ASSERT_TRUE(result.contains(dead_node));
            }
            { // 3 agree 2 do not
                params.auto_decom_report_map.erase(model::node_id{2});
                auto result = partition_balancer_planner::
                  do_get_auto_decommission_actions(params);
                ASSERT_EQ(result.size(), 0);
            }
        }
    }

    static void test_postprocess() {
        absl::flat_hash_set<model::node_id> candidates{
          model::node_id{3},
          model::node_id{2},
          model::node_id{1},
          model::node_id{0}};
        absl::flat_hash_set<model::node_id> decomming_nodes{model::node_id{1}};
        { // if theres a decomming node, dont choose anything
            auto maybe_node_to_decom = partition_balancer_planner::
              do_postprocess_auto_decommission_actions(
                candidates, decomming_nodes);
            ASSERT_FALSE(maybe_node_to_decom.has_value());
        }
        { // if theres no candidates, dont choose anything
            auto maybe_node_to_decom = partition_balancer_planner::
              do_postprocess_auto_decommission_actions({}, decomming_nodes);
            ASSERT_FALSE(maybe_node_to_decom.has_value());
        }
        { // choose the minimum node always
            auto maybe_node_to_decom = partition_balancer_planner::
              do_postprocess_auto_decommission_actions(candidates, {});
            ASSERT_TRUE(maybe_node_to_decom.has_value());
            ASSERT_EQ(maybe_node_to_decom, model::node_id{0});
        }
    }
};

TEST(AutoDecomTestSuite, Smoke) {
    partition_balancer_planner_accessor::smoke_test();
};
TEST(AutoDecomTestSuite, NormalRemove) {
    partition_balancer_planner_accessor::test_normal_removal();
};
TEST(AutoDecomTestSuite, NoQuorum) {
    partition_balancer_planner_accessor::test_votes_no_quorum();
}
TEST(AutoDecomTestSuite, InsufficientUptime) {
    partition_balancer_planner_accessor::test_votes_insufficient_uptime();
}
TEST(AutoDecomTestSuite, DontDecomTwice) {
    partition_balancer_planner_accessor::test_dont_decom_twice();
}
TEST(AutoDecomTestSuite, DontDecomMaintenance) {
    partition_balancer_planner_accessor::test_dont_decom_maintenance();
}
TEST(AutoDecomTestSuite, IgnoreNonMembers) {
    partition_balancer_planner_accessor::test_ignore_non_members();
}
TEST(AutoDecomTestSuite, DecomMultipleNodes) {
    partition_balancer_planner_accessor::test_multi_decom();
}
TEST(AutoDecomTestSuite, QuorumLimits) {
    partition_balancer_planner_accessor::test_quorum_limits();
}
TEST(AutoDecomTestSuite, PostProcess) {
    partition_balancer_planner_accessor::test_postprocess();
}
} // namespace cluster
