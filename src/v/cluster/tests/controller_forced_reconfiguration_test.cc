// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/vassert.h"
#include "cluster/controller_forced_reconfiguration_manager.h"
#include "cluster/errc.h"
#include "cluster/members_frontend.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "config/configuration.h"
#include "gtest/gtest.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "raft/fundamental.h"
#include "redpanda/tests/fixture.h"

#include <algorithm>
#include <exception>

namespace {
ss::logger _logger{"controller_forced_reconfiguration_test"};

// help log begin and end of operations
struct scope_logger {
    ss::sstring _log_line;

    scope_logger(ss::sstring log_line) noexcept
      : _log_line{std::move(log_line)} {
        vlog(_logger.info, "starting {}", _log_line);
    }

    ~scope_logger() { vlog(_logger.info, "finishing {}", _log_line); }
};

struct topic_config {
    model::topic name;
    uint8_t partition_count;
    uint8_t replication_factor;
};

constexpr auto small_wait = 3s;
constexpr auto medium_wait = 15s;
constexpr auto large_wait = 120s;

namespace { // smoke tests
static const auto default_topic = topic_config{
  .name = model::topic{"test-topic"},
  .partition_count = 3,
  .replication_factor = 3};

constexpr raft::vnode to_vnode(int node_id) {
    return raft::vnode{model::node_id{node_id}, model::revision_id{0}};
}

template<typename T>
auto typed_range(int begin, T end) {
    return std::views::iota(static_cast<T>(begin), end);
}

class cfr_fixture_base : public cluster_test_fixture {
public:
    application* create_node_application(
      model::node_id node_id, model::node_id survivor = model::node_id{0}) {
        // cluster_test_fixture::instance(model::node_id id)
        return cluster_test_fixture::create_node_application(
          node_id,
          9092,
          11000,
          std::nullopt,
          std::nullopt,
          configure_node_id::yes,
          empty_seed_starts_cluster::yes,
          std::nullopt,
          std::nullopt,
          std::nullopt,
          true,
          false,
          false,
          true,
          survivor);
    }
};

class CFRFixture
  : public cfr_fixture_base
  , public ::testing::Test {
public:
    explicit CFRFixture(uint16_t cluster_size) noexcept
      : _cluster_size(cluster_size) {}

    // init _cluster_size nodes, wait for a controller leader
    void SetUp() override {
        // cut down on compaction memory usage otherwise this test will OOM
        config::shard_local_cfg().storage_compaction_key_map_memory.set_value(
          16_MiB);
        config::shard_local_cfg().log_compaction_use_sliding_window.set_value(
          false);

        for (const auto node_number : typed_range(0, _cluster_size)) {
            create_node_application(model::node_id{node_number});
            _living_nodes.emplace(node_number);
        }

        wait_for_all_members(small_wait).get();
        wait_for_controller_leadership(model::node_id{0}).get();
    }
    void TearDown() override {}

    // kills a list of nodes
    void batch_kill_nodes(const std::vector<model::node_id>& nodes_to_kill) {
        for (const auto node_to_kill : nodes_to_kill) {
            _logger.info("killing node: {}", node_to_kill);
            this->remove_node_application(node_to_kill);

            _dead_nodes.emplace(node_to_kill);
            _living_nodes.erase(node_to_kill);
        }
    }

    // take a list of node_ids to start and a seed node to ring for cluster join
    void batch_start_nodes(
      const std::vector<model::node_id>& nodes_to_start,
      model::node_id survivor = model::node_id{0}) {
        for (const auto& node_to_start : nodes_to_start) {
            _logger.info(
              "starting node: {}, seed node to ring: {}",
              node_to_start,
              survivor);
            this->create_node_application(node_to_start, survivor);

            _dead_nodes.erase(node_to_start);
            _living_nodes.insert(node_to_start);
        }
    }

    std::vector<model::node_id> get_nodes_to_kill() {
        // cluster size of 5 -> kill [0,3)
        uint16_t kill_up_to_exclusive = _cluster_size / 2 + 1;
        std::vector<model::node_id> nodes_to_kill{};
        nodes_to_kill.reserve(kill_up_to_exclusive);
        for (const auto node_number : typed_range(0, kill_up_to_exclusive)) {
            nodes_to_kill.emplace_back(node_number);
        }
        return nodes_to_kill;
    }

    // kill the majority of the cluster nodes starting at 0
    // for a cluster of size 5, will kill 0, 1, and 2
    void induce_controller_loss() {
        scope_logger sl{"induce controller loss"};

        batch_kill_nodes(get_nodes_to_kill());
    }

    // snap the dead nodes to a vector
    std::vector<model::node_id> get_dead_nodes() {
        return std::vector<model::node_id>{
          _dead_nodes.begin(), _dead_nodes.end()};
    }

    // snap the living nodes to a vector
    std::vector<model::node_id> get_living_nodes() {
        return std::vector<model::node_id>{
          _living_nodes.begin(), _living_nodes.end()};
    }

    // shutdown all living nodes, swap recovery mode, restart all living nodes
    void toggle_recovery_mode(bool recovery_mode_enabled) {
        scope_logger sl{
          fmt::format("toggling recovery mode to {}", recovery_mode_enabled)};
        const auto living_nodes = get_living_nodes();
        batch_kill_nodes(living_nodes);
        config::node().recovery_mode_enabled.set_value(recovery_mode_enabled);
        batch_start_nodes(living_nodes, living_nodes.front());
    }

    // for all surviving nodes, run controller forced recovery
    ss::future<std::vector<std::error_code>> force_recover_cluster_async() {
        scope_logger sl{"all survivor controller force recovery"};
        auto living_nodes = get_living_nodes();
        auto dead_nodes = get_dead_nodes();
        vassert(living_nodes.size() > 0, "can't progress with 0 nodes");

        std::vector<std::error_code> results{};
        results.reserve(living_nodes.size());
        for (const auto living_node : living_nodes) {
            vlog(_logger.info, "force recovering node: {}", living_node);
            const auto err
              = co_await this->get_node_application(living_node)
                  ->controller->initialize_controller_forced_reconfiguration(
                    dead_nodes, static_cast<uint16_t>(living_nodes.size()));
            vlog(_logger.info, "node: {}, finished with {}", living_node, err);
            results.emplace_back(err.err);
        }
        co_return results;
    }

    std::vector<std::error_code> force_recover_cluster() {
        return force_recover_cluster_async().get();
    }

    // checks that all living nodes see no controller leader, timeout exception
    // if check fails
    void check_no_controller_leader() {
        auto living_nodes = get_living_nodes();
        std::vector<ss::future<>> leadership_futures{};
        leadership_futures.reserve(living_nodes.size());
        for (const auto living_node : living_nodes) {
            leadership_futures.push_back(
              wait_for_controller_leadership(living_node));
        }

        // make sure that we're seeing as many timeouts as there are living
        // nodes
        uint timeout_count{0};
        for (auto& future : leadership_futures) {
            try {
                std::move(future).get();
            } catch (const seastar::timed_out_error& e) {
                vlog(_logger.info, "controller is leaderless as expected");
                ++timeout_count;
            } catch (...) {
                vlog(
                  _logger.info,
                  "unexpected leadership wait exception {}",
                  std::current_exception());
                throw;
            }
        }

        ASSERT_EQ(timeout_count, living_nodes.size())
          << "controller leadership should not be found on any node in the "
             "cluster";
    }

    // either leader will be found or this will throw timeout
    void check_controller_leader() {
        wait_for(medium_wait, [this] {
            vlog(_logger.info, "waiting for controller leader");
            auto [rp_thread, partition] = get_leader(model::controller_ntp);
            return rp_thread != nullptr && partition != nullptr;
        });
    }

    // create a topic per its configuration
    void setup_topic(const topic_config& topic_config) {
        scope_logger sl{fmt::format("add topic {}", topic_config.name)};
        auto living_nodes = get_living_nodes();
        wait_for_controller_leadership(living_nodes.front()).get();

        auto* app = get_node_application(living_nodes.front());
        auto maybe_controller_leader
          = app->controller->get_partition_leaders().local().get_leader(
            model::controller_ntp);
        ASSERT_TRUE(maybe_controller_leader.has_value())
          << "failed to get controller leader";
        cluster::topic_properties props{};
        props.cleanup_policy_bitflags = model::cleanup_policy_bitflags::none;
        props.compaction_strategy = std::nullopt;

        create_topic(
          model::topic_namespace_view{
            model::kafka_namespace, topic_config.name},
          topic_config.partition_count,
          topic_config.replication_factor,
          std::move(props))
          .get();
    }

    // restore the cluster to its original node size
    void restore_node_number() {
        scope_logger sl{"restore original node count"};
        uint16_t first_survivor = _cluster_size / 2 + 1;

        std::vector<application*> new_apps{};
        // for each dead node, create a replacement
        for (const auto dead_node_number : typed_range(0, first_survivor)) {
            const auto new_node_number = dead_node_number + _cluster_size;
            auto new_app = create_node_application(
              model::node_id{new_node_number}, model::node_id{first_survivor});

            ASSERT_NE(new_app, nullptr) << "app failed to be created";
            new_apps.emplace_back(new_app);
        }

        // check that we have returned to _cluster_size from the perspective of
        // every node
        for (const auto node_number : typed_range(0, _cluster_size)) {
            const model::node_id node_to_query{node_number + first_survivor};
            // Wait for cluster to recognize at least _cluster_size
            try {
                tests::cooperative_spin_wait_with_timeout(
                  medium_wait,
                  [this, node_to_query] {
                      vlog(
                        _logger.info,
                        "iterating node count wait with expected: {}, found: "
                        "{}",
                        _cluster_size,
                        get_local_cache(node_to_query).node_count());
                      return get_local_cache(node_to_query).node_count()
                             >= _cluster_size;
                  })
                  .get();
            } catch (...) {
                vlog(
                  _logger.info,
                  "restore_node_number failed with exception on coop spin on "
                  "node {}",
                  node_to_query);
            }
        }
    }

    // given a topic config, check that all partitions have leadership restored
    // throws timeout exception otherwise
    void wait_for_leader_restoration(topic_config topic_config) {
        // step 1, wait for a controller leader
        check_controller_leader();
        auto [controller_rp_thread, controller_partition] = get_leader(
          model::controller_ntp);

        // step 2, force the leader to catch up
        controller_rp_thread->app.controller->linearizable_barrier().get();

        // step 3, find a leader for all partitions
        for (const auto partition_number :
             typed_range(0, topic_config.partition_count)) {
            wait_for(large_wait, [this, &topic_config, partition_number] {
                const auto ntp = model::ntp{
                  model::kafka_namespace,
                  topic_config.name,
                  model::partition_id{partition_number}};
                vlog(_logger.info, "polling for leadership on ntp: {}", ntp);
                auto [rp_thread, partition] = this->get_leader(ntp);
                return rp_thread != nullptr && partition != nullptr;
            });
        }
    }

    // this assumes that the controller is alive, dont use this to poll or
    // otherwise
    redpanda_thread_fixture* safe_get_controller_rp() {
        // get controller from living nodes
        auto living_nodes = get_living_nodes();
        wait_for_controller_leadership(living_nodes.front()).get();
        auto* app = get_node_application(living_nodes.front());
        auto maybe_controller_leader
          = app->controller->get_partition_leaders().local().get_leader(
            model::controller_ntp);
        vassert(
          maybe_controller_leader.has_value(),
          "failed to get controller leader");

        auto controller_leader = *maybe_controller_leader;
        auto controller_rp = instance(controller_leader);

        vassert(
          controller_rp != nullptr,
          "controller leader instance should not be nullptr");

        return controller_rp;
    }

    // put one partition of the topic as much as possible onto the dead nodes
    void move_partition_onto_dying_nodes(const topic_config& topic_config) {
        auto to_move_ntp = model::ntp{
          model::kafka_namespace, topic_config.name, model::partition_id{0}};

        scope_logger sl{
          fmt::format("move partition onto dying nodes {}", to_move_ntp)};

        auto* controller_rp = safe_get_controller_rp();

        // use topic table to request the move
        auto& topics_frontend
          = controller_rp->app.controller->get_topics_frontend().local();

        std::vector<model::broker_shard> to_kill_broker_shards{
          std::from_range,
          std::ranges::views::transform(
            get_nodes_to_kill(),
            [](model::node_id node_id) -> model::broker_shard {
                return model::broker_shard{.node_id = node_id, .shard = 0};
            })};

        // request the move
        auto move_err
          = topics_frontend
              .move_partition_replicas(
                to_move_ntp,
                to_kill_broker_shards,
                cluster::reconfiguration_policy::full_local_retention,
                ss::lowres_clock::now() + medium_wait)
              .get();
        ASSERT_FALSE(move_err)
          << "failed to move partition with error_code " << move_err;

        // spin until theres no move in progress
        auto& topics_table
          = controller_rp->app.controller->get_topics_state().local();
        wait_for(medium_wait, [&topics_table, to_move_ntp] {
            return !topics_table.is_update_in_progress(to_move_ntp);
        });
    }

    // execute nodewise recovery, wait for completion
    void execute_nodewise_recovery() {
        scope_logger sl{"nodewise recovery"};
        auto* controller_rp = safe_get_controller_rp();

        auto& topic_frontend
          = controller_rp->app.controller->get_topics_frontend().local();
        auto maybe_pwlm = topic_frontend
                            .partitions_with_lost_majority(get_dead_nodes())
                            .get();
        ASSERT_TRUE(maybe_pwlm.has_value())
          << "get partitions_with_lost_majority failed with error: "
          << maybe_pwlm.error().message();

        auto pwlm = std::move(maybe_pwlm).assume_value();

        for (const auto& entry : pwlm) {
            vlog(
              _logger.info, "executing nodewise recovery on entry: {}", entry);
        }

        auto force_error = topic_frontend
                             .force_recover_partitions_from_nodes(
                               get_dead_nodes(),
                               std::move(pwlm),
                               ss::lowres_clock::now() + large_wait)
                             .get();

        ASSERT_FALSE(force_error)
          << "failed to execute force recovery with error "
          << force_error.message();

        auto& topic_table
          = controller_rp->app.controller->get_topics_state().local();

        wait_for(large_wait, [&topic_table] {
            return topic_table.updates_in_progress().size() == 0;
        });
    }

    void decommission_dead_nodes() {
        scope_logger sl{"decommission dead nodes"};

        auto* controller_rp = safe_get_controller_rp();

        auto& members_frontend
          = controller_rp->app.controller->get_members_frontend().local();

        for (auto dead_node : get_dead_nodes()) {
            auto decom_err
              = members_frontend.decommission_node(dead_node).get();
            ASSERT_FALSE(decom_err) << "failed to decommission node "
                                    << dead_node << " with error " << decom_err;
        }

        auto& members_table
          = controller_rp->app.controller->get_members_table().local();

        wait_for(large_wait, [&members_table, dead_nodes = get_dead_nodes()] {
            for (const auto dead_node : dead_nodes) {
                auto dead_meta = members_table.get_node_metadata(dead_node);
                if (dead_meta) {
                    vlog(
                      _logger.info,
                      "still has metadata for dead node: {}",
                      dead_node);
                    return false;
                }
            }
            vlog(_logger.info, "successfully removed dead_nodes");
            return true;
        });
    }

    void check_final_replica_locations() {
        // given a cluster size of N, check that all nodes
        // [0, floor(N/2)] are actually gone (killed to lose quorum)
        // and that [floor(N/2) + 1, N + floor(N/2) + 1] are alive
        auto nodes = this->instance_ids();

        ASSERT_EQ(nodes.size(), _cluster_size);

        // original of 5, total created nodes -> 5 + (5/2+1) = 5+3 = 8
        const auto survivors_start = (_cluster_size / 2 + 1);
        auto total_created_nodes = _cluster_size + survivors_start;
        for (int node_number{0}; node_number < survivors_start; ++node_number) {
            ASSERT_TRUE(
              std::ranges::find(nodes, model::node_id{node_number})
              == nodes.end());
        }
        for (int node_number{survivors_start};
             node_number < total_created_nodes;
             ++node_number) {
            ASSERT_TRUE(
              std::ranges::find(nodes, model::node_id{node_number})
              != nodes.end());
        }

        // check that the metadata on all members agrees
        wait_for_all_members(small_wait).get();
    }

private:
    const uint16_t _cluster_size;
    std::set<model::node_id> _dead_nodes;
    std::set<model::node_id> _living_nodes;
};

template<uint16_t Size>
class SizedCFRFixture : public CFRFixture {
public:
    SizedCFRFixture() noexcept
      : CFRFixture(Size) {}

    void do_smoke_test() {
        // set up
        setup_topic(default_topic);
        move_partition_onto_dying_nodes(default_topic);

        // disaster
        induce_controller_loss();
        toggle_recovery_mode(true);
        check_no_controller_leader();

        // recovery
        force_recover_cluster();
        check_controller_leader();
        restore_node_number();
        check_controller_leader();
        toggle_recovery_mode(false);
        check_controller_leader();
        execute_nodewise_recovery();
        decommission_dead_nodes();

        // check recovery succeeded
        wait_for_leader_restoration(default_topic);
        check_final_replica_locations();
    }
};

} // namespace

TEST(CalculateRaftGroup, TwoSuvivors) {
    std::vector<raft::vnode> voter_config = {
      to_vnode(0), to_vnode(1), to_vnode(2), to_vnode(3), to_vnode(4)};

    std::vector<model::node_id> dead_nodes = {
      model::node_id{0}, model::node_id{1}, model::node_id{2}};

    auto maybe_result_config
      = cluster::controller_forced_reconfiguration_manager::
        calculation_reconfiguration_group(dead_nodes, voter_config, 2);
    ASSERT_TRUE(maybe_result_config.has_value());

    auto result_config = std::move(maybe_result_config).value();

    std::vector<raft::vnode> expected = {to_vnode(3), to_vnode(4), to_vnode(0)};

    ASSERT_EQ(result_config.size(), expected.size());
    for (const auto& result_node : result_config) {
        ASSERT_TRUE(std::ranges::find(expected, result_node) != expected.end());
    }
}

TEST(CalculateRaftGroup, EvenClusterSize) {
    std::vector<raft::vnode> voter_config = {
      to_vnode(0),
      to_vnode(1),
      to_vnode(2),
      to_vnode(3),
      to_vnode(4),
      to_vnode(5)};

    std::vector<model::node_id> dead_nodes = {
      model::node_id{0}, model::node_id{1}, model::node_id{2}};

    auto maybe_result_config
      = cluster::controller_forced_reconfiguration_manager::
        calculation_reconfiguration_group(dead_nodes, voter_config, 2);
    ASSERT_TRUE(maybe_result_config.has_value());

    auto result_config = std::move(maybe_result_config).value();

    std::vector<raft::vnode> expected = {to_vnode(3), to_vnode(4), to_vnode(5)};

    ASSERT_EQ(result_config.size(), expected.size());
    for (const auto& result_node : result_config) {
        ASSERT_TRUE(std::ranges::find(expected, result_node) != expected.end());
    }
}

TEST(CalculateRaftGroup, NoSurvivors) {
    std::vector<raft::vnode> voter_config = {
      to_vnode(0), to_vnode(1), to_vnode(2), to_vnode(3), to_vnode(4)};

    std::vector<model::node_id> dead_nodes = {
      model::node_id{0},
      model::node_id{1},
      model::node_id{2},
      model::node_id{3},
      model::node_id{4}};

    auto maybe_result_config
      = cluster::controller_forced_reconfiguration_manager::
        calculation_reconfiguration_group(dead_nodes, voter_config, 2);
    ASSERT_FALSE(maybe_result_config.has_value());
    ASSERT_EQ(maybe_result_config.error().err, cluster::errc::invalid_request);
}

TEST(CalculateRaftGroup, NotEnoughNodes) {
    std::vector<raft::vnode> voter_config = {
      to_vnode(0), to_vnode(1), to_vnode(2), to_vnode(3), to_vnode(4)};

    std::vector<model::node_id> dead_nodes = {
      model::node_id{0},
      model::node_id{1},
      model::node_id{2},
      model::node_id{3},
      model::node_id{4}};

    auto maybe_result_config
      = cluster::controller_forced_reconfiguration_manager::
        calculation_reconfiguration_group(dead_nodes, voter_config, 100);
    ASSERT_FALSE(maybe_result_config.has_value());
    ASSERT_EQ(maybe_result_config.error().err, cluster::errc::invalid_request);
}

using CFRFixture5 = SizedCFRFixture<5>;
TEST_F(CFRFixture5, Smoke5) { do_smoke_test(); }

using CFRFixture3 = SizedCFRFixture<3>;
TEST_F(CFRFixture3, Smoke3) { do_smoke_test(); }

using CFRFixture4 = SizedCFRFixture<4>;
TEST_F(CFRFixture4, Smoke4) { do_smoke_test(); }

} // namespace
