/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/coordinator/coordinator.h"
#include "datalake/coordinator/state_machine.h"
#include "datalake/coordinator/tests/state_test_utils.h"
#include "datalake/logger.h"
#include "raft/tests/raft_fixture.h"
#include "random/generators.h"
#include "test_utils/async.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

#include <functional>

using namespace datalake::coordinator;

namespace {
const model::topic topic_base{"test_topic"};
model::topic_partition tp(int t, int pid) {
    return model::topic_partition(
      model::topic{fmt::format("{}_{}", topic_base, t)},
      model::partition_id(pid));
}
struct coordinator_node {
    explicit coordinator_node(ss::shared_ptr<coordinator_stm> stm)
      : stm(*stm)
      , crd(stm) {}

    coordinator_stm& stm;
    coordinator crd;
};

ss::future<> random_sleep_ms(int max_ms) {
    co_await ss::sleep(random_generators::get_int(max_ms) * 1ms);
}

// Simulates a data translator making their way through translated files,
// sending them to the coordinator in order, and resyncing with the coordinator
// on error.
using pairs_t = std::vector<std::pair<int64_t, int64_t>>;
ss::future<> file_adder_loop(
  const pairs_t& files,
  const model::topic_partition& tp,
  coordinator_node& n,
  int fiber_id,
  bool& done) {
    constexpr auto max_files_at_once = 10;
    auto last_offset = files.back().second;
    auto id = fmt::format(
      "adder {} node {}", fiber_id, n.stm.raft()->self().id());
    while (!done) {
        co_await random_sleep_ms(100);
        vlog(datalake::datalake_log.debug, "[{}] getting last added", id);
        auto last_res = co_await n.crd.sync_get_last_added_offset(tp);
        if (last_res.has_error()) {
            continue;
        }
        auto cur_last_opt = last_res.value();
        while (true) {
            co_await random_sleep_ms(100);
            if (cur_last_opt && cur_last_opt.value()() == last_offset) {
                done = true;
                // If the coordinator says we've hit the last offset,
                // everyone is done!
                co_return;
            }
            auto iter = files.begin();
            if (cur_last_opt.has_value()) {
                // If we've previously sent some state, figure out where to
                // begin next.
                auto cur_iter = std::ranges::find(
                  files,
                  cur_last_opt.value()(),
                  &std::pair<int64_t, int64_t>::second);
                ASSERT_FALSE_CORO(cur_iter == files.end())
                  << fmt::format("Couldn't locate iterator: {}", cur_last_opt);
                // Start sending the file _after_ the one containing the
                // last added file.
                iter = ++cur_iter;
            }
            // Collect the next N files to send.
            size_t num_files = random_generators::get_int(1, max_files_at_once);
            pairs_t files_to_send;
            while (iter != files.end() && files_to_send.size() < num_files) {
                files_to_send.push_back(*(iter++));
            }
            // Send the files.
            vlog(
              datalake::datalake_log.debug,
              "[{}] adding {} files starting at {}",
              id,
              files_to_send.size(),
              files_to_send.begin()->first);
            auto add_res = co_await n.crd.sync_add_files(
              tp, make_pending_files(files_to_send));
            if (add_res.has_error()) {
                // Leave this inner loop on error so we can refetch.
                break;
            }
            cur_last_opt = kafka::offset(files_to_send.back().second);
        }
    }
}
} // namespace

class CoordinatorTest : public raft::raft_fixture {
public:
    static constexpr auto num_nodes = 3;
    void SetUp() override {
        raft::raft_fixture::SetUpAsync().get();
        for (auto i = 0; i < num_nodes; ++i) {
            add_node(model::node_id(i), model::revision_id(0));
        }
        for (auto& [id, node] : nodes()) {
            node->initialise(all_vnodes()).get();
            auto* raft = node->raft().get();
            raft::state_machine_manager_builder builder;
            auto stm = builder.create_stm<coordinator_stm>(
              datalake::datalake_log, raft);
            node->start(std::move(builder)).get();
            crds.at(id()) = std::make_unique<coordinator_node>(std::move(stm));
        }
    }
    // Returns the coordinator on the current leader.
    using opt_ref = std::optional<std::reference_wrapper<coordinator_node>>;
    opt_ref leader_node() {
        auto leader_id = get_leader();
        if (!leader_id.has_value()) {
            return std::nullopt;
        }
        auto& node = *crds.at(leader_id.value()());
        if (!node.stm.raft()->is_leader()) {
            return std::nullopt;
        }
        return node;
    }

    // Waits for a stable leader to be elected, and returns it.
    ss::future<> wait_for_leader(opt_ref& leader) {
        RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [&] {
            leader = leader_node();
            return leader.has_value();
        });
    }

    // Waits for all nodes to have applied the current committed offset.
    ss::future<> wait_for_apply() {
        auto committed_offset = co_await with_leader(
          10s, [](auto& node) { return node.raft()->committed_offset(); });

        co_await parallel_for_each_node([committed_offset](auto& node) {
            return node.raft()->stm_manager()->wait(
              committed_offset, model::no_timeout);
        });
    }

    // Repeatedly forces leadership to step down on the current leader. Waits
    // before doing so to ensure progress is made for the given partition.
    ss::future<> transfer_leaders_loop(model::topic_partition tp, bool& done) {
        size_t last_num_entries = 0;
        while (!done) {
            co_await ss::sleep(10ms);
            opt_ref leader_opt;
            ASSERT_NO_FATAL_FAILURE_CORO(co_await wait_for_leader(leader_opt));
            auto& leader_stm = leader_opt->get().stm;
            auto prt_state = leader_stm.state().partition_state(tp);
            auto num_entries = prt_state
                                 ? prt_state->get().pending_entries.size()
                                 : 0;
            if (last_num_entries == num_entries) {
                // Allow for progress.
                continue;
            }
            last_num_entries = num_entries;
            auto* leader_raft = leader_stm.raft();
            co_await leader_raft->step_down("test");
        }
    }
    std::array<std::unique_ptr<coordinator_node>, num_nodes> crds;
};

TEST_F(CoordinatorTest, TestAddFilesHappyPath) {
    opt_ref leader_opt;
    ASSERT_NO_FATAL_FAILURE(wait_for_leader(leader_opt).get());
    auto& leader = leader_opt->get();
    const auto tp00 = tp(0, 0);
    const auto tp01 = tp(0, 1);
    const auto tp10 = tp(1, 0);
    pairs_t total_expected_00;
    for (const auto& v : {
           pairs_t{{0, 100}},
           pairs_t{{101, 200}},
           pairs_t{{201, 300}, {301, 400}},
           pairs_t{{401, 500}, {501, 600}},
         }) {
        auto add_res
          = leader.crd.sync_add_files(tp00, make_pending_files(v)).get();
        ASSERT_FALSE(add_res.has_error()) << add_res.error();
        wait_for_apply().get();

        // Collect the full list of offset ranges to compare against.
        total_expected_00.insert(total_expected_00.end(), v.begin(), v.end());
        for (const auto& c : crds) {
            ASSERT_NO_FATAL_FAILURE(check_partition(
              c->stm.state(), tp00, std::nullopt, total_expected_00));
        }
    }
    // Now try adding to a different partition of the same topic.
    pairs_t total_expected_01;
    for (const auto& v : {pairs_t{{0, 100}}, pairs_t{{101, 200}}}) {
        auto add_res
          = leader.crd.sync_add_files(tp01, make_pending_files(v)).get();
        ASSERT_FALSE(add_res.has_error()) << add_res.error();
        wait_for_apply().get();

        total_expected_01.insert(total_expected_01.end(), v.begin(), v.end());
        for (const auto& c : crds) {
            ASSERT_NO_FATAL_FAILURE(check_partition(
              c->stm.state(), tp01, std::nullopt, total_expected_01));
            ASSERT_NO_FATAL_FAILURE(check_partition(
              c->stm.state(), tp00, std::nullopt, total_expected_00));
        }
    }
    // And finally a different topic entirely.
    pairs_t total_expected_10;
    for (const auto& v : {pairs_t{{100, 200}}, pairs_t{{201, 300}}}) {
        auto add_res
          = leader.crd.sync_add_files(tp10, make_pending_files(v)).get();
        ASSERT_FALSE(add_res.has_error()) << add_res.error();
        wait_for_apply().get();

        total_expected_10.insert(total_expected_10.end(), v.begin(), v.end());
        for (const auto& c : crds) {
            ASSERT_NO_FATAL_FAILURE(check_partition(
              c->stm.state(), tp10, std::nullopt, total_expected_10));
            ASSERT_NO_FATAL_FAILURE(check_partition(
              c->stm.state(), tp01, std::nullopt, total_expected_01));
            ASSERT_NO_FATAL_FAILURE(check_partition(
              c->stm.state(), tp00, std::nullopt, total_expected_00));
        }
    }
}

TEST_F(CoordinatorTest, TestLastAddedHappyPath) {
    opt_ref leader_opt;
    ASSERT_NO_FATAL_FAILURE(wait_for_leader(leader_opt).get());
    auto& leader = leader_opt->get();
    const auto tp00 = tp(0, 0);
    const auto tp01 = tp(0, 1);
    pairs_t total_expected_00;
    for (const auto& v :
         {pairs_t{{101, 200}}, pairs_t{{201, 300}, {301, 400}}}) {
        auto add_res
          = leader.crd.sync_add_files(tp00, make_pending_files(v)).get();
        ASSERT_FALSE(add_res.has_error()) << add_res.error();
    }

    auto last_res = leader.crd.sync_get_last_added_offset(tp00).get();
    ASSERT_FALSE(last_res.has_error()) << last_res.error();
    ASSERT_TRUE(last_res.value().has_value());
    ASSERT_EQ(400, last_res.value().value()());

    last_res = leader.crd.sync_get_last_added_offset(tp01).get();
    ASSERT_FALSE(last_res.has_error()) << last_res.error();
    ASSERT_FALSE(last_res.value().has_value());
}

TEST_F(CoordinatorTest, TestNotLeader) {
    opt_ref leader_opt;
    ASSERT_NO_FATAL_FAILURE(wait_for_leader(leader_opt).get());
    opt_ref non_leader_opt;
    for (const auto& c : crds) {
        if (c.get() != &leader_opt.value().get()) {
            non_leader_opt = *c;
            break;
        }
    }
    ASSERT_TRUE(non_leader_opt.has_value());
    auto& non_leader = non_leader_opt->get();
    const auto tp00 = tp(0, 0);
    pairs_t total_expected_00;

    auto add_res = non_leader.crd
                     .sync_add_files(tp00, make_pending_files({{0, 100}}))
                     .get();
    ASSERT_TRUE(add_res.has_error());
    EXPECT_EQ(coordinator::errc::not_leader, add_res.error());

    auto last_res = non_leader.crd.sync_get_last_added_offset(tp00).get();
    ASSERT_TRUE(last_res.has_error()) << last_res.error();
    EXPECT_EQ(coordinator::errc::not_leader, last_res.error());
}

class CoordinatorTestChaosParam
  : public CoordinatorTest
  , public ::testing::WithParamInterface<bool> {};

TEST_P(CoordinatorTestChaosParam, TestConcurrentAddFiles) {
    constexpr auto num_files = 200;
    constexpr auto offsets_per_file = 10;
    constexpr auto num_adders_per_node = 3;
    pairs_t files;
    files.reserve(num_files);
    for (size_t i = 0; i < num_files; ++i) {
        int64_t cur_start = i * offsets_per_file;
        int64_t next_start = (i + 1) * offsets_per_file;
        files.emplace_back(
          std::pair<int64_t, int64_t>{cur_start, next_start - 1});
    }
    const auto tp00 = tp(0, 0);
    bool done = false;
    std::vector<ss::future<>> adders;
    int fiber_id = 0;

    // Stress the system a bit by adding multiple add fibers per coordinator
    // replica. This helps exercise when there are races across leadership
    // changes, and requests get sent to stale leaders, etc.
    for (auto& n : crds) {
        for (size_t i = 0; i < num_adders_per_node; i++) {
            adders.push_back(
              file_adder_loop(files, tp00, *n, fiber_id++, done));
        }
    }
    std::optional<ss::future<>> chaos;
    if (GetParam()) {
        chaos = transfer_leaders_loop(tp00, done);
    }
    auto stop = ss::defer([&] {
        done = true;
        for (auto& f : adders) {
            f.get();
        }
        if (chaos) {
            chaos->get();
        }
    });
    RPTEST_REQUIRE_EVENTUALLY(60s, [&done] { return done; });
    for (auto& f : adders) {
        EXPECT_NO_FATAL_FAILURE(f.get());
    }
    if (chaos) {
        EXPECT_NO_FATAL_FAILURE(chaos->get());
    }
    stop.cancel();
    wait_for_apply().get();
    for (const auto& c : crds) {
        ASSERT_NO_FATAL_FAILURE(
          check_partition(c->stm.state(), tp00, std::nullopt, files));
    }
}
INSTANTIATE_TEST_SUITE_P(
  WithChaos, CoordinatorTestChaosParam, ::testing::Bool());
