// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"
#include "raft/tests/raft_fixture.h"
#include "test_utils/async.h"

#include <seastar/core/lowres_clock.hh>

using namespace raft;

namespace {

auto populate_waiters(
  ss::lw_shared_ptr<consensus> raft, bool write_caching, size_t waiters) {
    // populates waiters before replicating. They are not resolved until
    // the corresponding entries are successfully replicated.
    auto futures = std::vector<ss::future<errc>>();
    futures.reserve(waiters);
    auto& replication_monitor = raft->get_replication_monitor();
    auto log_offsets = raft->log()->offsets();
    auto base = log_offsets.dirty_offset;
    for (size_t i = 0; i < waiters; i++) {
        auto offset = base + model::offset_delta(i + 1);
        storage::append_result wait_for{
          .append_time = storage::log_clock::now(),
          .base_offset = offset,
          .last_offset = offset,
          .byte_size = 1234,
          .last_term = raft->term()};
        auto f = write_caching
                   ? replication_monitor.wait_until_majority_replicated(
                       wait_for)
                   : replication_monitor.wait_until_committed(wait_for);
        futures.push_back(std::move(f));
    }
    return ss::when_all_succeed(futures.begin(), futures.end());
}

class monitor_test_fixture
  : public raft_fixture
  , public ::testing::WithParamInterface<std::tuple<bool, size_t>> {
public:
    static bool write_caching() { return std::get<0>(GetParam()); }
    static size_t num_waiters() { return std::get<1>(GetParam()); }

    ss::future<> truncation_detection_test(raft_node_instance& leader) {
        auto raft = leader.raft();
        auto wait_futures = ::populate_waiters(
          raft, write_caching(), num_waiters());
        co_await ss::sleep(500ms);
        ASSERT_FALSE_CORO(wait_futures.available());

        for (auto& [id, node] : nodes()) {
            if (id == leader.get_vnode().id()) {
                node->on_dispatch(
                  [](model::node_id, raft::msg_type) { return ss::sleep(3s); });
            }
        }

        std::vector<ss::future<result<replicate_result>>> replicate_f;
        replicate_f.reserve(num_waiters());
        for (size_t i = 0; i < num_waiters(); i++) {
            replicate_f.push_back(raft->replicate(
              make_batches({{"k", "v"}}),
              replicate_options{raft::consistency_level::quorum_ack}));
        }
        auto repl_results = co_await ss::when_all(
          replicate_f.begin(), replicate_f.end());
        for (auto& r : repl_results) {
            auto res = r.get();
            ASSERT_TRUE_CORO(res.has_error());
            if (res.error() == errc::not_leader) {
                throw raft_not_leader_exception();
            }
            ASSERT_EQ_CORO(res.error(), errc::replicated_entry_truncated);
        }

        co_await tests::cooperative_spin_wait_with_timeout(2s, [&] {
            return wait_futures.available() && !wait_futures.failed();
        });

        auto wait_results = wait_futures.get();
        for (size_t i = 0; i < num_waiters(); i++) {
            if (wait_results.at(i) == errc::not_leader) {
                throw raft_not_leader_exception();
            }
            ASSERT_EQ_CORO(
              wait_results.at(i), errc::replicated_entry_truncated);
        }
    }
    ss::future<> replication_monitor_wait_test(raft_node_instance& leader) {
        auto raft = leader.raft();

        auto wait_futures = ::populate_waiters(
          raft, write_caching(), num_waiters());
        co_await ss::sleep(500ms);
        ASSERT_FALSE_CORO(wait_futures.available());

        for (size_t i = 0; i < num_waiters(); i++) {
            auto repl_result = co_await raft->replicate(
              make_batches({{"k", "v"}}),
              replicate_options{raft::consistency_level::quorum_ack});
            if (
              repl_result.has_error()
              && repl_result.error() == errc::not_leader) {
                throw raft_not_leader_exception();
            }
            ASSERT_TRUE_CORO(repl_result.has_value()) << repl_result.error();
        }

        co_await tests::cooperative_spin_wait_with_timeout(2s, [&] {
            return wait_futures.available() && !wait_futures.failed();
        });

        auto wait_results = wait_futures.get();
        for (size_t i = 0; i < num_waiters(); i++) {
            if (wait_results.at(i) == errc::not_leader) {
                throw raft_not_leader_exception();
            }
            ASSERT_EQ_CORO(wait_results.at(i), errc::success);
        }
    }
};
} // namespace

TEST_P_CORO(monitor_test_fixture, replication_monitor_wait) {
    co_await create_simple_group(5);

    co_await set_write_caching(write_caching());

    co_await test_with_leader(
      60s, &monitor_test_fixture::replication_monitor_wait_test);
}

TEST_P_CORO(monitor_test_fixture, truncation_detection) {
    set_enable_longest_log_detection(false);
    co_await create_simple_group(3);

    co_await set_write_caching(write_caching());

    co_await test_with_leader(
      60s, &monitor_test_fixture::truncation_detection_test);
}

INSTANTIATE_TEST_SUITE_P(
  replication_monitor_basic,
  monitor_test_fixture,
  testing::Combine(::testing::Bool(), ::testing::Values(1UL, 5UL)));
