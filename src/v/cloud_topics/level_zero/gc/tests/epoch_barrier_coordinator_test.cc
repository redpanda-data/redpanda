/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/data_plane_api.h"
#include "cloud_topics/level_zero/gc/epoch_barrier_coordinator.h"
#include "cloud_topics/types.h"
#include "cluster/cluster_epoch_service.h"
#include "container/chunked_hash_map.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "test_utils/test.h"

#include <seastar/core/sharded.hh>

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <system_error>

using namespace cloud_topics;
using namespace cloud_topics::l0::gc;

namespace {

// -- Mock partition source ---------------------------------------------------

using pinfo = epoch_barrier_coordinator::partition_source::info;

class mock_partition_source
  : public epoch_barrier_coordinator::partition_source {
public:
    struct partition_state {
        model::offset committed_offset;
        model::term_id term;
        bool is_leader{true};
        std::optional<model::offset> last_reconciled_log_offset;
    };

    chunked_hash_map<model::ntp, partition_state> partitions;

    chunked_vector<std::pair<model::ntp, info>>
    cloud_topic_partitions() const override {
        chunked_vector<std::pair<model::ntp, info>> result;
        for (const auto& [ntp, state] : partitions) {
            result.emplace_back(
              ntp,
              info{
                .committed_offset = state.committed_offset,
                .term = state.term,
                .is_leader = state.is_leader,
                .last_reconciled_log_offset = state.last_reconciled_log_offset,
              });
        }
        return result;
    }

    std::optional<info> get(const model::ntp& ntp) const override {
        auto it = partitions.find(ntp);
        if (it == partitions.end()) {
            return std::nullopt;
        }
        const auto& state = it->second;
        return info{
          .committed_offset = state.committed_offset,
          .term = state.term,
          .is_leader = state.is_leader,
          .last_reconciled_log_offset = state.last_reconciled_log_offset,
        };
    }
};

// -- Mock data plane ---------------------------------------------------------

class mock_data_plane : public data_plane_api {
public:
    int drain_count{0};

    ss::future<> start() override { return ss::now(); }
    ss::future<> stop() override { return ss::now(); }

    ss::future<std::expected<staged_write, std::error_code>>
    stage_write(chunked_vector<model::record_batch>) override {
        throw std::logic_error("not implemented");
    }

    ss::future<std::expected<chunked_vector<extent_meta>, std::error_code>>
    execute_write(
      model::ntp,
      cluster_epoch,
      staged_write,
      model::timeout_clock::time_point) override {
        throw std::logic_error("not implemented");
    }

    ss::future<result<chunked_vector<model::record_batch>>> materialize(
      model::ntp,
      size_t,
      chunked_vector<extent_meta>,
      model::timeout_clock::time_point,
      model::opt_abort_source_t,
      allow_materialization_failure) override {
        throw std::logic_error("not implemented");
    }

    size_t materialize_max_bytes() const override { return 0; }

    void cache_put(
      const model::topic_id_partition&, const model::record_batch&) override {}

    std::optional<model::record_batch>
    cache_get(const model::topic_id_partition&, model::offset) override {
        return std::nullopt;
    }

    void cache_put_ordered(
      const model::topic_id_partition&,
      chunked_vector<model::record_batch>) override {}

    std::unique_ptr<inflight_write_token> track_inflight_write() override {
        return std::make_unique<inflight_write_token>();
    }

    ss::future<> drain_inflight_writes() override {
        ++drain_count;
        return ss::now();
    }

    ss::future<std::optional<cloud_topics::cluster_epoch>>
    get_current_epoch(ss::abort_source*) override {
        throw std::logic_error("not implemented");
    }

    ss::future<> cache_wait(
      const model::topic_id_partition&,
      model::offset,
      model::offset,
      model::timeout_clock::time_point,
      std::optional<std::reference_wrapper<ss::abort_source>>) override {
        co_return;
    }
};

// -- Helper to build NTPs ----------------------------------------------------

model::ntp make_ntp(const ss::sstring& topic, model::partition_id::type pid) {
    return model::ntp(
      model::kafka_namespace, model::topic(topic), model::partition_id(pid));
}

// -- Fixture -----------------------------------------------------------------

struct coordinator_test : public seastar_test {
    ss::sharded<cluster::cluster_epoch_service<ss::lowres_clock>> epoch_svc;
    ss::sharded<epoch_barrier_coordinator> coordinator;
    mock_data_plane data_plane;
    mock_partition_source* ps{nullptr};

    ss::future<> SetUpAsync() override {
        co_await epoch_svc.start(
          [](ss::abort_source*)
            -> ss::future<std::expected<int64_t, std::error_code>> {
              co_return int64_t{1};
          });
        co_await epoch_svc.invoke_on_all(
          &cluster::cluster_epoch_service<ss::lowres_clock>::start);

        co_await coordinator.start(
          std::ref(epoch_svc),
          std::ref(data_plane),
          ss::sharded_parameter([this] {
              auto p = std::make_unique<mock_partition_source>();
              ps = p.get();
              return p;
          }));
        co_await coordinator.invoke_on_all(&epoch_barrier_coordinator::start);
    }

    ss::future<> TearDownAsync() override {
        co_await coordinator.stop();
        co_await epoch_svc.stop();
    }

    void add_partition(
      const ss::sstring& topic,
      model::partition_id::type pid,
      model::offset committed,
      model::term_id term,
      std::optional<model::offset> lro = std::nullopt) {
        ps->partitions[make_ntp(topic, pid)]
          = mock_partition_source::partition_state{
            .committed_offset = committed,
            .term = term,
            .is_leader = true,
            .last_reconciled_log_offset = lro,
          };
    }

    void set_lro(
      const ss::sstring& topic,
      model::partition_id::type pid,
      model::offset lro) {
        auto it = ps->partitions.find(make_ntp(topic, pid));
        vassert(it != ps->partitions.end(), "partition not found");
        it->second.last_reconciled_log_offset = lro;
    }

    void set_term(
      const ss::sstring& topic,
      model::partition_id::type pid,
      model::term_id term) {
        auto it = ps->partitions.find(make_ntp(topic, pid));
        vassert(it != ps->partitions.end(), "partition not found");
        it->second.term = term;
    }

    void set_committed(
      const ss::sstring& topic,
      model::partition_id::type pid,
      model::offset committed) {
        auto it = ps->partitions.find(make_ntp(topic, pid));
        vassert(it != ps->partitions.end(), "partition not found");
        it->second.committed_offset = committed;
    }

    void set_leader(
      const ss::sstring& topic, model::partition_id::type pid, bool is_leader) {
        auto it = ps->partitions.find(make_ntp(topic, pid));
        vassert(it != ps->partitions.end(), "partition not found");
        it->second.is_leader = is_leader;
    }

    void
    remove_partition(const ss::sstring& topic, model::partition_id::type pid) {
        ps->partitions.erase(make_ntp(topic, pid));
    }

    ss::future<bool> invalidate_and_poll(cluster_epoch e) {
        auto inv = co_await coordinator.local().invalidate(e);
        EXPECT_TRUE(inv.has_value());
        auto result = co_await coordinator.local().poll_drain(e);
        EXPECT_TRUE(result.has_value());
        co_return *result;
    }

    ss::future<bool> poll(cluster_epoch e) {
        auto result = co_await coordinator.local().poll_drain(e);
        EXPECT_TRUE(result.has_value());
        co_return *result;
    }
};

} // namespace

// -- Data loss prevention tests ----------------------------------------------

TEST_F_CORO(coordinator_test, poll_drain_false_when_lro_not_caught_up) {
    // Partition committed=100, term=1, lro=nullopt
    add_partition("t0", 0, model::offset(100), model::term_id(1));

    auto result = co_await invalidate_and_poll(cluster_epoch(5));
    EXPECT_FALSE(result);
}

TEST_F_CORO(coordinator_test, seal_points_captured_at_drain_time) {
    // Partition committed=100 at drain time
    add_partition("t0", 0, model::offset(100), model::term_id(1));

    auto result = co_await invalidate_and_poll(cluster_epoch(5));
    EXPECT_FALSE(result);

    // Simulate more writes arriving after drain — committed advances to 200
    set_committed("t0", 0, model::offset(200));

    // LRO catches up to the original seal point (100), not 200
    set_lro("t0", 0, model::offset(100));

    result = co_await poll(cluster_epoch(5));
    EXPECT_TRUE(result) << "Should complete because LRO >= seal (100), "
                           "not current committed (200)";
}

TEST_F_CORO(coordinator_test, term_mismatch_refreshes_seal_forward) {
    // Partition committed=100, term=1
    add_partition("t0", 0, model::offset(100), model::term_id(1));

    auto result = co_await invalidate_and_poll(cluster_epoch(5));
    EXPECT_FALSE(result);

    // Term changes to 2, committed advances to 150
    set_term("t0", 0, model::term_id(2));
    set_committed("t0", 0, model::offset(150));

    // Seal refreshes — the old lro=100 won't be sufficient
    result = co_await poll(cluster_epoch(5));
    EXPECT_FALSE(result) << "Seal should have refreshed to 150";

    // Now LRO catches up to the new seal point
    set_lro("t0", 0, model::offset(150));

    result = co_await poll(cluster_epoch(5));
    EXPECT_TRUE(result);
}

TEST_F_CORO(coordinator_test, new_leaders_get_seal_points) {
    // Only t0/0 is a leader initially
    add_partition("t0", 0, model::offset(100), model::term_id(1));

    auto result = co_await invalidate_and_poll(cluster_epoch(5));
    EXPECT_FALSE(result);

    // New leader appears on t0/1
    add_partition("t0", 1, model::offset(50), model::term_id(1));

    // t0/0 catches up, but t0/1 is discovered as a new leader
    set_lro("t0", 0, model::offset(100));

    result = co_await poll(cluster_epoch(5));
    EXPECT_FALSE(result) << "New leader t0/1 should block completion";

    // t0/1 catches up
    set_lro("t0", 1, model::offset(50));

    result = co_await poll(cluster_epoch(5));
    EXPECT_TRUE(result);
}

// -- Resilience to leadership/term changes -----------------------------------

TEST_F_CORO(coordinator_test, lost_leadership_resets_and_redrains) {
    add_partition("t0", 0, model::offset(100), model::term_id(1));
    add_partition("t0", 1, model::offset(200), model::term_id(1));

    auto result = co_await invalidate_and_poll(cluster_epoch(5));
    EXPECT_FALSE(result);
    EXPECT_EQ(data_plane.drain_count, 1);

    // t0/0 loses leadership — seal table goes stale
    set_leader("t0", 0, false);
    set_lro("t0", 1, model::offset(200));

    result = co_await poll(cluster_epoch(5));
    EXPECT_FALSE(result) << "Stale seal table should return false";
    // Round was reset but no redrain yet (that happens on next poll)
    EXPECT_EQ(data_plane.drain_count, 1);

    // Next poll re-drains and builds a fresh seal table.
    // t0/0 is still not leader, so it won't be sealed this time.
    // t0/1 is caught up at 200 — should succeed.
    result = co_await poll(cluster_epoch(5));
    EXPECT_EQ(data_plane.drain_count, 2) << "Should have re-drained";
    EXPECT_TRUE(result) << "Fresh seal table without t0/0 should succeed";
}

TEST_F_CORO(coordinator_test, removed_partition_resets_and_redrains) {
    add_partition("t0", 0, model::offset(100), model::term_id(1));
    add_partition("t0", 1, model::offset(200), model::term_id(1));

    auto result = co_await invalidate_and_poll(cluster_epoch(5));
    EXPECT_FALSE(result);
    EXPECT_EQ(data_plane.drain_count, 1);

    // t0/0 is removed — seal table goes stale
    remove_partition("t0", 0);
    set_lro("t0", 1, model::offset(200));

    result = co_await poll(cluster_epoch(5));
    EXPECT_FALSE(result) << "Stale seal table should return false";

    // Next poll re-drains and builds fresh seal table without t0/0.
    result = co_await poll(cluster_epoch(5));
    EXPECT_EQ(data_plane.drain_count, 2) << "Should have re-drained";
    EXPECT_TRUE(result)
      << "Fresh seal table without removed partition should succeed";
}

TEST_F_CORO(coordinator_test, term_change_never_regresses_seal) {
    add_partition("t0", 0, model::offset(100), model::term_id(1));

    auto result = co_await invalidate_and_poll(cluster_epoch(5));
    EXPECT_FALSE(result);

    // Term bumps to 2, committed advances to 150
    set_term("t0", 0, model::term_id(2));
    set_committed("t0", 0, model::offset(150));

    result = co_await poll(cluster_epoch(5));
    EXPECT_FALSE(result) << "Seal should have refreshed forward to 150";

    // LRO at old committed (100) is not sufficient
    set_lro("t0", 0, model::offset(100));

    result = co_await poll(cluster_epoch(5));
    EXPECT_FALSE(result) << "LRO=100 < seal=150, should not complete";

    // LRO catches up to the refreshed seal
    set_lro("t0", 0, model::offset(150));

    result = co_await poll(cluster_epoch(5));
    EXPECT_TRUE(result);
}

// -- Making progress tests ---------------------------------------------------

TEST_F_CORO(coordinator_test, all_lros_caught_up_returns_true) {
    // All three partitions already caught up at drain time
    add_partition(
      "t0", 0, model::offset(100), model::term_id(1), model::offset(100));
    add_partition(
      "t0", 1, model::offset(200), model::term_id(1), model::offset(200));
    add_partition(
      "t1", 0, model::offset(50), model::term_id(1), model::offset(50));

    auto result = co_await invalidate_and_poll(cluster_epoch(5));
    EXPECT_TRUE(result) << "All LROs >= committed, should pass immediately";
}

TEST_F_CORO(coordinator_test, invalidate_resets_round_redrain) {
    add_partition("t0", 0, model::offset(100), model::term_id(1));

    // Complete a full round for epoch 5
    co_await invalidate_and_poll(cluster_epoch(5));
    set_lro("t0", 0, model::offset(100));
    auto result = co_await poll(cluster_epoch(5));
    EXPECT_TRUE(result);
    EXPECT_EQ(data_plane.drain_count, 1);

    // Committed advances
    set_committed("t0", 0, model::offset(200));

    // New round for epoch 6 — should re-drain
    result = co_await invalidate_and_poll(cluster_epoch(6));
    EXPECT_EQ(data_plane.drain_count, 2);
    EXPECT_FALSE(result);

    // New seal point should use current committed (200)
    set_lro("t0", 0, model::offset(200));
    result = co_await poll(cluster_epoch(6));
    EXPECT_TRUE(result);
}

TEST_F_CORO(coordinator_test, repeated_poll_drain_no_redrain) {
    add_partition("t0", 0, model::offset(100), model::term_id(1));

    co_await invalidate_and_poll(cluster_epoch(5));
    EXPECT_EQ(data_plane.drain_count, 1);

    // Second poll_drain for same epoch should NOT re-drain
    co_await poll(cluster_epoch(5));
    EXPECT_EQ(data_plane.drain_count, 1);

    // Third poll_drain for same epoch — still no re-drain
    co_await poll(cluster_epoch(5));
    EXPECT_EQ(data_plane.drain_count, 1);
}

TEST_F_CORO(coordinator_test, no_partitions_returns_true) {
    // No partitions at all — vacuously true
    auto result = co_await invalidate_and_poll(cluster_epoch(5));
    EXPECT_TRUE(result);
}

TEST_F_CORO(coordinator_test, publish_safe_epoch_stored) {
    auto inv = co_await coordinator.local().publish_safe_epoch(
      cluster_epoch(42));
    EXPECT_TRUE(inv.has_value());

    auto safe = coordinator.local().safe_epoch();
    ASSERT_TRUE_CORO(safe.has_value());
    EXPECT_EQ(*safe, cluster_epoch(42));
}
