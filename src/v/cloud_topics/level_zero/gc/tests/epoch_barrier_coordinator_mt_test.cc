/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

/*
 * Multithreaded tests for the epoch barrier coordinator.
 *
 * These tests run the coordinator on multiple real Seastar shards to verify
 * that cross-shard aggregation in poll_drain, invalidate, and
 * publish_safe_epoch works correctly.
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
#include <seastar/core/smp.hh>

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

    ss::future<> drain_inflight_writes() override { return ss::now(); }

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

/// Per-shard mock partition source pointers, indexed by shard ID.
/// Populated during coordinator construction.
std::vector<mock_partition_source*> g_per_shard_ps;

struct coordinator_mt_test : public seastar_test {
    ss::sharded<cluster::cluster_epoch_service<ss::lowres_clock>> epoch_svc;
    ss::sharded<epoch_barrier_coordinator> coordinator;
    mock_data_plane data_plane;

    ss::future<> SetUpAsync() override {
        vassert(ss::smp::count >= 2, "Need at least 2 shards");

        g_per_shard_ps.clear();
        g_per_shard_ps.resize(ss::smp::count, nullptr);

        co_await epoch_svc.start(
          [](ss::abort_source*)
            -> ss::future<std::expected<int64_t, std::error_code>> {
              co_return int64_t{1};
          });
        co_await epoch_svc.invoke_on_all(
          &cluster::cluster_epoch_service<ss::lowres_clock>::start);

        co_await coordinator.start(
          std::ref(epoch_svc), std::ref(data_plane), ss::sharded_parameter([] {
              auto p = std::make_unique<mock_partition_source>();
              g_per_shard_ps[ss::this_shard_id()] = p.get();
              return p;
          }));
        co_await coordinator.invoke_on_all(&epoch_barrier_coordinator::start);
    }

    ss::future<> TearDownAsync() override {
        co_await coordinator.stop();
        co_await epoch_svc.stop();
        g_per_shard_ps.clear();
    }

    /// Add a partition on a specific shard's mock partition source.
    ss::future<> add_partition_on_shard(
      ss::shard_id shard,
      const ss::sstring& topic,
      model::partition_id::type pid,
      model::offset committed,
      model::term_id term,
      std::optional<model::offset> lro = std::nullopt) {
        co_await ss::smp::submit_to(shard, [&, committed, term, lro, pid] {
            g_per_shard_ps[ss::this_shard_id()]
              ->partitions[make_ntp(topic, pid)]
              = mock_partition_source::partition_state{
                .committed_offset = committed,
                .term = term,
                .is_leader = true,
                .last_reconciled_log_offset = lro,
              };
        });
    }

    /// Set LRO for a partition on a specific shard.
    ss::future<> set_lro_on_shard(
      ss::shard_id shard,
      const ss::sstring& topic,
      model::partition_id::type pid,
      model::offset lro) {
        co_await ss::smp::submit_to(shard, [&, pid, lro] {
            auto& state = g_per_shard_ps[ss::this_shard_id()]
                            ->partitions[make_ntp(topic, pid)];
            state.last_reconciled_log_offset = lro;
        });
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

// -- Cross-shard tests -------------------------------------------------------

TEST_F_CORO(coordinator_mt_test, cross_shard_all_must_reconcile) {
    // Shard 0: partition not caught up
    co_await add_partition_on_shard(
      0, "t0", 0, model::offset(100), model::term_id(1));
    // Shard 1: partition caught up
    co_await add_partition_on_shard(
      1, "t0", 1, model::offset(200), model::term_id(1), model::offset(200));

    auto result = co_await invalidate_and_poll(cluster_epoch(5));
    EXPECT_FALSE(result) << "Shard 0 should block completion";

    // Advance shard 0's LRO
    co_await set_lro_on_shard(0, "t0", 0, model::offset(100));

    result = co_await poll(cluster_epoch(5));
    EXPECT_TRUE(result);
}

TEST_F_CORO(coordinator_mt_test, cross_shard_invalidate_resets_all) {
    // Put partitions on different shards, all caught up
    co_await add_partition_on_shard(
      0, "t0", 0, model::offset(100), model::term_id(1), model::offset(100));
    co_await add_partition_on_shard(
      1, "t0", 1, model::offset(200), model::term_id(1), model::offset(200));

    // Complete a round
    auto result = co_await invalidate_and_poll(cluster_epoch(5));
    EXPECT_TRUE(result);

    // Advance committed on shard 0 so a new round will require catching up
    co_await add_partition_on_shard(
      0, "t0", 0, model::offset(300), model::term_id(1));

    // New round — invalidate should reset all shards
    result = co_await invalidate_and_poll(cluster_epoch(6));
    EXPECT_FALSE(result) << "Shard 0 has new seal at 300, no LRO yet";

    // Catch up shard 0
    co_await set_lro_on_shard(0, "t0", 0, model::offset(300));

    result = co_await poll(cluster_epoch(6));
    EXPECT_TRUE(result);
}

TEST_F_CORO(coordinator_mt_test, cross_shard_publish_safe_epoch) {
    auto inv = co_await coordinator.local().publish_safe_epoch(
      cluster_epoch(10));
    EXPECT_TRUE(inv.has_value());

    // Verify safe_epoch is visible on all shards
    auto check = co_await coordinator.map_reduce0(
      [](epoch_barrier_coordinator& c) -> bool {
          auto safe = c.safe_epoch();
          return safe.has_value() && *safe == cluster_epoch(10);
      },
      true,
      std::logical_and<>{});

    EXPECT_TRUE(check) << "safe_epoch should be 10 on all shards";
}
