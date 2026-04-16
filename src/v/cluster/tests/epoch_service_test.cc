// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/cluster_epoch_service.h"
#include "cluster/errc.h"
#include "config/configuration.h"
#include "gmock/gmock.h"
#include "ssx/abort_source.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

using epoch_service = cluster::cluster_epoch_service<ss::manual_clock>;

class ClusterEpochService : public seastar_test {
protected:
    ss::future<> SetUpAsync() override {
        co_await abort_source.start(parent_abort_source);
        co_await service.start(ss::sharded_parameter([this] {
            return [this](ss::abort_source*) { return get_leader_epoch(); };
        }));
        co_await service.invoke_on_all(&epoch_service::start);
    }
    ss::future<> TearDownAsync() override {
        co_await abort_source.stop();
        co_await service.stop();
    }

    ss::future<> reset_abort_source() {
        co_await abort_source.stop();
        co_await abort_source.start(parent_abort_source);
    }

    ss::future<std::expected<int64_t, std::error_code>> get_leader_epoch() {
        auto guard = co_await ss::smp::submit_to(0, [this] {
            return ss::get_shared_lock(_mutex).then([](auto guard) {
                return ss::make_foreign(ss::make_lw_shared(std::move(guard)));
            });
        });
        ++accesses;
        if (injected_failure_count > 0) {
            --injected_failure_count;
            co_return std::unexpected(
              cluster::make_error_code(cluster::errc::shutting_down));
        }
        co_return cluster_epoch.load();
    }

    auto acquire_barrier() { return ss::get_unique_lock(_mutex); }

    ss::future<int64_t> get_cached_epoch() {
        auto result = co_await service.local().get_cached_epoch(
          &abort_source.local());
        if (!result) {
            throw std::runtime_error(
              fmt::format("unable to fetch epoch: {}", result.error()));
        }
        co_return result.value();
    }

    ss::future<std::vector<int64_t>> all_epochs() {
        auto results = co_await service.map([this](epoch_service& es) {
            return es.get_cached_epoch(&abort_source.local());
        });
        std::vector<int64_t> epochs;
        for (const auto& res : results) {
            if (res.has_value()) {
                epochs.push_back(res.value());
            } else {
                throw std::runtime_error(
                  fmt::format("unable to fetch epoch: {}", res.error()));
            }
        }
        co_return epochs;
    }

    ss::shared_mutex _mutex;
    std::atomic<int64_t> cluster_epoch = 0;
    std::atomic<int64_t> accesses = 0;
    std::atomic<int64_t> injected_failure_count = 0;
    ss::sharded<epoch_service> service;
    ss::abort_source parent_abort_source;
    ssx::sharded_abort_source abort_source;
};

TEST_F_CORO(ClusterEpochService, TestCaching) {
    EXPECT_EQ(co_await get_cached_epoch(), cluster_epoch);
    EXPECT_EQ(accesses, 1);
    ++cluster_epoch;
    // Value is cached
    EXPECT_EQ(co_await get_cached_epoch(), cluster_epoch - 1);
    co_await tests::drain_task_queue();
    EXPECT_EQ(accesses, 1);
    // After the timeout we async re-fetch the value
    ss::manual_clock::advance(
      config::shard_local_cfg()
        .cloud_topics_epoch_service_local_epoch_cache_duration()
      + 1us);
    auto barrier = co_await acquire_barrier();
    EXPECT_EQ(co_await get_cached_epoch(), cluster_epoch - 1);
    barrier.unlock();
    co_await tests::drain_task_queue();
    EXPECT_EQ(accesses, 2);
    EXPECT_EQ(co_await get_cached_epoch(), cluster_epoch);
    EXPECT_EQ(accesses, 2);
    // After the max duration we wait to fetch the value
    ++cluster_epoch;
    ss::manual_clock::advance(
      config::shard_local_cfg()
        .cloud_topics_epoch_service_max_same_epoch_duration()
      + 1us);
    EXPECT_EQ(co_await get_cached_epoch(), cluster_epoch);
    EXPECT_EQ(accesses, 3);
}

TEST_F_CORO(ClusterEpochService, IncrementMustHappenEventually) {
    EXPECT_EQ(co_await get_cached_epoch(), cluster_epoch);
    EXPECT_EQ(accesses, 1);
    auto must_refresh_deadline
      = ss::manual_clock::now()
        + config::shard_local_cfg()
            .cloud_topics_epoch_service_max_same_epoch_duration();
    // After the timeout we async re-fetch the value
    ss::manual_clock::advance(
      config::shard_local_cfg()
        .cloud_topics_epoch_service_local_epoch_cache_duration()
      + 1us);
    while (ss::manual_clock::now() < must_refresh_deadline) {
        auto accesses_before = accesses.load();
        EXPECT_EQ(co_await get_cached_epoch(), cluster_epoch);
        co_await tests::drain_task_queue();
        EXPECT_EQ(accesses, accesses_before + 1);
        ss::manual_clock::advance(
          config::shard_local_cfg()
            .cloud_topics_epoch_service_local_epoch_cache_duration()
          + 1us);
    }
    // After the max duration we wait to fetch the value, and will fail if we
    // cannot
    auto fut = get_cached_epoch();
    co_await abort_source.request_abort();
    while (!fut.available()) {
        ss::manual_clock::advance(1s);
        co_await tests::drain_task_queue();
    }
    EXPECT_TRUE(fut.failed());
    fut.ignore_ready_future();
    co_await reset_abort_source();
    // After the epoch is updated we can fetch again successfully
    ++cluster_epoch;
    auto accesses_before = accesses.load();
    EXPECT_EQ(co_await get_cached_epoch(), cluster_epoch);
    EXPECT_EQ(accesses, accesses_before + 1);
}

TEST_F_CORO(ClusterEpochService, FetchesLimitedToShard0) {
    using ::testing::ElementsAre;
    EXPECT_THAT(co_await all_epochs(), ElementsAre(0, 0));
    EXPECT_EQ(accesses, 1);
    // Bumping the epoch and awaiting for the cache to expire should only cause
    // one additional fetch
    ++cluster_epoch;
    ss::manual_clock::advance(
      config::shard_local_cfg()
        .cloud_topics_epoch_service_local_epoch_cache_duration()
      + 1us);
    auto barrier = co_await acquire_barrier();
    EXPECT_THAT(co_await all_epochs(), ElementsAre(0, 0));
    barrier.unlock();
    co_await tests::drain_task_queue();
    EXPECT_EQ(accesses, 2);
    // Due to the cross core requests happening, we have to check a few times
    // for the requests to resolve, otherwise even if the task queue is idle
    // the cross core request from shard1 might not have finished yet.
    RPTEST_REQUIRE_EVENTUALLY_CORO(1s, [this]() -> ss::future<bool> {
        return tests::drain_task_queue().then([this]() {
            return all_epochs().then([](const std::vector<int64_t>& epochs) {
                return epochs == std::vector<int64_t>{1, 1};
            });
        });
    });
}

TEST_F_CORO(ClusterEpochService, CacheInvalidation) {
    using ::testing::ElementsAre;
    ++cluster_epoch;
    EXPECT_THAT(co_await all_epochs(), ElementsAre(1, 1));
    EXPECT_EQ(accesses, 1);
    ++cluster_epoch;
    // Lower epoch does not invalidate anything
    co_await service.invoke_on_all(&epoch_service::invalidate_epoch_cache, 0);
    EXPECT_THAT(co_await all_epochs(), ElementsAre(1, 1));
    co_await tests::drain_task_queue();
    EXPECT_EQ(accesses, 1);
    co_await service.invoke_on_all(&epoch_service::invalidate_epoch_cache, 2);
    EXPECT_THAT(co_await all_epochs(), ElementsAre(2, 2));
    EXPECT_EQ(accesses, 2);
}

TEST_F_CORO(ClusterEpochService, InjectedError) {
    using ::testing::ElementsAre;
    ++cluster_epoch;
    ++injected_failure_count;
    EXPECT_ANY_THROW(
      co_await ss::smp::submit_to(1, [this] { return get_cached_epoch(); }));
}
