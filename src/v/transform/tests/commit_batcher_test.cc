/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/errc.h"
#include "config/property.h"
#include "model/fundamental.h"
#include "model/transform.h"
#include "random/generators.h"
#include "test_utils/async.h"
#include "test_utils/randoms.h"
#include "transform/commit_batcher.h"
#include "transform/logger.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/print.hh>

#include <absl/container/btree_map.h>
#include <absl/container/btree_set.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <fmt/ostream.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace transform {
namespace {

using namespace std::chrono;

struct committed_offset {
    model::transform_id id;
    model::partition_id partition;
    kafka::offset offset;

    friend auto operator<=>(const committed_offset&, const committed_offset&)
      = default;
    friend bool operator==(const committed_offset&, const committed_offset&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& os, const committed_offset& co) {
        fmt::print(os, "{}/{}@{}", co.id(), co.partition(), co.offset());
        return os;
    }

    /**
     * Parse an offset in the form of "id/partition@offset"
     */
    static committed_offset parse(std::string_view s) {
        std::vector<std::string_view> splits = absl::StrSplit(s, '/');
        if (splits.size() != 2) {
            throw std::runtime_error(
              ss::format("invalid committed_offset: {}", s));
        }
        std::string_view id = splits[0];
        splits = absl::StrSplit(splits[1], '@');
        if (splits.size() != 2) {
            throw std::runtime_error(
              ss::format("invalid committed_offset: {}", s));
        }
        std::string_view partition = splits[0];
        std::string_view offset = splits[1];
        committed_offset result;
        bool valid = true;
        valid &= absl::SimpleAtoi(id, &result.id);
        valid &= absl::SimpleAtoi(partition, &result.partition);
        valid &= absl::SimpleAtoi(offset, &result.offset);
        if (!valid) {
            throw std::runtime_error(
              ss::format("invalid committed_offset: {}", s));
        }
        return result;
    }
};

using offset_map = absl::
  btree_map<model::transform_offsets_key, model::transform_offsets_value>;

class fake_offset_committer : public offset_committer {
public:
    fake_offset_committer(
      std::vector<model::partition_id> partitions, size_t limit)
      : _partitions(std::move(partitions))
      , _limit(limit) {}

    void adjust_limit(size_t limit) { _limit = limit; }
    void inject_coordinator_failures(int count) {
        _coordinator_failures = count;
    }
    void inject_commit_failures(int count) { _commit_failures = count; }

    ss::future<result<model::partition_id, cluster::errc>>
    find_coordinator(model::transform_offsets_key key) override {
        if (_coordinator_failures-- > 0) {
            throw std::runtime_error("injected error finding coordinator");
        }
        vlog(tlog.info, "find_coordinator={}", key);
        auto it = _mapping.find(key);
        if (it != _mapping.end()) {
            co_return it->second;
        }
        if (_mapping.size() >= _limit) {
            co_return cluster::errc::trackable_keys_limit_exceeded;
        }
        model::partition_id coordinator = random_generators::random_choice(
          _partitions);
        _mapping.emplace(key, coordinator);
        co_return coordinator;
    }

    ss::future<cluster::errc>
    batch_commit(model::partition_id coordinator, offset_map batch) override {
        vlog(tlog.info, "batch_commit={}->{}", coordinator, batch.size());
        for (const auto& [k, v] : batch) {
            auto c = co_await find_coordinator(k);
            if (c.has_error()) {
                co_return c.error();
            }
            if (c.value() != coordinator) {
                co_return cluster::errc::not_leader;
            }
        }
        _cond_var.broadcast();
        if (_commit_failures-- > 0) {
            throw std::runtime_error("injected error committing");
        }
        offset_map& m = _offsets[coordinator];
        m.swap(batch);
        m.merge(batch);
        co_return cluster::errc::success;
    }

    ss::future<> wait_for_next_commit() { return _cond_var.wait(); }

    absl::btree_set<committed_offset> committed() const {
        absl::btree_set<committed_offset> result;
        for (const auto& by_partition : _offsets) {
            for (const auto& e : by_partition.second) {
                result.emplace(e.first.id, e.first.partition, e.second.offset);
            }
        }
        return result;
    }

private:
    ss::condition_variable _cond_var;
    std::vector<model::partition_id> _partitions;
    size_t _limit;
    int64_t _coordinator_failures = 0;
    int64_t _commit_failures = 0;
    absl::btree_map<model::transform_offsets_key, model::partition_id> _mapping;
    absl::btree_map<model::partition_id, offset_map> _offsets;
};

constexpr static auto commit_interval = 5s;
constexpr static auto default_key_limit = 10;

class OffsetBatcherTest : public testing::Test {
public:
    void SetUp() override {
        std::vector<model::partition_id> partitions = {
          model::partition_id(1),
          model::partition_id(2),
          model::partition_id(3),
        };

        auto foc = std::make_unique<fake_offset_committer>(
          partitions, default_key_limit);

        _foc = foc.get();
        _batcher = std::make_unique<commit_batcher<ss::manual_clock>>(
          config::mock_binding(
            std::chrono::duration_cast<std::chrono::milliseconds>(
              commit_interval)),
          std::move(foc));
        _batcher->start().get();
        _batcher_running = true;
    }

    void TearDown() override {
        if (_batcher_running) {
            stop_batcher();
        }
        _batcher = nullptr;
    }

    void stop_batcher() {
        _batcher->stop().get();
        _batcher_running = false;
    }

    void enqueue(std::string_view s) {
        auto co = committed_offset::parse(s);
        _batcher
          ->commit_offset(
            {
              .id = co.id,
              .partition = co.partition,
              .output_topic = model::output_topic_index{0},
            },
            {.offset = co.offset})
          .get();
    }

    void unload(std::string_view s) {
        auto co = committed_offset::parse(absl::StrCat(s, "@0"));
        _batcher->unload({
          .id = co.id,
          .partition = co.partition,
          .output_topic = model::output_topic_index{0},
        });
    }

    void advance(ss::manual_clock::duration d) {
        ss::manual_clock::advance(d);
        // Drain the task queue so that all clock events have ran.
        drain_task_queue();
    }

    void drain_task_queue() {
        // Drain the task queue so that all clock events have ran.
        tests::drain_task_queue().get();
    }

    void set_max_keys(size_t max) { _foc->adjust_limit(max); }
    void inject_coordinator_failures(int c) {
        _foc->inject_coordinator_failures(c);
    }
    void inject_commit_failures(int c) { _foc->inject_commit_failures(c); }

    auto committed() { return _foc->committed(); }

    auto after_next_commit() {
        auto fut = _foc->wait_for_next_commit();
        while (!fut.available()) {
            advance(1s);
        }
        return committed();
    }

    template<typename... Args>
    auto committed_are(Args... args) {
        absl::btree_set<committed_offset> s;
        make_offset_set(s, args...);
        return ::testing::ContainerEq(s);
    }

private:
    template<typename... Rest>
    void make_offset_set(absl::btree_set<committed_offset>&)
    requires(sizeof...(Rest) == 0)
    {}

    template<typename... Rest>
    void make_offset_set(
      absl::btree_set<committed_offset>& output,
      std::string_view s,
      Rest... rest) {
        output.emplace(committed_offset::parse(s));
        make_offset_set(output, rest...);
    }

    fake_offset_committer* _foc = nullptr;
    bool _batcher_running = false;
    std::unique_ptr<commit_batcher<ss::manual_clock>> _batcher;
};

TEST_F(OffsetBatcherTest, Smoke) {
    enqueue("1/1@2");
    enqueue("1/2@3");
    enqueue("1/3@1");
    EXPECT_THAT(after_next_commit(), committed_are("1/1@2", "1/2@3", "1/3@1"));
    enqueue("1/2@10");
    EXPECT_THAT(after_next_commit(), committed_are("1/1@2", "1/2@10", "1/3@1"));
}

TEST_F(OffsetBatcherTest, CoordinatorLimit) {
    set_max_keys(2);
    enqueue("1/1@2");
    enqueue("1/2@3");
    enqueue("1/3@1");
    EXPECT_THAT(after_next_commit(), committed_are("1/1@2", "1/2@3"));
    // We always retry coordinator requests until they pass
    set_max_keys(3);
    EXPECT_THAT(after_next_commit(), committed_are("1/1@2", "1/2@3", "1/3@1"));
}

TEST_F(OffsetBatcherTest, CoordinatorLimitUnload) {
    set_max_keys(1);
    enqueue("1/1@2");
    enqueue("1/2@3");
    EXPECT_THAT(after_next_commit(), committed_are("1/1@2"));
    // Unloading removes pending coordinator lookups
    set_max_keys(3);
    unload("1/2");
    enqueue("1/1@3");
    EXPECT_THAT(after_next_commit(), committed_are("1/1@3"));
}

TEST_F(OffsetBatcherTest, StopDoesNotLookupCoordinators) {
    enqueue("1/1@3");
    stop_batcher();
    EXPECT_THAT(committed(), committed_are());
}

TEST_F(OffsetBatcherTest, StopFlushesPendingCommits) {
    enqueue("1/1@2");
    // Enqueue so that the coordinator is already loaded.
    EXPECT_THAT(after_next_commit(), committed_are("1/1@2"));
    enqueue("1/1@3");
    stop_batcher();
    EXPECT_THAT(committed(), committed_are("1/1@3"));
}

TEST_F(OffsetBatcherTest, FlushFailuresAreNotRetried) {
    enqueue("1/1@2");
    inject_commit_failures(1);
    EXPECT_THAT(after_next_commit(), committed_are());
    enqueue("1/2@1");
    EXPECT_THAT(after_next_commit(), committed_are("1/2@1"));
}

TEST_F(OffsetBatcherTest, CoordinatorLookupsAreRetried) {
    enqueue("1/1@2");
    inject_coordinator_failures(3);
    EXPECT_THAT(after_next_commit(), committed_are("1/1@2"));
}

} // namespace
} // namespace transform
