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

#include "cluster_link/task.h"
#include "test_utils/test.h"

#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>

#include <gmock/gmock.h>

using namespace std::chrono_literals;
using namespace ::testing;

namespace cluster_link {

namespace {
template<typename T>
auto IsInRange(T lo, T hi) {
    return AllOf(Ge((lo)), Le((hi)));
}
} // namespace

static constexpr auto initial_run_interval = 500ms;

struct test_metadata : public model::metadata {
    ss::lowres_clock::duration run_interval{initial_run_interval};
};
class test_task : public task {
public:
    static constexpr auto name = "test_task";
    explicit test_task(link* link, ss::lowres_clock::duration run_interval)
      : task(link, run_interval, name) {}

    bool should_start_impl(ss::shard_id, ::model::node_id) const override {
        return true;
    }
    bool should_stop_impl(ss::shard_id, ::model::node_id) const override {
        return false;
    }

    void update_config(const model::metadata& meta) override {
        auto cfg = static_cast<const test_metadata*>(&meta);
        vassert(cfg != nullptr, "meta is not of type test_metadata");
        set_run_interval(cfg->run_interval);
    }

    ss::future<> run_impl() override {
        _count++;
        return ss::now();
    }

    unsigned count() const noexcept { return _count; }

private:
    unsigned _count{0};
};

class test_task_factory : public task_factory {
public:
    std::string_view created_task_name() const noexcept override {
        return test_task::name;
    }
    std::unique_ptr<task> create_task(link* link) override {
        return std::make_unique<test_task>(link, initial_run_interval);
    }
};

class test_task_fixture : public seastar_test {
public:
    ss::future<void> SetUpAsync() override {
        _task_factory = std::make_unique<test_task_factory>();
        return ss::now();
    }
    ss::future<void> TearDownAsync() override {
        _task_factory.reset(nullptr);
        return ss::now();
    }

    std::unique_ptr<task> create_task() {
        return _task_factory->create_task(nullptr);
    }

private:
    std::unique_ptr<task_factory> _task_factory{nullptr};
};

TEST_F_CORO(test_task_fixture, test_task_run) {
    auto task = create_task();
    auto test_task_inst = dynamic_cast<test_task*>(task.get());
    ASSERT_EQ_CORO(task->get_state(), model::task_state::stopped);

    auto res = co_await task->pause();
    EXPECT_FALSE(res.has_value())
      << "Was able to pause task when in stopped state";
    EXPECT_EQ(res.assume_error().code(), errc::invalid_task_state_change);
    ASSERT_EQ_CORO(task->get_state(), model::task_state::stopped);

    res = co_await task->start();
    ASSERT_TRUE_CORO(res.has_value())
      << "Failed to start task: " << res.assume_error().message();
    EXPECT_EQ(task->get_state(), model::task_state::active);

    res = co_await task->start();
    ASSERT_FALSE_CORO(res.has_value())
      << "Was able to start task when already running";
    EXPECT_EQ(res.assume_error().code(), errc::task_already_running);

    auto cur_val = test_task_inst->count();
    auto prev_val = cur_val;
    co_await ss::sleep(initial_run_interval * 2);
    cur_val = test_task_inst->count();
    EXPECT_THAT(cur_val, IsInRange(prev_val + 1, prev_val + 2));

    res = co_await task->stop();
    ASSERT_TRUE_CORO(res.has_value())
      << "Failed to stop task: " << res.assume_error().message();
    EXPECT_EQ(task->get_state(), model::task_state::stopped);
}

class test_task_started_fixture : public test_task_fixture {
public:
    ss::future<void> SetUpAsync() override {
        co_await test_task_fixture::SetUpAsync();
        _task = create_task();
        auto res = co_await _task->start();
        ASSERT_TRUE_CORO(res.has_value())
          << "Failed to start task: " << res.assume_error().message();
        ASSERT_EQ_CORO(_task->get_state(), model::task_state::active);
    }

    ss::future<void> TearDownAsync() override {
        auto res = co_await _task->stop();
        ASSERT_TRUE_CORO(res.has_value())
          << "Failed to stop task: " << res.assume_error().message();
        _task.reset(nullptr);
        co_await test_task_fixture::TearDownAsync();
    }

    test_task* get_task() { return dynamic_cast<test_task*>(_task.get()); }

private:
    std::unique_ptr<task> _task{nullptr};
};

TEST_F_CORO(test_task_started_fixture, test_pause_resume) {
    // make sure the task is running
    co_await ss::sleep(initial_run_interval);
    auto pre_count = get_task()->count();
    EXPECT_GT(pre_count, 0);

    auto res = co_await get_task()->pause();
    ASSERT_TRUE_CORO(res.has_value())
      << "Failed to pause task: " << res.assume_error().message();
    ASSERT_EQ_CORO(get_task()->get_state(), model::task_state::paused);

    co_await ss::sleep(initial_run_interval * 2);
    auto cur_count = get_task()->count();
    EXPECT_THAT(cur_count, IsInRange(pre_count, pre_count + 1));

    res = co_await get_task()->start();
    ASSERT_TRUE_CORO(res.has_value())
      << "Failed to resume task: " << res.assume_error().message();

    co_await ss::sleep(initial_run_interval * 2);
    pre_count = cur_count;
    cur_count = get_task()->count();
    EXPECT_THAT(cur_count, IsInRange(pre_count + 2, pre_count + 3));
}

TEST_F_CORO(test_task_started_fixture, test_change_run_interval) {
    test_metadata meta;
    meta.run_interval = initial_run_interval * 2;
    co_await ss::sleep(initial_run_interval);
    auto pre_count = get_task()->count();
    get_task()->update_config(meta);
    co_await ss::sleep(initial_run_interval);
    auto cur_count = get_task()->count();
    EXPECT_THAT(cur_count, IsInRange(pre_count, pre_count + 1));

    co_await ss::sleep(initial_run_interval);
    cur_count = get_task()->count();
    EXPECT_THAT(cur_count, IsInRange(pre_count + 1, pre_count + 2));
}

TEST_F_CORO(test_task_started_fixture, test_callbacks) {
    model::task_state prev_state = model::task_state::stopped,
                      cur_state = model::task_state::stopped;
    auto cb = [&](std::string_view name, task::state_change change) {
        prev_state = change.prev;
        cur_state = change.cur;
        EXPECT_EQ(name, test_task::name);
    };
    auto id = get_task()->register_for_updates(std::move(cb));
    auto remove_cb = ss::defer([&] { get_task()->unregister_for_updates(id); });
    auto res = co_await get_task()->pause();
    ASSERT_TRUE_CORO(res.has_value())
      << "Failed to pause task: " << res.assume_error().message();
    EXPECT_EQ(prev_state, model::task_state::active);
    EXPECT_EQ(cur_state, model::task_state::paused);

    res = co_await get_task()->start();
    ASSERT_TRUE_CORO(res.has_value())
      << "Failed to resume task: " << res.assume_error().message();
    EXPECT_EQ(prev_state, model::task_state::paused);
    EXPECT_EQ(cur_state, model::task_state::active);
}

class evil_task : public task {
public:
    static constexpr auto name = "evil_task";
    explicit evil_task(ss::lowres_clock::duration run_interval)
      : task(nullptr, run_interval, name) {}

    bool should_start_impl(ss::shard_id, ::model::node_id) const override {
        return true;
    }
    bool should_stop_impl(ss::shard_id, ::model::node_id) const override {
        return false;
    }

    void update_config(const model::metadata&) override {}

    ss::future<> run_impl() override {
        throw std::runtime_error("evil task failed");
    }
};

class evil_task_fixture : public seastar_test {};

TEST_F_CORO(evil_task_fixture, test_failing_task) {
    auto task = std::make_unique<evil_task>(initial_run_interval);
    auto res = co_await task->start();
    ASSERT_TRUE_CORO(res.has_value())
      << "Failed to start task: " << res.assume_error().message();
    co_await ss::sleep(initial_run_interval * 2);
    EXPECT_EQ(task->get_state(), model::task_state::faulted);

    res = co_await task->stop();
    ASSERT_TRUE_CORO(res.has_value())
      << "Failed to stop task: " << res.assume_error().message();
}

class link_unavailable_task : public task {
public:
    static constexpr auto name = "link_unavailable_task";
    explicit link_unavailable_task(ss::lowres_clock::duration run_interval)
      : task(nullptr, run_interval, name) {}

    bool should_start_impl(ss::shard_id, ::model::node_id) const override {
        return true;
    }
    bool should_stop_impl(ss::shard_id, ::model::node_id) const override {
        return false;
    }

    void update_config(const model::metadata&) override {}

    ss::future<> run_impl() override {
        if (get_state() == model::task_state::active) {
            vlog(logger().info, "Simulating link unavailability");
            auto res = change_state(
              model::task_state::link_unavailable, "Simulated link down");
            vassert(
              res.has_value()
                && res.assume_value() == model::task_state::active,
              "Failed to change state to link_unavailable");
        } else if (get_state() == model::task_state::link_unavailable) {
            vlog(logger().info, "Simulating link availability");
            auto res = change_state(
              model::task_state::active, "Simulated link up");
            vassert(
              res.has_value()
                && res.assume_value() == model::task_state::link_unavailable,
              "Failed to change state to active");
        }

        return ss::now();
    }
};

class link_unavailable_fixture : public seastar_test {};

TEST_F_CORO(link_unavailable_fixture, test_link_unavailable_task) {
    auto task = std::make_unique<link_unavailable_task>(initial_run_interval);
    auto res = co_await task->start();
    ASSERT_TRUE_CORO(res.has_value())
      << "Failed to start task: " << res.assume_error().message();

    co_await ss::sleep(initial_run_interval / 2);
    EXPECT_EQ(task->get_state(), model::task_state::link_unavailable);

    co_await ss::sleep(initial_run_interval);
    EXPECT_EQ(task->get_state(), model::task_state::active);

    co_await ss::sleep(initial_run_interval);
    EXPECT_EQ(task->get_state(), model::task_state::link_unavailable);

    res = co_await task->stop();
    ASSERT_TRUE_CORO(res.has_value())
      << "Failed to stop task: " << res.assume_error().message();
}
} // namespace cluster_link
