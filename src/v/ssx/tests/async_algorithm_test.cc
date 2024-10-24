// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "base/seastarx.h"
#include "gmock/gmock.h"
#include "ssx/async-clear.h"
#include "ssx/async_algorithm.h"
#include "ssx/future-util.h"
#include "utils/move_canary.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <unordered_map>

namespace ssx {

namespace {

static ss::future<>
async_push_back(auto& container, ssize_t n, const auto& val) {
    constexpr ssize_t batch = 1000;

    while (n > 0) {
        std::fill_n(std::back_inserter(container), std::min(n, batch), val);
        n -= batch;
        co_await ss::coroutine::maybe_yield();
    }
}

struct add_one {
    void operator()(int& x) const { x++; };
    void operator()(std::pair<const int, int>& x) const { x.second++; };
};

const auto add_one_slow = [](int& x) {
    static volatile int sink = 0;
    for (int i = 0; i < 100; i++) {
        sink = sink + 1;
    }
    x++;
};

static thread_local int yields;

template<ssize_t I>
struct test_traits : async_algo_traits {
    constexpr static ssize_t interval = I;
    static void yield_called() { yields++; }
};

static void check_same_result(const auto& container, auto fn) {
    auto std_input = container, async_input0 = container,
         async_input1 = container, async_input2 = container,
         async_input3 = container;

    std::for_each(std_input.begin(), std_input.end(), fn);

    // vanilla
    ssx::async_for_each(async_input0.begin(), async_input0.end(), fn).get();

    // interval 1
    ssx::async_for_each<test_traits<1>>(
      async_input1.begin(), async_input1.end(), fn)
      .get();

    // vanilla with counter
    async_counter c;
    ssx::async_for_each_counter(c, async_input2.begin(), async_input2.end(), fn)
      .get();

    // interval 1 with counter
    ssx::async_for_each_counter<test_traits<1>>(
      c, async_input3.begin(), async_input3.end(), fn)
      .get();

    EXPECT_EQ(std_input, async_input0);
    EXPECT_EQ(std_input, async_input1);
    EXPECT_EQ(std_input, async_input2);
    EXPECT_EQ(std_input, async_input3);
};

struct task_counter {
    static int64_t tasks() {
        return (int64_t)ss::engine().get_sched_stats().tasks_processed;
    }

    int64_t task_start = tasks();
    ssize_t yield_start = yields;

    int64_t task_delta() { return tasks() - task_start; }
    ssize_t yield_delta() { return yields - yield_start; }
};

} // namespace

std::vector<int> make_container(size_t elems, std::vector<int>) {
    std::vector<int> ret;
    for (int i = 0; i < elems; i++) {
        ret.push_back(i);
    }
    return ret;
}

std::unordered_map<int, int>
make_container(size_t elems, std::unordered_map<int, int>) {
    std::unordered_map<int, int> ret;
    for (int i = 0; i < elems; i++) {
        ret[i] = i;
    }
    return ret;
}

template<typename T>
struct AsyncAlgo : public testing::Test {
    using container = T;

    static T make(size_t size) { return make_container(size, T{}); }
};

// using container_types = ::testing::Types<std::vector<int>>;
using container_types
  = ::testing::Types<std::vector<int>, std::unordered_map<int, int>>;
TYPED_TEST_SUITE(AsyncAlgo, container_types);

TYPED_TEST(AsyncAlgo, make_container) {
    auto c = this->make(2);
    ASSERT_EQ(2, c.size());
    ASSERT_EQ(c[0], 0);
    ASSERT_EQ(c[1], 1);
}

TYPED_TEST(AsyncAlgo, async_for_each_same_result) {
    // basic checks
    check_same_result(this->make(0), add_one{});
    check_same_result(this->make(4), add_one{});
}

TYPED_TEST(AsyncAlgo, yield_count) {
    // helper to check that async_for_each results in the same final state
    // as std::for_each

    auto v = this->make(5);

    task_counter c;
    ssx::async_for_each<test_traits<1>>(v.begin(), v.end(), add_one{}).get();
    EXPECT_EQ(5, c.yield_delta());

    c = {};
    ssx::async_for_each<test_traits<2>>(v.begin(), v.end(), add_one{}).get();
    // floor(5/2), as we don't yield on partial intervals
    EXPECT_EQ(2, c.yield_delta());
}

TYPED_TEST(AsyncAlgo, yield_count_counter) {
    async_counter a_counter;

    auto v = this->make(2);

    task_counter t_counter;
    ssx::async_for_each_counter<test_traits<4>>(
      a_counter, v.begin(), v.end(), add_one{})
      .get();
    EXPECT_EQ(0, t_counter.yield_delta());
    EXPECT_EQ(3, a_counter.count);

    // now we should get a yield since we carry over the 2 ops
    // from above
    t_counter = {};
    ssx::async_for_each_counter<test_traits<4>>(
      a_counter, v.begin(), v.end(), add_one{})
      .get();
    EXPECT_EQ(1, t_counter.yield_delta());

    v = this->make(3);
    t_counter = {};
    a_counter = {};
    ssx::async_for_each_counter<test_traits<2>>(
      a_counter, v.begin(), v.end(), add_one{})
      .get();
    EXPECT_EQ(1, a_counter.count);
    EXPECT_EQ(1, t_counter.yield_delta());

    t_counter = {};
    ssx::async_for_each_counter<test_traits<2>>(
      a_counter, v.begin(), v.end(), add_one{})
      .get();
    EXPECT_EQ(0, a_counter.count);
    EXPECT_EQ(2, t_counter.yield_delta());
}

TYPED_TEST(AsyncAlgo, yield_count_counter_empty) {
    async_counter a_counter;
    task_counter t_counter;

    TypeParam empty;

    auto call = [&] {
        ssx::async_for_each_counter<test_traits<2>>(
          a_counter, empty.begin(), empty.end(), add_one{})
          .get();
    };

    call();
    EXPECT_EQ(1, a_counter.count);
    EXPECT_EQ(0, t_counter.yield_delta());

    call();
    EXPECT_EQ(0, a_counter.count);
    EXPECT_EQ(1, t_counter.yield_delta());

    call();
    EXPECT_EQ(1, a_counter.count);
    EXPECT_EQ(1, t_counter.yield_delta());

    a_counter = {};
    t_counter = {};
    constexpr auto iters = 101;
    for (int i = 0; i < iters; i++) {
        call();
    }

    // here we check that even though the vector is empty, we do yield 50 times
    // (100 iterations / work interval of 2), since always assume 1 unit of work
    // has been performed
    EXPECT_EQ(iters / 2, t_counter.yield_delta());
}

TEST(AsyncAlgo, async_for_each_large_container) {
    constexpr size_t size = 1'000'000;
    constexpr int answer = 42;

    std::deque<int> v;
    async_push_back(v, size, answer).get();

    task_counter tasks;

    async_for_each(v.begin(), v.end(), add_one_slow).get();

    EXPECT_GT(tasks.task_delta(), 2); // in practice it's > 100
}

int value(int i) { return i; }
int value(std::pair<const int, int> p) { return p.second; }

TYPED_TEST(AsyncAlgo, async_for_each_move_correctness) {
    auto v = this->make(10);

    auto func = [canary = move_canary{}](const auto& i) {
        if (value(i) != -1) {
            EXPECT_FALSE(canary.is_moved_from());
        }
        return canary.is_moved_from();
    };

    ASSERT_FALSE(func(-1));

    async_for_each<test_traits<2>>(v.begin(), v.end(), func).get();

    // test that the canary is working
    auto other_func = std::move(func);
    // NOLINTNEXTLINE(bugprone-use-after-move)
    ASSERT_TRUE(func(-1));
    ASSERT_FALSE(other_func(-1));
}

} // namespace ssx
