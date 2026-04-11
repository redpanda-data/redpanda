// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "container/chunked_vector.h"
#include "container/priority_queue.h"
#include "container/tests/bench_utils.h"
#include "random/generators.h"

#include <seastar/core/map_reduce.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/testing/perf_tests.hh>

#include <algorithm>
#include <cstddef>
#include <queue>
#include <type_traits>
#include <vector>

constexpr size_t inner_iters_for(size_t size) {
    return std::max<size_t>(1, 10000 / std::max<size_t>(1, size));
}

template<typename PriorityQueue, size_t Size>
struct PriorityQueueBenchTest {
    using value_type = typename PriorityQueue::value_type;
    static constexpr size_t inner_iters = inner_iters_for(Size);
    const chunked_vector<value_type> test_data = make_test_data();

    static auto make_random_value() {
        if constexpr (std::is_integral_v<value_type>) {
            return random_generators::get_int<value_type>();
        } else if constexpr (std::same_as<value_type, ss::sstring>) {
            return random_generators::gen_alphanum_string(32);
        } else if constexpr (std::same_as<value_type, large_struct>) {
            constexpr size_t max_str_len = 64;
            constexpr size_t max_num_cardinality = 16;
            return large_struct{
              .foo = max_num_cardinality / 3,
              .bar = max_num_cardinality - 2,
              .qux = std::string(max_str_len / 8, 'x'),
              .baz = random_generators::gen_alphanum_string(max_str_len / 4),
              .more = random_generators::gen_alphanum_string(max_str_len / 2),
              .data = random_generators::gen_alphanum_string(max_str_len / 1),
              .okdone = 4242,
            };
        }
    }

    static auto make_test_data() {
        static const auto data = []() {
            chunked_vector<value_type> vec;
            vec.reserve(Size);
            for (size_t j = 0; j < Size; ++j) {
                vec.emplace_back(make_random_value());
            }
            return vec;
        }();
        return data.copy();
    }

    PriorityQueue make_filled() {
        PriorityQueue pq;
        pq.push_range(make_test_data());
        return pq;
    }

    // No start/stop — trivial setup only, framework handles timing.
    [[gnu::noinline]]
    void run_push_test() {
        PriorityQueue pq;
        for (const auto& val : test_data) {
            pq.push(val);
        }
        perf_tests::do_not_optimize(pq);
    }

    [[gnu::noinline]]
    size_t run_push_rvalue_test() {
        std::vector<chunked_vector<value_type>> inputs;
        inputs.reserve(inner_iters);
        for (size_t i = 0; i < inner_iters; ++i) {
            inputs.push_back(make_test_data());
        }
        perf_tests::start_measuring_time();
        for (auto& td : inputs) {
            PriorityQueue pq;
            for (auto& val : std::move(td)) {
                pq.push(std::move(val));
            }
            perf_tests::do_not_optimize(pq);
        }
        perf_tests::stop_measuring_time();
        return inner_iters;
    }

    // No start/stop — trivial setup only, framework handles timing.
    [[gnu::noinline]]
    void run_push_range_test() {
        PriorityQueue pq;
        pq.push_range(test_data);
        perf_tests::do_not_optimize(pq);
    }

    [[gnu::noinline]]
    size_t run_push_rvalue_range_test() {
        std::vector<chunked_vector<value_type>> inputs;
        inputs.reserve(inner_iters);
        for (size_t i = 0; i < inner_iters; ++i) {
            inputs.push_back(make_test_data());
        }
        perf_tests::start_measuring_time();
        for (auto& td : inputs) {
            PriorityQueue pq;
            pq.push_range(std::move(td));
            perf_tests::do_not_optimize(pq);
        }
        perf_tests::stop_measuring_time();
        return inner_iters;
    }

    [[gnu::noinline]]
    ss::future<size_t> run_async_push_rvalue_range_test() {
        std::vector<chunked_vector<value_type>> inputs;
        inputs.reserve(inner_iters);
        for (size_t i = 0; i < inner_iters; ++i) {
            inputs.push_back(make_test_data());
        }
        perf_tests::start_measuring_time();
        for (auto& td : inputs) {
            PriorityQueue pq;
            co_await pq.async_push_range(std::move(td));
            perf_tests::do_not_optimize(pq);
        }
        perf_tests::stop_measuring_time();
        co_return inner_iters;
    }

    [[gnu::noinline]]
    size_t run_pop_test() {
        std::vector<PriorityQueue> inputs;
        inputs.reserve(inner_iters);
        for (size_t i = 0; i < inner_iters; ++i) {
            inputs.push_back(make_filled());
        }
        perf_tests::start_measuring_time();
        for (auto& pq : inputs) {
            while (!pq.empty()) {
                if constexpr (!std::is_void_v<decltype(pq.pop())>) {
                    auto x = pq.pop();
                    perf_tests::do_not_optimize(x);
                } else {
                    auto x = pq.top();
                    pq.pop();
                    perf_tests::do_not_optimize(x);
                }
            }
        }
        perf_tests::stop_measuring_time();
        return inner_iters;
    }

    [[gnu::noinline]]
    size_t run_extract_sorted_test() {
        std::vector<PriorityQueue> inputs;
        inputs.reserve(inner_iters);
        for (size_t i = 0; i < inner_iters; ++i) {
            inputs.push_back(make_filled());
        }
        perf_tests::start_measuring_time();
        for (auto& pq : inputs) {
            auto x = std::move(pq).extract_sorted();
            perf_tests::do_not_optimize(x);
        }
        perf_tests::stop_measuring_time();
        return inner_iters;
    }

    [[gnu::noinline]]
    ss::future<size_t> run_async_extract_sorted_test() {
        std::vector<PriorityQueue> inputs;
        inputs.reserve(inner_iters);
        for (size_t i = 0; i < inner_iters; ++i) {
            PriorityQueue pq;
            pq.push_range(make_test_data());
            inputs.push_back(std::move(pq));
        }
        perf_tests::start_measuring_time();
        for (auto& pq : inputs) {
            auto x = co_await std::move(pq).async_extract_sorted();
            perf_tests::do_not_optimize(x);
        }
        perf_tests::stop_measuring_time();
        co_return inner_iters;
    }

    [[gnu::noinline]]
    size_t run_sort_extract_heap_test() {
        std::vector<PriorityQueue> inputs;
        inputs.reserve(inner_iters);
        for (size_t i = 0; i < inner_iters; ++i) {
            inputs.push_back(make_filled());
        }
        perf_tests::start_measuring_time();
        for (auto& pq : inputs) {
            auto x = std::move(pq).extract_heap();
            std::ranges::sort(x);
            perf_tests::do_not_optimize(x);
        }
        perf_tests::stop_measuring_time();
        return inner_iters;
    }
};

template<typename PriorityQueue, size_t InputSize, size_t Cap>
struct BoundedPriorityQueueBenchTest
  : PriorityQueueBenchTest<PriorityQueue, InputSize> {
    [[gnu::noinline]]
    size_t run_push_rvalue_range_test() {
        std::vector<chunked_vector<typename PriorityQueue::value_type>> inputs;
        inputs.reserve(this->inner_iters);
        for (size_t i = 0; i < this->inner_iters; ++i) {
            inputs.push_back(this->make_test_data());
        }
        perf_tests::start_measuring_time();
        for (auto& td : inputs) {
            PriorityQueue pq{Cap};
            pq.push_range(std::move(td));
            perf_tests::do_not_optimize(pq);
        }
        perf_tests::stop_measuring_time();
        return this->inner_iters;
    }

    [[gnu::noinline]]
    ss::future<size_t> run_async_push_rvalue_range_test() {
        std::vector<chunked_vector<typename PriorityQueue::value_type>> inputs;
        inputs.reserve(this->inner_iters);
        for (size_t i = 0; i < this->inner_iters; ++i) {
            inputs.push_back(this->make_test_data());
        }
        perf_tests::start_measuring_time();
        for (auto& td : inputs) {
            PriorityQueue pq{Cap};
            co_await pq.async_push_range(std::move(td));
            perf_tests::do_not_optimize(pq);
        }
        perf_tests::stop_measuring_time();
        co_return this->inner_iters;
    }
};

// NOLINTBEGIN(*-macro-*)
#define PRIORITY_QUEUE_PERF_TEST(container, element, size)                     \
    class PriorityQueueBenchTest_##container##_##element##_##size              \
      : public PriorityQueueBenchTest<container<element>, size> {};            \
    PERF_TEST_F(                                                               \
      PriorityQueueBenchTest_##container##_##element##_##size, Push) {         \
        run_push_test();                                                       \
    }                                                                          \
    PERF_TEST_F(                                                               \
      PriorityQueueBenchTest_##container##_##element##_##size, PushRange) {    \
        run_push_range_test();                                                 \
    }                                                                          \
    PERF_TEST_F(                                                               \
      PriorityQueueBenchTest_##container##_##element##_##size, Pop) {          \
        return run_pop_test();                                                 \
    }

#define PRIORITY_QUEUE_MOVE_TEST(container, element, size)                     \
    class PriorityQueueRValueBenchTest_##container##_##element##_##size        \
      : public PriorityQueueBenchTest<container<element>, size> {};            \
    PERF_TEST_F(                                                               \
      PriorityQueueRValueBenchTest_##container##_##element##_##size, Push) {   \
        return run_push_rvalue_test();                                         \
    }                                                                          \
    PERF_TEST_F(                                                               \
      PriorityQueueRValueBenchTest_##container##_##element##_##size,           \
      PushRange) {                                                             \
        return run_push_rvalue_range_test();                                   \
    }                                                                          \
    PERF_TEST_F(                                                               \
      PriorityQueueRValueBenchTest_##container##_##element##_##size,           \
      AsyncPushRange) {                                                        \
        return run_async_push_rvalue_range_test();                             \
    }

#define PRIORITY_QUEUE_SORT_TEST(container, element, size)                     \
    class PriorityQueueSortBenchTest_##container##_##element##_##size          \
      : public PriorityQueueBenchTest<container<element>, size> {};            \
    PERF_TEST_F(                                                               \
      PriorityQueueSortBenchTest_##container##_##element##_##size,             \
      ExtractSorted) {                                                         \
        return run_extract_sorted_test();                                      \
    }                                                                          \
    PERF_TEST_F(                                                               \
      PriorityQueueSortBenchTest_##container##_##element##_##size,             \
      SortExtractHeap) {                                                       \
        return run_sort_extract_heap_test();                                   \
    }                                                                          \
    PERF_TEST_F(                                                               \
      PriorityQueueSortBenchTest_##container##_##element##_##size,             \
      AsyncExtractSorted) {                                                    \
        return run_async_extract_sorted_test();                                \
    }

#define BOUNDED_PRIORITY_QUEUE_PERF_TEST(                                                \
  container, element, input_size, capacity)                                              \
    class                                                                                \
      BoundedPriorityQueueBenchTest_##container##_##element##_##input_size##_##          \
      capacity                                                                           \
      : public BoundedPriorityQueueBenchTest<                                            \
          container<element>,                                                            \
          input_size,                                                                    \
          capacity> {};                                                                  \
    PERF_TEST_F(                                                                         \
      BoundedPriorityQueueBenchTest_##container##_##element##_##input_size##_##capacity, \
      PushRange) {                                                                       \
        return run_push_rvalue_range_test();                                             \
    }                                                                                    \
    PERF_TEST_F(                                                                         \
      BoundedPriorityQueueBenchTest_##container##_##element##_##input_size##_##capacity, \
      AsyncPushRange) {                                                                  \
        return run_async_push_rvalue_range_test();                                       \
    }
// NOLINTEND(*-macro-*)

template<typename T>
using std_pq = std::priority_queue<T>;
template<typename T>
using our_pq = priority_queue<T>;
template<typename T>
using bounded_pq = bounded_priority_queue<T>;

PRIORITY_QUEUE_PERF_TEST(std_pq, int64_t, 64)
PRIORITY_QUEUE_PERF_TEST(our_pq, int64_t, 64)

PRIORITY_QUEUE_PERF_TEST(std_pq, int64_t, 1024)
PRIORITY_QUEUE_PERF_TEST(our_pq, int64_t, 1024)

PRIORITY_QUEUE_PERF_TEST(std_pq, sstring, 1024)
PRIORITY_QUEUE_PERF_TEST(our_pq, sstring, 1024)

PRIORITY_QUEUE_PERF_TEST(std_pq, large_struct, 1024)
PRIORITY_QUEUE_PERF_TEST(our_pq, large_struct, 1024)

PRIORITY_QUEUE_PERF_TEST(std_pq, int64_t, 1048576)
PRIORITY_QUEUE_PERF_TEST(our_pq, int64_t, 1048576)

PRIORITY_QUEUE_MOVE_TEST(our_pq, sstring, 1024)

PRIORITY_QUEUE_SORT_TEST(our_pq, int64_t, 64)
PRIORITY_QUEUE_SORT_TEST(our_pq, int64_t, 1024)
PRIORITY_QUEUE_SORT_TEST(our_pq, int64_t, 1048576)

// small
BOUNDED_PRIORITY_QUEUE_PERF_TEST(bounded_pq, int64_t, 1024, 1024)
// reduce
BOUNDED_PRIORITY_QUEUE_PERF_TEST(bounded_pq, int64_t, 1048576, 1024)
// large
BOUNDED_PRIORITY_QUEUE_PERF_TEST(bounded_pq, int64_t, 1048576, 1048576)
