/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "seastarx.h"
#include "v8_engine/executor.h"

#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/when_all.hh>
#include <seastar/testing/thread_test_case.hh>

#include <algorithm>
#include <chrono>
#include <unordered_map>

class test_exception final : public std::exception {
public:
    explicit test_exception(ss::sstring msg) noexcept
      : _msg(std::move(msg)) {}

    const char* what() const noexcept final { return _msg.c_str(); }

private:
    ss::sstring _msg;
};

ss::future<> add_future_handlers(ss::future<>& fut) {
    return fut
      .handle_exception_type([](ss::gate_closed_exception& ex) {
          throw test_exception("Executor is stopped");
      })
      .handle_exception_type([](ss::broken_semaphore& ex) {
          throw test_exception("Executor is stopped");
      });
}

SEASTAR_THREAD_TEST_CASE(task_queue_full_test) {
    struct task_for_test {
        void operator()() { sleep(1); }

        void cancel(){};

        void on_timeout() {}
    };

    v8_engine::executor test_executor(1, ss::smp::count);

    std::vector<ss::future<>> futures;
    futures.reserve(3);

    for (int i = 0; i < 3; ++i) {
        futures.emplace_back(test_executor.submit(
          task_for_test(), std::chrono::milliseconds(5000)));
    }

    ss::when_all_succeed(futures.begin(), futures.end()).get();
    test_executor.stop().get();
}

SEASTAR_THREAD_TEST_CASE(simple_stop_executor_test) {
    struct task_for_test {
        void operator()() {}

        void cancel() {}

        void on_timeout() {}
    };

    v8_engine::executor test_executor(1, ss::smp::count);
    test_executor.stop().get();
    auto fut = test_executor.submit(
      task_for_test(), std::chrono::milliseconds(5000));

    BOOST_REQUIRE_EXCEPTION(
      add_future_handlers(fut).get(),
      test_exception,
      [](const test_exception& e) {
          return "Executor is stopped" == std::string(e.what());
      });
}

SEASTAR_THREAD_TEST_CASE(task_with_exception_test) {
    struct task_for_test_with_process_exception {
        void operator()() { throw test_exception("Process exception"); }

        void cancel() {}

        void on_timeout() {}
    };

    struct task_for_test_with_process_set_cancel_exception {
        void operator()() { sleep(1); }

        void cancel() { throw test_exception("Set cancel() exception"); }

        void on_timeout() {}
    };

    struct task_for_test_with_process_set_on_timeout_exception {
        void operator()() { sleep(1); }

        void cancel() {}

        void on_timeout() {
            throw test_exception("Set on_timeout() exception");
        }
    };

    v8_engine::executor test_executor(1, ss::smp::count);

    auto fut_process = test_executor.submit(
      task_for_test_with_process_exception(), std::chrono::milliseconds(20));

    BOOST_REQUIRE_EXCEPTION(
      fut_process.get(), test_exception, [](const test_exception& e) {
          return "Process exception" == std::string(e.what());
      });

    auto fut_cancel = test_executor.submit(
      task_for_test_with_process_set_cancel_exception(),
      std::chrono::milliseconds(20));

    BOOST_REQUIRE_EXCEPTION(
      fut_cancel.get(), test_exception, [](const test_exception& e) {
          return "Set cancel() exception" == std::string(e.what());
      });

    auto fut_on_timeout = test_executor.submit(
      task_for_test_with_process_set_on_timeout_exception(),
      std::chrono::milliseconds(20));

    BOOST_REQUIRE_EXCEPTION(
      fut_on_timeout.get(), test_exception, [](const test_exception& e) {
          return "Set on_timeout() exception" == std::string(e.what());
      });

    test_executor.stop().get();
}

SEASTAR_THREAD_TEST_CASE(stop_executor_with_finish_item_test) {
    struct task_for_test {
        explicit task_for_test(char& is_finish)
          : _is_finish(is_finish) {}

        void operator()() {
            sleep(2);
            _is_finish = 1;
        }

        void cancel() {}

        void on_timeout() {}

        char& _is_finish;
    };

    v8_engine::executor test_executor(1, ss::smp::count);

    const size_t futures_count = 5;

    std::vector<char> finish_flags(futures_count, 0);
    std::vector<ss::future<>> futures;
    futures.reserve(futures_count);

    for (int i = 0; i < futures_count; ++i) {
        futures.emplace_back(test_executor.submit(
          task_for_test(finish_flags[i]), std::chrono::milliseconds(5000)));
    }

    // Allow executor complete task after stop
    ss::sleep(std::chrono::milliseconds(100)).get();
    test_executor.stop().get();

    auto res = ss::when_all(futures.begin(), futures.end()).get();

    for (auto i = 0; i < futures_count; ++i) {
        if (finish_flags[i] != 1) {
            BOOST_REQUIRE_EXCEPTION(
              add_future_handlers(res[i]).get(),
              test_exception,
              [](const test_exception& e) {
                  return "Executor is stopped" == std::string(e.what());
              });
        }
    }
}

SEASTAR_THREAD_TEST_CASE(stress_test) {
    struct task_for_test {
        explicit task_for_test(char& is_finish)
          : _is_finish(is_finish) {}

        void operator()() { _is_finish = 1; }

        void cancel() {}

        void on_timeout() {}

        char& _is_finish;
    };

    std::vector<size_t> queue_size = {1, 10, 1000};

    for (auto size : queue_size) {
        v8_engine::executor test_executor(1, size);

        std::unordered_map<size_t, char> finish_flags;
        std::vector<ss::future<>> futures;

        auto finish_time = ss::lowres_clock::now() + std::chrono::seconds(10);

        size_t it_count = 0;

        while (ss::lowres_clock::now() < finish_time) {
            if (it_count % 5 == 0) {
                char is_finish = 0;
                auto fut = test_executor.submit(
                  task_for_test(is_finish), std::chrono::milliseconds(100));
                fut.get();
                BOOST_REQUIRE_EQUAL(is_finish, 1);
            } else {
                finish_flags[it_count] = 0;
                futures.emplace_back(test_executor.submit(
                  task_for_test(finish_flags[it_count]),
                  std::chrono::milliseconds(5000)));
            }

            ++it_count;
        }

        auto futures_res = ss::when_all(futures.begin(), futures.end()).get();
        for (auto res : finish_flags) {
            BOOST_REQUIRE_EQUAL(res.second, 1);
        }

        test_executor.stop().get();
    }
}
