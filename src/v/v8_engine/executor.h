/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "seastarx.h"
#include "utils/concepts-enabled.h"
#include "utils/gate_guard.h"
#include "utils/mutex.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/internal/pollable_fd.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/later.hh>

#include <boost/lockfree/spsc_queue.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <mutex>

namespace v8_engine {

namespace internal {

// Abstract class for executor task
struct work_item {
    virtual ~work_item() {}
    virtual void process() noexcept = 0;
    virtual void cancel() noexcept = 0;
    virtual void on_timeout() noexcept = 0;
    virtual void set_exception(const std::exception_ptr& exception) {
        _exception = exception;
    };
    virtual std::chrono::milliseconds get_timeout() const noexcept = 0;

    virtual void done() = 0;

    std::exception_ptr _exception;
};

// This class implement task for executor. Contains promise logic
template<typename Func>
struct task final : work_item {
    task(Func&& f, std::chrono::milliseconds timeout)
      : work_item()
      , _func(std::forward<Func>(f))
      , _timeout(timeout) {}

    // Save first exception for task
    template<typename FunForExecute>
    void func_wrapper(FunForExecute&& func) {
        if (_exception) {
            return;
        }

        try {
            func();
        } catch (...) {
            set_exception(std::current_exception());
        }
    }

    // Process task from executor in executor thread and set state in ss::future
    void process() noexcept override {
        func_wrapper([this] { _func(); });
    }

    // Run this function for cancel task execution
    void cancel() noexcept override {
        func_wrapper([this] { _func.cancel(); });
    }

    // Run this function after wathdog was alarmed and execution was canceled.
    // For example: prepare inside task fields for next execution
    void on_timeout() noexcept override {
        func_wrapper([this] { _func.on_timeout(); });
    }

    std::chrono::milliseconds get_timeout() const noexcept override {
        return _timeout;
    }

    // Signal from std::thread to seastar about the end of task processing
    void done() override { _on_done.write_side().signal(1); }

    // Get future from seastar thread for waiting when executor thread will
    // complete task
    ss::future<> get_future() {
        return _on_done.wait().discard_result().then([this] {
            if (_exception) {
                return ss::make_exception_future(_exception);
            } else {
                return ss::now();
            }
        });
    }

    Func _func;
    seastar::readable_eventfd _on_done;

    std::chrono::milliseconds _timeout;
};

// This class implement queue for submit task from seastar to std::thread. Only
// one core with seastar can submit task to queue. Only one std::thread can
// consume task from queue.
class spsc_queue {
    static constexpr std::chrono::milliseconds _timeout_cond_wait_std_thread_ms{
      30};

public:
    explicit spsc_queue(size_t queue_size);

    // Close queue and break waiting on seastar mutex
    void close();

    // Push new item to queue. Only one future can wait on readable_eventfd
    ss::future<> push(work_item* item);

    // Pop element from queue (blocking)
    work_item* pop();

    bool empty();

private:
    mutex _push_mutex;
    seastar::readable_eventfd _is_not_full;

    std::atomic<bool> _is_stopped{false};

    boost::lockfree::spsc_queue<work_item*> _items;

    std::mutex _std_mutex;
    std::condition_variable _has_element_cv;
};

} // namespace internal

// This class implement executor (std::thread) for runing v8 script. It
// uses boost::spsc_queue for storing task.
// For use we need create ss::shared<executor> and run submit like
// executor_sharded.invoke_on(<core with executor>)
class executor {
public:
    executor(uint8_t cpu_id, size_t queue_size);

    executor(const executor& other) = delete;
    executor& operator=(const executor& other) = delete;
    executor(executor&& other) = delete;
    executor& operator=(executor&& other) = delete;

    ~executor() = default;

    // Stop executou. Stop queue with task and close gate
    ss::future<> stop();

    bool is_stopping() const;

    /// Submit new task in executor.
    ///
    /// \param start_func is used for set watchdog
    /// \param func_for_executor is v8 script

    // clang-format off
    template<typename WrapperFuncForExecutor>
    CONCEPT(requires requires(WrapperFuncForExecutor func) {
        { func() } -> std::same_as<void>;
        { func.cancel() } -> std::same_as<void>;
        { func.on_timeout() } -> std::same_as<void>;
    })
    // clang-format on

    ss::future<> submit(
      WrapperFuncForExecutor&& func_for_executor,
      std::chrono::milliseconds timeout) {
        gate_guard guard{_gate};

        auto new_task
          = std::make_unique<internal::task<WrapperFuncForExecutor>>(
            std::forward<WrapperFuncForExecutor>(func_for_executor), timeout);

        co_await _tasks.push(new_task.get());
        co_await new_task->get_future();
    }

private:
    // Set callback and rearm watchdog
    void rearm_watchdog(
      internal::work_item& task, std::chrono::milliseconds timeout);

    // Cancel watchdog
    void cancel_watchdog(internal::work_item& task);

    // We need to pin thread to core without seastar reactor
    void pin(unsigned cpu_id);

    // Main loop for threads in executor
    void loop();

    std::atomic<bool> _is_stopped{false};

    std::thread _thread;

    ss::gate _gate;
    internal::spsc_queue _tasks;

    ss::timer<ss::lowres_clock> _watchdog;

    ss::shard_id _watchdog_shard;
};

} // namespace v8_engine
