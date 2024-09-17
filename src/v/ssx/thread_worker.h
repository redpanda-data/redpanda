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

#pragma once

#include "base/seastarx.h"
#include "base/vassert.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/alien.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/internal/pollable_fd.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/posix.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

#include <boost/lockfree/spsc_queue.hpp>
#include <sys/eventfd.h>

#include <exception>
#include <memory>
#include <sched.h>
#include <span>
#include <thread>
#include <type_traits>

namespace ssx {

namespace impl {

class task_base {
public:
    task_base() = default;
    task_base(task_base&&) = delete;
    task_base(const task_base&) = delete;
    task_base& operator=(task_base&&) = delete;
    task_base& operator=(const task_base&) = delete;

    virtual ss::stop_iteration process(ss::alien::instance&, ss::shard_id) = 0;
    virtual void
    set_exception(ss::alien::instance&, ss::shard_id, std::exception_ptr)
      = 0;
    virtual ~task_base() = default;
};

class stop_task final : public task_base {
public:
    ss::future<> get_future() noexcept { return _promise.get_future(); }

    ss::stop_iteration
    process(ss::alien::instance& alien, ss::shard_id shard) final {
        ss::alien::run_on(
          alien, shard, [this]() noexcept { _promise.set_value(); });
        return ss::stop_iteration::yes;
    }

    void set_exception(
      ss::alien::instance& alien,
      ss::shard_id shard,
      std::exception_ptr p) final {
        ss::alien::run_on(
          alien, shard, [this, p]() noexcept { _promise.set_exception(p); });
    }

private:
    ss::promise<> _promise;
};

template<typename Func>
class worker_task final : public task_base {
    using value_type = std::invoke_result_t<Func>;

public:
    explicit worker_task(Func func)
      : _func{std::move(func)} {}

    ss::future<value_type> get_future() noexcept {
        return _promise.get_future();
    }

    ss::stop_iteration
    process(ss::alien::instance& alien, ss::shard_id shard) final {
        try {
            if constexpr (std::is_void_v<value_type>) {
                _func();
                ss::alien::run_on(
                  alien, shard, [this]() noexcept { _promise.set_value(); });
            } else {
                auto v = _func();
                ss::alien::run_on(
                  alien, shard, [this, v{std::move(v)}]() mutable noexcept {
                      _promise.set_value(std::move(v));
                  });
            }
        } catch (...) {
            set_exception(alien, shard, std::current_exception());
        };
        return ss::stop_iteration::no;
    }

    void set_exception(
      ss::alien::instance& alien,
      ss::shard_id shard,
      std::exception_ptr p) final {
        ss::alien::run_on(
          alien, shard, [this, p]() noexcept { _promise.set_exception(p); });
    }

private:
    Func _func;
    typename ss::promise<value_type> _promise;
};

class thread_worker {
public:
    struct config {
        // If the thread should be pinned to a the same core as the shard it was
        // created on.
        bool pin_to_shard_core = false;
        // NOTE: pthread names must be less than 12 bytes
        //
        // Why 12 bytes? pthread only support names of length 16 and we want to
        // suffix threads with the core it's associated with via `-xxx`.
        ss::sstring name = "thread";
    };

    explicit thread_worker()
      : _alien(&ss::engine().alien())
      , _cpu_id(::sched_getcpu())
      , _shard_id(ss::this_shard_id()) {}

    void start(config c) {
        _worker = std::thread([this, c = std::move(c)]() mutable {
            configure_thread(std::move(c));
            run();
        });
    }

    ss::future<> stop() {
        co_await _gate.close();
        if (_worker.joinable()) {
            auto task = impl::stop_task();
            vassert(
              _queue.push(&task),
              "expected to be able to push a task onto the queue");
            _ready.signal(1);
            co_await task.get_future();
            _worker.join();
        }
    }

    template<typename Func>
    auto submit(Func func) ->
      typename ss::futurize<std::invoke_result_t<Func>>::type {
        auto gh = _gate.hold();
        auto units = co_await ss::get_units(_semaphore, 1);
        auto task = impl::worker_task<Func>(std::move(func));
        vassert(
          _queue.push(&task),
          "expected to be able to push a task onto the queue");
        _ready.signal(1);
        co_return co_await task.get_future();
    }

private:
    void configure_thread(config c) {
        auto name = ss::format("{}-{}", c.name, _shard_id);
        ss::throw_pthread_error(
          ::pthread_setname_np(::pthread_self(), name.c_str()));
        if (c.pin_to_shard_core) {
            cpu_set_t cs;
            CPU_ZERO(&cs);
            CPU_SET(_cpu_id, &cs);
            ss::throw_pthread_error(
              pthread_setaffinity_np(pthread_self(), sizeof(cs), &cs));
        }
        // Ignore all signals - let seastar handle them
        sigset_t mask;
        ::sigfillset(&mask);
        ss::throw_pthread_error(::pthread_sigmask(SIG_BLOCK, &mask, nullptr));
    }

    void run() {
        std::array<impl::task_base*, max_length> items{};
        while (true) {
            eventfd_t result = 0;
            auto r = ::eventfd_read(_ready.get_read_fd(), &result);
            while (!_queue.empty()) {
                size_t num_tasks = _queue.pop(items.data(), items.size());
                auto stop_iteration = process_tasks({items.data(), num_tasks});
                if (stop_iteration == ss::stop_iteration::yes) {
                    // stop was requested, we can be done
                    vassert(
                      _queue.empty(),
                      "expected a stop task to be the last task in the task "
                      "queue, but there were {} available to process.",
                      _queue.read_available());
                    return;
                }
            }
            if (r < 0) {
                return;
            }
        }
    }

    ss::stop_iteration process_tasks(std::span<impl::task_base*> tasks) {
        for (size_t i = 0; i < tasks.size(); ++i) {
            auto stop_iteration = tasks[i]->process(*_alien, _shard_id);
            if (stop_iteration == ss::stop_iteration::yes) {
                fail_tasks(tasks.subspan(i + 1));
                return ss::stop_iteration::yes;
            }
        }
        return ss::stop_iteration::no;
    }

    void fail_tasks(std::span<impl::task_base*> tasks) {
        for (auto* task : tasks) {
            task->set_exception(
              *_alien,
              _shard_id,
              std::make_exception_ptr(ss::abort_requested_exception{}));
        }
    }

    ss::alien::instance* _alien;
    unsigned _cpu_id;
    ss::shard_id _shard_id;
    std::thread _worker;
    ss::gate _gate;
    ss::writeable_eventfd _ready;

    constexpr static size_t max_length = 128;
    ss::semaphore _semaphore = ss::semaphore(max_length);
    boost::lockfree::
      spsc_queue<impl::task_base*, boost::lockfree::capacity<max_length>>
        _queue;
};

} // namespace impl

/**
 * singleton_thread_worker runs tasks in a std::thread, it's expected that only
 * a single core manages the lifetime of this shard, and all other shards submit
 * their requests through that shard.
 *
 * By running in a std::thread, it's possible to make blocking calls such as
 * file I/O and posix thread primitives without blocking a reactor.
 *
 * The thread worker will drain all operations before joining the thread in
 * stop(), but it should be noted that joining a thread may block. As such, this
 * class is most suited to run for the lifetime of an application, rather than
 * short-lived.
 */
class singleton_thread_worker {
public:
    static constexpr ss::shard_id shard_id = 0;
    struct config {
        // NOTE: pthread names must be less than 12 bytes
        //
        // Why 12 bytes? pthread only support names of length 16 and we want to
        // suffix threads with the core it's associated with via `-xxx`.
        ss::sstring name = "thread";
    };

    /**
     * start the background thread.
     */
    ss::future<> start(config c) {
        vassert(
          ss::this_shard_id() == shard_id,
          "thread_worker must be started on shard ",
          shard_id);
        co_await _gate.start();
        _impl.start({.name = std::move(c.name)});
    }

    /**
     * stop and join the background thread.
     *
     * Although the work has completed, it should be noted that joining a thread
     * may block the reactor.
     */
    ss::future<> stop() {
        vassert(
          ss::this_shard_id() == shard_id,
          "thread_worker must be stopped on shard ",
          shard_id);

        co_await _gate.invoke_on_all(&ss::gate::close);
        co_await _impl.stop();
        co_await _gate.stop();
    }

    /**
     * submit a task to the background thread
     */
    template<typename Func>
    auto submit(Func func) ->
      typename ss::futurize<std::invoke_result_t<Func>>::type {
        return ss::with_gate(
          _gate.local(), [this, func{std::move(func)}]() mutable {
              return ss::smp::submit_to(
                shard_id, [this, func{std::move(func)}]() mutable {
                    return _impl.submit(std::move(func));
                });
          });
    }

private:
    ss::sharded<ss::gate> _gate;
    impl::thread_worker _impl;
};

/**
 * sharded_thread_worker runs tasks in a std::thread, and creates a thread core
 * each reactor core so that there is a twin std::thread for each reactor core.
 *
 * By running in a std::thread, it's possible to make blocking calls such as
 * file I/O and posix thread primitives without blocking a reactor.
 *
 * The thread worker will drain all operations before joining the thread in
 * stop(), but it should be noted that joining a thread may block. As such, this
 * class is most suited to run for the lifetime of an application, rather than
 * short-lived.
 */
class sharded_thread_worker {
public:
    struct config {
        // NOTE: pthread names must be less than 12 bytes
        //
        // Why 12 bytes? pthread only support names of length 16 and we want to
        // suffix threads with the core it's associated with via `-xxx`.
        ss::sstring name = "thread";
    };

    /**
     * start the background thread.
     */
    ss::future<> start(config c) {
        co_await _impl.start();
        co_await _impl.invoke_on_all([&c](impl::thread_worker& w) {
            return w.start({.pin_to_shard_core = true, .name = c.name});
        });
    }

    /**
     * stop and join the background thread.
     *
     * Although the work has completed, it should be noted that joining a thread
     * may block the reactor.
     */
    ss::future<> stop() { co_await _impl.stop(); }

    /**
     * submit a task to the background thread
     */
    template<typename Func>
    auto submit(Func func) ->
      typename ss::futurize<std::invoke_result_t<Func>>::type {
        return _impl.local().submit(std::move(func));
    }

private:
    ss::sharded<impl::thread_worker> _impl;
};

} // namespace ssx
