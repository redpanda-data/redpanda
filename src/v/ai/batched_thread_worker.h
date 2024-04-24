/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "base/vassert.h"

#include <seastar/core/alien.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/smp.hh>

#include <absl/container/fixed_array.h>

namespace ai {

constexpr static size_t default_max_queue_length = 128;

/**
 * An alien thread that can control consumption of work.
 */
template<
  typename Parameters,
  typename Request,
  typename Response,
  size_t max_length = default_max_queue_length>
class batched_thread_worker {
public:
    // An item in the queue that hold the resolving promise and the input data.
    //
    // This is allocated on the seastar thead and a pointer is given to the
    // alien thread.
    class work_item final {
    public:
        explicit work_item(Request parameter)
          : _parameter(std::move(parameter)) {}

        Request take_parameter() noexcept { return std::move(_parameter); }

    private:
        ss::future<Response> get_future() noexcept {
            return _promise.get_future();
        }

        void set_value(
          ss::alien::instance* alien, ss::shard_id shard, Response r) noexcept {
            // run_on only enqueues a task so move the memory for the result
            // into the reactor thread to free.
            ss::alien::run_on(
              *alien, shard, [this, r = std::move(r)]() mutable noexcept {
                  _promise.set_value(std::move(r));
              });
        }

    private:
        friend class batched_thread_worker;
        Request _parameter;
        ss::promise<Response> _promise;
    };

    struct config {
        // NOTE: pthread names must be less than 12 bytes
        //
        // Why 12 bytes? pthread only support names of length 16 and we want to
        // suffix threads with the core it's associated with via `-xxx`.
        ss::sstring name = "thread";
        Parameters parameters;
    };

    batched_thread_worker()
      : _alien(&ss::engine().alien())
      , _shard_id(ss::this_shard_id()) {}
    batched_thread_worker(const batched_thread_worker&) = delete;
    batched_thread_worker(batched_thread_worker&&) = delete;
    batched_thread_worker& operator=(const batched_thread_worker&) = delete;
    batched_thread_worker& operator=(batched_thread_worker&&) = delete;
    virtual ~batched_thread_worker() = default;

    ss::future<> start(config c) {
        ss::promise<> p;
        _worker = std::thread([this, c = std::move(c), &p]() mutable {
            configure_thread(std::move(c.name));
            initialize(std::move(c.parameters));
            ss::alien::run_on(
              *_alien, _shard_id, [&p]() mutable noexcept { p.set_value(); });
            run();
            deinitialize();
        });
        co_await p.get_future();
    }

    ss::future<> stop() {
        co_await _gate.close();
        if (_worker.joinable()) {
            vassert(
              _queue.push(nullptr),
              "expected to be able to push a task onto the queue");
            _ready.signal(1);
            // This can cause reactor stalls, but this should only happen on
            // shutdown so meh, we could do a similar trick like the ssx workers
            // to prevent that.
            _worker.join();
        }
    }

    ss::future<Response> submit(Request p) {
        auto gh = _gate.hold();
        auto units = co_await ss::get_units(_semaphore, 1);
        auto task = work_item(std::move(p));
        vassert(
          _queue.push(&task),
          "expected to be able to push a task onto the queue");
        _ready.signal(1);
        co_return co_await task.get_future();
    }

protected:
    /**
     * The function to implement setup.
     */
    virtual void initialize(Parameters) noexcept = 0;
    /**
     * The function to implement work. Call `pop_n` to get more work items and
     * `send_response` to complete work.
     */
    virtual void process() noexcept = 0;
    /**
     * The function to implement shutdown.
     */
    virtual void deinitialize() noexcept = 0;

    /**
     * Take up to `limit` work items from the queue if no tasks are available
     * this does *not* wait.
     */
    std::vector<work_item*> pop_n(size_t limit) noexcept {
        std::vector<work_item*> items(limit);
        size_t num_tasks = _queue.pop(items.data(), items.size());
        items.resize(num_tasks);
        if (num_tasks == 1 && items.front() == nullptr) [[unlikely]] {
            _done = true;
            return {};
        }
        return items;
    }
    /**
     * Popped a single item, or nullptr if there is none.
     */
    work_item* pop() noexcept {
        work_item* item = nullptr;
        bool popped = _queue.pop(item);
        if (popped && item == nullptr) [[unlikely]] {
            _done = true;
        }
        return item;
    }

    /**
     * Respond to this work item, `item` should not be used after this, as it
     * could now be freed at any time.
     */
    void send_response(work_item* item, Response r) noexcept {
        item->set_value(_alien, _shard_id, std::move(r));
    }

private:
    void configure_thread(const ss::sstring& name) {
        auto thread_name = ss::format("{}-{}", name, _shard_id);
        ss::throw_pthread_error(
          ::pthread_setname_np(::pthread_self(), thread_name.c_str()));
        // Ignore all signals - let seastar handle them
        sigset_t mask;
        ::sigfillset(&mask);
        ss::throw_pthread_error(::pthread_sigmask(SIG_BLOCK, &mask, nullptr));
    }

    void run() {
        while (!_done) {
            eventfd_t result = 0;
            auto r = ::eventfd_read(_ready.get_read_fd(), &result);
            while (!_queue.empty()) {
                process();
            }
            if (r < 0) {
                return;
            }
        }
    }

    // Seastar thread state:
    std::thread _worker;
    ss::gate _gate;
    ss::writeable_eventfd _ready;
    ss::semaphore _semaphore = ss::semaphore(max_length);

    // Alien thread state:
    ss::alien::instance* _alien;
    ss::shard_id _shard_id;
    bool _done = false;

    // Shared state:
    boost::lockfree::
      spsc_queue<work_item*, boost::lockfree::capacity<max_length>>
        _queue;
};

} // namespace ai
