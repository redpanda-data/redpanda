/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "v8_engine/executor.h"

#include "hashing/xx.h"
#include "seastarx.h"
#include "utils/mutex.h"
#include "vassert.h"

#include <seastar/core/alien.hh>
#include <seastar/core/future.hh>
#include <seastar/core/smp.hh>

#include <exception>

namespace v8_engine {

namespace internal {

// spsc_queue

spsc_queue::spsc_queue(size_t queue_size)
  : _items(queue_size) {}

void spsc_queue::close() {
    _is_stopped = true;
    _push_mutex.broken();
    _has_element_cv.notify_all();
}

ss::future<> spsc_queue::push(work_item* item) {
    // Only one coroutine can wait on ss::readable_eventfd, so we need to use
    // mutex for push
    auto lock = co_await _push_mutex.get_units();

    while (!_items.push(item)) {
        co_await _is_not_full.wait();
    }

    _has_element_cv.notify_one();
}

work_item* spsc_queue::pop() {
    work_item* item = nullptr;
    std::unique_lock lock{_std_mutex};

    // We need to use wait_for, because std::thread can miss notification from
    // seastar thread (between check pred and sleep)
    _has_element_cv.wait_for(lock, _timeout_cond_wait_std_thread_ms, [this] {
        return !empty() || _is_stopped;
    });

    if (!empty()) {
        item = _items.front();
        _items.pop();
        _is_not_full.write_side().signal(1);
    }

    return item;
}

bool spsc_queue::empty() { return _items.empty(); }

} // namespace internal

// executor

executor::executor(
  ss::alien::instance& instance, uint8_t cpu_id, size_t queue_size)
  : _alien_instance(instance)
  , _thread([this, cpu_id] {
      pin(cpu_id);
      loop();
  })
  , _tasks(queue_size)
  , _watchdog_shard(ss::this_shard_id()) {}

ss::future<> executor::stop() {
    _tasks.close();
    return _gate.close().then([this] {
        _is_stopped = true;
        _thread.join();
    });
}

bool executor::is_stopping() const {
    return _is_stopped.load(std::memory_order_relaxed);
}

void executor::rearm_watchdog(
  internal::work_item& task, std::chrono::milliseconds timeout) {
    // We need to reset callback for each item in queue. Because we have only
    // one timer for all tasks in executor.
    _watchdog.set_callback([&task] { task.cancel(); });

    _watchdog.rearm(
      ss::lowres_clock::time_point(ss::lowres_clock::now() + timeout));
}

void executor::cancel_watchdog(internal::work_item& task) {
    if (!_watchdog.cancel()) {
        task.on_timeout();
    }
}

void executor::pin(unsigned cpu_id) {
    cpu_set_t cs;
    CPU_ZERO(&cs);
    CPU_SET(cpu_id, &cs);
    auto r = pthread_setaffinity_np(pthread_self(), sizeof(cs), &cs);
    vassert(r == 0, "Can not pin executor thread to core {}", cpu_id);
}

void executor::loop() {
    while (!(is_stopping() && _tasks.empty())) {
        internal::work_item* item = _tasks.pop();

        if (item) {
            ss::alien::submit_to(
              _alien_instance,
              _watchdog_shard,
              [this, item] {
                  rearm_watchdog(*item, item->get_timeout());
                  return ss::now();
              })
              .wait();

            item->process();

            ss::alien::submit_to(
              _alien_instance,
              _watchdog_shard,
              [this, item] {
                  cancel_watchdog(*item);
                  return ss::now();
              })
              .wait();

            item->done();
        }
    }
}

ss::future<>
executor_service::start(ss::alien::instance& instance, int64_t size) {
    co_await _executor.start_single(
      std::ref(instance), _core_for_executor_thread, size);
    co_await _code_database.start_single();
}

ss::future<> executor_service::stop() {
    co_await _code_database.stop();
    co_await _executor.stop();
}

ss::future<>
executor_service::insert_or_assign(coproc::script_id id, iobuf code) {
    auto iobuf_ptr = std::make_unique<iobuf>(std::move(code));
    auto code_ptr = ss::make_foreign<std::unique_ptr<iobuf>>(
      std::move(iobuf_ptr));
    return _code_database.invoke_on(
      _home_core, [id, code_ptr = std::move(code_ptr)](code_database_t& db) {
          db.insert_or_assign(id, code_ptr.get()->copy());
      });
}

ss::future<> executor_service::erase(coproc::script_id id) {
    return _code_database.invoke_on(
      _home_core, [id](code_database_t& db) { db.erase(id); });
}

ss::future<std::optional<iobuf>>
executor_service::get_code(std::string_view name) {
    // rpk use xxhash_64 for create script_is from script name
    size_t id(xxhash_64(name.data(), name.size()));
    using return_type = std::unique_ptr<iobuf>;
    auto result = co_await _code_database.invoke_on(
      _home_core,
      [id](
        const code_database_t& db) -> ss::future<ss::foreign_ptr<return_type>> {
          std::unique_ptr<iobuf> res;
          auto code = db.find(coproc::script_id(id));
          if (code == db.end()) {
              res = std::unique_ptr<iobuf>(nullptr);
          } else {
              res = std::make_unique<iobuf>(code->second.copy());
          }

          co_return ss::make_foreign<return_type>(std::move(res));
      });

    if (result.get() == nullptr) {
        co_return std::nullopt;
    } else {
        co_return result.get()->copy();
    }
}

} // namespace v8_engine
