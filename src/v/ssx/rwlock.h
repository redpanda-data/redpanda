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

#include "seastarx.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/log.hh>

using namespace std::chrono_literals;

namespace ssx {

// Wrapper around seastar::rwlock, meant to be used as a drop-in replacement.
// Logs an error if the lock has waited a long time to take.
class logging_rwlock {
public:
    using Clock = ss::lowres_clock;
    using holder_id = int64_t;
    using resource_str_func = std::function<ss::sstring()>;
    static constexpr Clock::duration default_log_timeout = 10000ms;

    // Wrapper around seastar::rwlock::holder that is associated with a
    // specific holder_id, and is able to log about the resource being locked.
    struct holder {
    public:
        holder() = delete;
        holder(const holder&) = delete;
        holder operator=(holder&) = delete;

        holder(logging_rwlock* lock, ss::rwlock::holder&& h, holder_id id)
          : _lock(lock)
          , _h(std::move(h))
          , _id(id) {}
        holder(holder&& o) noexcept
          : _lock(std::exchange(o._lock, nullptr))
          , _h(std::move(o._h))
          , _id(o._id) {}
        holder& operator=(holder&& o) noexcept {
            _lock = std::exchange(o._lock, nullptr);
            _h = std::move(o._h);
            _id = o._id;
            return *this;
        }
        ~holder() {
            if (_lock) {
                vlog(
                  _lock->_logger.trace,
                  "Dropping lock on {} ({})",
                  _lock->_resource_str_gen(),
                  _id);
            }
        }

    private:
        logging_rwlock* _lock;
        ss::rwlock::holder _h;
        holder_id _id;
    };

    explicit logging_rwlock(ss::logger& logger)
      : _logger(logger) {}

    explicit logging_rwlock(ss::logger& logger, resource_str_func fn)
      : _logger(logger)
      , _resource_str_gen(std::move(fn)) {}

    ss::future<holder> hold_read_lock(
      ss::semaphore::time_point take_deadline
      = ss::semaphore::time_point::max(),
      Clock::duration log_timeout = default_log_timeout) {
        ss::timer<Clock> log_timer;
        auto [holder_id, start_time] = log_and_start_timer(
          "Read", log_timeout, log_timer);
        auto h = co_await _lock.hold_read_lock(take_deadline);
        log_and_stop_timer("Read", holder_id, start_time, log_timer);
        co_return holder(this, std::move(h), holder_id);
    }

    ss::future<holder> hold_write_lock(
      ss::semaphore::time_point take_deadline
      = ss::semaphore::time_point::max(),
      Clock::duration log_timeout = default_log_timeout) {
        ss::timer<Clock> log_timer;
        auto [holder_id, start_time] = log_and_start_timer(
          "Write", log_timeout, log_timer);
        auto h = co_await _lock.hold_write_lock(take_deadline);
        log_and_stop_timer("Write", holder_id, start_time, log_timer);
        co_return holder(this, std::move(h), holder_id);
    }

    bool locked() const { return _lock.locked(); }

    bool try_write_lock() {
        auto ret = _lock.try_write_lock();
        if (ret) {
            vlog(
              _logger.trace,
              "Write lock on {} acquired immediately",
              _resource_str_gen());
        }
        return ret;
    }

    void write_unlock() {
        vlog(_logger.trace, "Dropping write lock on {}", _resource_str_gen());
        _lock.write_unlock();
    }

private:
    friend struct holder;

    // Starts a timer that will log if log_and_stop_timer() isn't called within
    // the log timeout. Should be called immediately before locking.
    std::pair<holder_id, Clock::time_point> log_and_start_timer(
      const char* acquire_type,
      Clock::duration log_timeout,
      ss::timer<Clock>& log_timer) {
        auto id = _next_id++;
        const auto start_time = Clock::now();
        log_timer.set_callback([id, start_time, acquire_type, this] {
            vlog(
              _logger.error,
              "{} lock on {} ({}) has taken {}ns so far",
              acquire_type,
              _resource_str_gen(),
              id,
              (Clock::now() - start_time).count());
        });
        log_timer.arm(log_timeout);
        vlog(
          _logger.trace,
          "{} lock on {} ({}) being acquired",
          acquire_type,
          _resource_str_gen(),
          id);
        return {id, start_time};
    }

    // Cancels the timer and logs a message indicating how long it took to
    // acquire the lock. Should be called immediately after acquiring the lock.
    void log_and_stop_timer(
      const char* acquire_type,
      holder_id id,
      Clock::time_point start_time,
      ss::timer<Clock>& log_timer) {
        log_timer.cancel();
        auto acquire_duration = Clock::now() - start_time;
        vlog(
          _logger.trace,
          "{} lock on {} ({}) acquired after {}ns",
          acquire_type,
          _resource_str_gen(),
          id,
          acquire_duration.count());
    }

    ss::logger& _logger;
    ss::rwlock _lock;
    const resource_str_func _resource_str_gen;
    holder_id _next_id{0};
};

} // namespace ssx
