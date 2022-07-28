/*
 * Copyright 2020 Redpanda Data, Inc.
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
#include "ssx/semaphore.h"

#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>

#include <utility>

/*
 * This is a extension of mutex.h. The primary motivation for this class
 * is to gather statistics on how ofter the mutex is taken (lock_counter)
 * and the id of the top lock holder.
 *
 * Usage
 * =====
 *
 *    ```
 *    timed_mutex m;
 *    return m.with("timed_mutex_example", [] { ... });
 *    ```
 * The first argument of `with` and `lock` is const char* lock_holder_id.
 * timed_mutex doesn't make any copy of the string, just pass the pointer
 * around and may store it indefinely long so it should have unlimited
 * lifetime. From the caller perspective it means they can't really pass
 * in a dynamic or stack-allocated string: it should really only be a string
 * literal. This is important for performance to avoid allocating on every
 * lock()
 */

class timed_mutex;

class locked_token {
    timed_mutex* _mutex;
    const char* _lock_holder_id;
    ssx::semaphore_units _unit;
    bool _is_moved{false};

public:
    locked_token(
      timed_mutex* mutex,
      const char* lock_holder_id,
      ssx::semaphore_units&& unit) noexcept
      : _mutex(mutex)
      , _lock_holder_id(lock_holder_id)
      , _unit(std::move(unit)) {}
    locked_token(locked_token&& token) noexcept
      : _mutex(token._mutex)
      , _lock_holder_id(token._lock_holder_id)
      , _unit(std::move(token._unit)) {
        token._is_moved = true;
    }
    locked_token(const locked_token&) = delete;
    locked_token() = delete;

    ~locked_token() noexcept;
};

struct lock_trace {
    std::chrono::microseconds duration{0};
    const char* lock_holder_id{nullptr};
};

class timed_mutex {
public:
    using duration = typename ss::semaphore::duration;
    using time_point = typename ss::semaphore::time_point;

    explicit timed_mutex(bool should_tracing)
      : _sem(1, "timed-mutex")
      , _is_tracing(should_tracing) {}

    ss::future<> start_tracing() {
        return this->lock("timed_mutex:start_tracing")
          .then([this](locked_token t) {
              if (!this->_is_tracing) {
                  this->_is_tracing = true;
                  _lock_counter += 1;
                  _acquired_at = std::chrono::steady_clock::now();
              }
          });
    }

    ss::future<> stop_tracing() {
        return this->lock("timed_mutex:stop_tracing")
          .then([this](locked_token t) { this->_is_tracing = false; });
    }

    template<typename Func>
    auto
    with(const char* lock_holder_id, duration timeout, Func&& func) noexcept {
        return ss::with_semaphore(
          _sem,
          1,
          timeout,
          [this, lock_holder_id, func = std::forward<Func>(func)]() mutable {
              if (_is_tracing) {
                  _lock_counter += 1;
                  _acquired_at = std::chrono::steady_clock::now();
              }
              return ss::futurize_invoke(std::forward<Func>(func))
                .finally(
                  [this, lock_holder_id] { about_to_release(lock_holder_id); });
          });
    }

    template<typename Func>
    auto
    with(const char* lock_holder_id, time_point timeout, Func&& func) noexcept {
        return ss::get_units(_sem, 1, timeout)
          .then([this, lock_holder_id, func = std::forward<Func>(func)](
                  auto units) mutable {
              if (_is_tracing) {
                  _lock_counter += 1;
                  _acquired_at = std::chrono::steady_clock::now();
              }
              return ss::futurize_invoke(std::forward<Func>(func))
                .finally([this, lock_holder_id, units = std::move(units)] {
                    about_to_release(lock_holder_id);
                });
          });
    }

    ss::future<locked_token> lock(const char* lock_holder_id) {
        return ss::get_units(_sem, 1).then(
          [this, lock_holder_id](ssx::semaphore_units u) {
              if (_is_tracing) {
                  _lock_counter += 1;
                  _acquired_at = std::chrono::steady_clock::now();
              }
              return locked_token(this, lock_holder_id, std::move(u));
          });
    }

    lock_trace get_top_leasee() { return _top_leasee; }

    int get_lock_counter() { return _lock_counter; }

    void reset_statistics() {
        _lock_counter = 0;
        _top_leasee = lock_trace{std::chrono::microseconds{0}, nullptr};
    }

private:
    ssx::semaphore _sem;
    bool _is_tracing{false};
    int64_t _lock_counter{0};
    std::chrono::steady_clock::time_point _acquired_at;
    lock_trace _top_leasee;

    void about_to_release(const char* lock_holder_id) {
        auto duration_us
          = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - _acquired_at);

        if (duration_us > _top_leasee.duration) {
            _top_leasee.duration = duration_us;
            _top_leasee.lock_holder_id = lock_holder_id;
        }
    }

    friend class locked_token;
};

inline locked_token::~locked_token() noexcept {
    if (!_is_moved) {
        _mutex->about_to_release(_lock_holder_id);
    }
}
