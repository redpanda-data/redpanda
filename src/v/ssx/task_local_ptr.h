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

#include "base/vassert.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/task.hh>
#include <seastar/core/thread.hh>

#include <stdexcept>

namespace ssx {

/// Exception that indicates that the accessed
/// task_local_ptr object is invalidated.
class task_local_ptr_invalidated : public std::runtime_error {
public:
    task_local_ptr_invalidated()
      : std::runtime_error("task_local_ptr is invalidated") {}
};

/// The value that gets invalidated after first scheduling point.
///
/// Any attempt to access the value after invalidation
/// will trigger 'task_local_invalidated' exception.
template<class T>
class task_local {
    static uint64_t get_current_task_cnt() {
        return seastar::engine().get_sched_stats().tasks_processed;
    }

public:
    explicit task_local(const T& val)
      : _val(val)
      , _task_cnt(get_current_task_cnt()) {}

    explicit task_local(T&& val)
      : _val(val)
      , _task_cnt(get_current_task_cnt()) {}

    task_local() = delete;
    ~task_local() = default;
    task_local(const task_local& other) = default;
    task_local(task_local&&) noexcept = default;
    task_local& operator=(const task_local&) = default;
    task_local& operator=(task_local&&) noexcept = default;

    bool operator==(const task_local& other) const {
        throw_on_bad_access();
        return _val == other._val;
    }

    bool operator==(const T& other) const {
        throw_on_bad_access();
        return _val == other;
    }

    const T& value() const {
        check_time_slice();
        throw_on_bad_access();
        return _val.value();
    }

    T& value() {
        check_time_slice();
        throw_on_bad_access();
        return _val.value();
    }

    bool has_value() const {
        check_time_slice();
        return _val.has_value();
    }

private:
    /// Check if the time slice is the same. Invalidate
    /// the pointer if its not.
    void check_time_slice() const {
        if (get_current_task_cnt() != _task_cnt) {
            // Opt-out from all checks if we don't have a valid pointer to
            // the current task.
            _val = std::nullopt;
        }
    }

    void throw_on_bad_access() const {
        if (!_val.has_value()) {
            throw task_local_ptr_invalidated();
        }
    }

    mutable std::optional<T> _val;
    uint64_t _task_cnt;
};

template<class T, class... Args>
auto make_task_local(Args&&... args) {
    return task_local<T>(T((std::forward<Args>(args))...));
}

/// Non-owning smart pointer.
/// Gets invalidated after first scheduling point.
///
/// Any attempt to access the object after invalidation
/// will trigger 'task_local_invalidated' exception.
template<class T>
class task_local_ptr {
public:
    explicit task_local_ptr(T* val)
      : _ref(val) {}

    task_local_ptr()
      : _ref(nullptr) {}

    task_local_ptr(const task_local_ptr&) = default;
    task_local_ptr(task_local_ptr&&) noexcept = default;
    task_local_ptr& operator=(const task_local_ptr&) = default;
    task_local_ptr& operator=(task_local_ptr&&) noexcept = default;
    ~task_local_ptr() = default;

    bool operator==(const task_local_ptr& other) const {
        return _ref == other._ref;
    }

    bool operator==(const T* other) const { return _ref.value() == other; }

    const T* operator->() const { return _ref.value(); }

    T* operator->() { return _ref.value(); }

    const T& operator*() const { return *_ref.value(); }

    T& operator*() { return *_ref.value(); }

    const T& value() const { return *_ref.value(); }

    T& value() { return *_ref.value(); }

    bool has_value() const { return _ref.has_value(); }

private:
    task_local<T*> _ref;
};

} // namespace ssx
