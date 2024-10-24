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
#include "base/seastarx.h"
#include "ssx/semaphore.h"

#include <seastar/core/rwlock.hh>

namespace ssx {

class rwlock;

class rwlock_unit {
    rwlock* _lock;
    bool _is_write_lock{false};

    rwlock_unit(rwlock* lock, bool is_write_lock) noexcept
      : _lock(lock)
      , _is_write_lock(is_write_lock) {}

    friend class rwlock;

public:
    rwlock_unit(rwlock_unit&& token) noexcept
      : _lock(token._lock)
      , _is_write_lock(token._is_write_lock) {
        token._lock = nullptr;
    }
    rwlock_unit(const rwlock_unit&) = delete;
    rwlock_unit(const rwlock_unit&&) = delete;
    rwlock_unit() = delete;

    explicit operator bool() const noexcept { return _lock != nullptr; }

    rwlock_unit& operator=(rwlock_unit&& other) noexcept {
        if (this != &other) {
            this->_lock = other._lock;
            other._lock = nullptr;
        }
        return *this;
    }

    ~rwlock_unit() noexcept;
};

class rwlock : public ss::basic_rwlock<> {
public:
    std::optional<rwlock_unit> attempt_read_lock() {
        bool locked = try_read_lock();

        if (!locked) {
            return std::nullopt;
        }

        return rwlock_unit(this, false);
    }

    std::optional<rwlock_unit> attempt_write_lock() {
        bool locked = try_write_lock();

        if (!locked) {
            return std::nullopt;
        }

        return rwlock_unit(this, true);
    }
};

inline rwlock_unit::~rwlock_unit() noexcept {
    if (_lock) {
        if (_is_write_lock) {
            _lock->write_unlock();
        } else {
            _lock->read_unlock();
        }
    }
}

} // namespace ssx
