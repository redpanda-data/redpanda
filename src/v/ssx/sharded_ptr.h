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
#include "base/vassert.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include <memory>

namespace ssx {

/// A pointer that is shared between shards.
/// reset() must be called on the home shard.
/// The pointer is stable until the fiber yields.
template<typename T>
class sharded_ptr {
public:
    sharded_ptr()
      : _shard{ss::this_shard_id()} {}
    ~sharded_ptr() {
        if (*this) {
            std::default_delete<T>{}(_state.local());
        }
    }

    sharded_ptr(sharded_ptr&& other) noexcept = delete;
    sharded_ptr(sharded_ptr const&) noexcept = delete;
    sharded_ptr& operator=(sharded_ptr const&) = delete;
    sharded_ptr& operator=(sharded_ptr&&) = delete;

    T& operator*() const { return *_state.local(); }
    T* operator->() const { return _state.local(); }
    explicit operator bool() const {
        return _state.local_is_initialized() && _state.local() != nullptr;
    }

    ss::future<> reset(std::unique_ptr<T> u = nullptr) {
        assert_shard();
        auto up = u.get();

        if (!_state.local_is_initialized()) {
            if (u == nullptr) {
                co_return;
            }
            co_await _state.start(up).finally(
              [this, u{std::move(u)}]() mutable {
                  if (u.get() == _state.local()) {
                      void(u.release());
                  }
              });

            co_return;
        }

        co_await _state
          .invoke_on_others(
            [up, gh{_gate.hold()}](auto& state) noexcept { state = up; })
          .finally([this, u{std::move(u)}]() mutable noexcept {
              std::default_delete<T>{}(_state.local());
              _state.local() = u.release();
          });
    }

    template<typename... Args>
    ss::future<> reset(Args&&... args) {
        return reset(std::make_unique<T>(std::forward<Args>(args)...));
    }

    ss::future<> stop() {
        co_await reset();
        co_await _gate.close();
        co_await _state.stop();
    }

private:
    void assert_shard() const {
        vassert(
          ss::this_shard_id() == _shard,
          "reset must be called on home shard: ",
          _shard);
    }
    unsigned int _shard;
    ss::sharded<T*> _state;
    ss::gate _gate;
};

} // namespace ssx
