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

#include <seastar/core/shared_ptr.hh>

#include <bits/stdint-uintn.h>

#include <chrono>
#include <memory>

namespace rpc {

class backoff_policy final {
public:
    struct impl {
        virtual void next_backoff() = 0;

        virtual std::chrono::milliseconds current_backoff_duration() = 0;

        virtual void reset() = 0;

        virtual ~impl() noexcept = default;
    };

    explicit backoff_policy(std::unique_ptr<impl> i)
      : _impl(std::move(i)) {}

    void next_backoff() { _impl->next_backoff(); }

    std::chrono::milliseconds current_backoff_duration() {
        return _impl->current_backoff_duration();
    };

    void reset() { _impl->reset(); }

private:
    std::unique_ptr<impl> _impl;
};

template<typename Impl, typename... Args>
backoff_policy make_backoff_policy(Args&&... args) {
    return backoff_policy(std::make_unique<Impl>(std::forward<Args>(args)...));
}

template<
  typename ClockType,
  typename DurationType = typename ClockType::duration>
backoff_policy make_exponential_backoff_policy(
  DurationType backoff_base, DurationType max_backoff) {
    class policy final : public backoff_policy::impl {
    public:
        policy(DurationType base, DurationType max)
          : _base_duration(base)
          , _max_backoff(max) {}

        std::chrono::milliseconds current_backoff_duration() final {
            return _current * _base_duration;
        }

        void next_backoff() final {
            _current = std::max<uint32_t>(
              std::min<uint32_t>(_max_backoff.count(), _current << (uint32_t)1),
              1);
        }

        void reset() final { _current = 0; }

    private:
        DurationType _base_duration;
        DurationType _max_backoff;
        uint32_t _current = 0;
    };

    return make_backoff_policy<policy>(backoff_base, max_backoff);
}

} // namespace rpc
