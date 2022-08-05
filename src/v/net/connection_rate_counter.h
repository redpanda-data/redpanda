/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "net/server_probe.h"
#include "seastarx.h"
#include "ssx/semaphore.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/util/bool_class.hh>

using namespace std::chrono_literals;

namespace net {

using should_spawn_fiber_on_update
  = ss::bool_class<struct should_spawn_fiber_on_update_tag>;

template<typename Clock = ss::lowres_clock>
class connection_rate_counter {
public:
    explicit connection_rate_counter(int64_t max_rate) noexcept
      : max_tokens(max_rate)
      , one_token_time(std::max(1000 / max_tokens, 1l))
      , bucket(max_rate, "net/conn-rate")
      , last_update_time(Clock::now()) {}

    void break_semaphore() { bucket.broken(); }

    should_spawn_fiber_on_update update_max_rate(int64_t new_max_rate) {
        auto diff = new_max_rate - max_tokens;
        if (diff == 0) {
            return should_spawn_fiber_on_update::no;
        }

        max_tokens = new_max_rate;
        one_token_time = std::chrono::milliseconds(
          std::max(1000 / max_tokens, 1l));

        if (diff > 0) {
            auto need_add = max_tokens - avaiable_new_connections();
            bucket.signal(need_add);
        } else {
            auto need_move = avaiable_new_connections() - max_tokens;
            if (need_move > 0) {
                bucket.consume(need_move);
            }
            return should_spawn_fiber_on_update::yes;
        }

        return should_spawn_fiber_on_update::no;
    }

    void update_rate(int64_t spawned_tokens, typename Clock::time_point time) {
        bucket.signal(spawned_tokens);
        last_update_time = time;
    }

    int64_t avaiable_new_connections() {
        return static_cast<int64_t>(bucket.current());
    }

    ss::future<>
    wait(typename Clock::duration max_wait_time, server_probe& probe) {
        if (need_wait()) {
            probe.waiting_for_conection_rate();
        }
        co_await bucket.wait(max_wait_time, 1);
    }

    typename Clock::time_point get_last_update_time() {
        return last_update_time;
    }

    int64_t max_tokens;
    std::chrono::milliseconds one_token_time;

private:
    bool need_wait() { return avaiable_new_connections() == 0; }

    ssx::named_semaphore<Clock> bucket;
    typename Clock::time_point last_update_time;
};

} // namespace net