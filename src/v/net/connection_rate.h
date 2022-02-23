/*
 * Copyright 2022 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "net/inet_address_wrapper.h"
#include "seastarx.h"

#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/net.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>

#include <chrono>

using namespace std::chrono_literals;

namespace net {

struct connection_rate_info {
    std::optional<int64_t> max_connection_rate;
    std::vector<ss::sstring> overrides;
};

enum class should_spawn_fiber_on_update { yes, no };

struct connection_rate_counter {
    explicit connection_rate_counter(int64_t max_rate) noexcept
      : max_tokens(max_rate)
      , one_token_time(std::max(1000 / max_tokens, 1l))
      , rate(max_rate)
      , last_update_time(ss::lowres_clock::now()) {}

    void broke_semaphore() { rate.broken(); }

    should_spawn_fiber_on_update update_max_rate(int64_t new_max_rate) {
        auto diff = new_max_rate - max_tokens;
        if (diff == 0) {
            return should_spawn_fiber_on_update::no;
        }

        max_tokens = new_max_rate;
        one_token_time = std::chrono::milliseconds(
          std::max(1000 / max_tokens, 1l));

        if (diff > 0) {
            auto need_add = max_tokens - rate.current();
            rate.signal(need_add);
        } else {
            auto need_move = static_cast<int64_t>(rate.current()) - max_tokens;
            if (need_move > 0) {
                rate.consume(need_move);
            }
            return should_spawn_fiber_on_update::yes;
        }

        return should_spawn_fiber_on_update::no;
    }

    void
    update_rate(int64_t spawned_tokens, ss::lowres_clock::time_point time) {
        rate.signal(spawned_tokens);
        last_update_time = time;
    }

    int64_t avaiable_new_connections() {
        return static_cast<int64_t>(rate.current());
    }

    int64_t max_tokens;
    std::chrono::milliseconds one_token_time;
    ss::semaphore rate;
    ss::lowres_clock::time_point last_update_time;
};

// This class implement logic to count connections for current seconds. It uses
// token bucket algorithm inside. If we can not accept new connections they will
// wait new tokens. We use handlers to on-line updates for redpanda config.
class connection_rate {
    static constexpr ss::lowres_clock::duration _max_wait_time
      = ss::lowres_clock::duration(100ms);

public:
    connection_rate(
      const connection_rate_info& rate_info,
      ss::gate& connection_gate) noexcept;

    // Handlers for on-line update config calue for connection_rate
    void update_general_rate(std::optional<int64_t> new_value);
    void update_overrides_rate(const std::vector<ss::sstring>& new_value);

    // New connection will wait when redpanda have free tokens for new
    // connection
    ss::future<> maybe_wait(const ss::net::inet_address& addr);

    void stop();

private:
    void fill_overrides(const std::vector<ss::sstring>& new_overrides);

    using connection_rate_t = ss::lw_shared_ptr<connection_rate_counter>;

    connection_rate_t find_sem(const inet_address_wrapper& addr);

    // Increae rate_counter to max connections for current second
    void allow_new_connections(connection_rate_t rate_counter);

    void spawn_updating_fiber_if_needed(connection_rate_t rate_counter);

    void allow_one_new_connection(connection_rate_t rate_counter);

    void update_connection_rate(
      connection_rate_t rate_counter, std::optional<int64_t> new_value);

    connection_rate_t _general_rate;
    // absl::node_hash_map<ss::sstring, connection_rate_t> _overrides;
    absl::node_hash_map<inet_address_wrapper, connection_rate_t> _overrides;

    ss::gate& _connection_gate;
};

} // namespace net