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

#include "net/connection_rate.h"

#include "config/validators.h"
#include "seastar/core/coroutine.hh"
#include "ssx/future-util.h"
#include "vassert.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/net/inet_address.hh>

#include <absl/container/node_hash_map.h>

#include <algorithm>
#include <chrono>
#include <optional>

namespace net {

connection_rate::connection_rate(
  const connection_rate_info& rate_info, ss::gate& connection_gate) noexcept
  : _connection_gate(connection_gate) {
    if (rate_info.max_connection_rate) {
        _general_rate = ss::make_lw_shared<connection_rate_counter>(
          rate_info.max_connection_rate.value());
    }

    fill_overrides(rate_info.overrides);
}

void connection_rate::update_general_rate(std::optional<int64_t> new_value) {
    if (_general_rate) {
        update_connection_rate(_general_rate, new_value);
    } else {
        if (!new_value) {
            return;
        }

        _general_rate = ss::make_lw_shared<connection_rate_counter>(
          new_value.value());
    }
}

void connection_rate::update_overrides_rate(
  const std::vector<ss::sstring>& new_value) {
    if (_overrides.size() != 0) {
        if (new_value.size() != 0) {
            absl::node_hash_map<inet_address_wrapper, int64_t> new_overrides;
            for (const auto& new_connection_rate_str : new_value) {
                auto parsed_override = config::parse_connection_rate_override(
                  new_connection_rate_str);
                vassert(
                  parsed_override.has_value(),
                  "Validation for redpanda config should signal about invalid "
                  "overrides");

                ss::net::inet_address addr;
                try {
                    addr = ss::net::inet_address(parsed_override->first);
                } catch (...) {
                    vassert(
                      false,
                      "Validation for redpanda config should signal about "
                      "invalid ip: {}",
                      parsed_override->first);
                }

                auto [_, res] = new_overrides.emplace(
                  addr, parsed_override->second);
                vassert(
                  res,
                  "Validation for redpanda config should signal about invalid "
                  "overrides");
            }

            auto current_overrides_it = _overrides.begin();
            while (current_overrides_it != _overrides.end()) {
                auto new_overrides_it = new_overrides.find(
                  current_overrides_it->first);
                if (new_overrides_it != new_overrides.end()) {
                    current_overrides_it->second->update_max_rate(
                      new_overrides_it->second);
                    current_overrides_it++;
                } else {
                    update_connection_rate(
                      current_overrides_it->second, std::nullopt);
                    _overrides.erase(current_overrides_it++);
                }
            }

        } else {
            for (auto& [_, sem] : _overrides) {
                update_connection_rate(sem, std::nullopt);
            }
            _overrides.clear();
        }
    } else {
        if (new_value.size() == 0) {
            return;
        }

        fill_overrides(new_value);
    }
}

ss::future<> connection_rate::maybe_wait(const ss::net::inet_address& addr) {
    inet_address_wrapper addr_wrapper(addr);
    auto sem = find_sem(addr_wrapper);
    if (sem.get() == nullptr) {
        co_return;
    }

    allow_new_connections(sem);
    co_await sem->bucket.wait(_max_wait_time, 1);
    spawn_updating_fiber_if_needed(sem);
}

connection_rate::connection_rate_t
connection_rate::find_sem(const inet_address_wrapper& addr) {
    auto overrides_it = _overrides.find(addr);
    if (overrides_it != _overrides.end()) {
        return overrides_it->second;
    }

    return _general_rate;
}

void connection_rate::stop() {
    if (_general_rate) {
        _general_rate->break_semaphore();
    }
    for (auto& [_, sem] : _overrides) {
        sem->break_semaphore();
    }
}

void connection_rate::allow_new_connections(connection_rate_t rate_counter) {
    int64_t max_tokens_for_update = std::max(
      0l, rate_counter->max_tokens - rate_counter->avaiable_new_connections());

    if (max_tokens_for_update == 0) {
        return;
    }

    auto now = ss::lowres_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      now - rate_counter->last_update_time);

    int64_t tokens_for_update = duration.count()
                                / rate_counter->one_token_time.count();

    if (tokens_for_update == 0) {
        return;
    }

    int64_t new_tokens_count = std::min(
      tokens_for_update, max_tokens_for_update);

    rate_counter->update_rate(new_tokens_count, now);
}

void connection_rate::spawn_updating_fiber_if_needed(
  connection_rate_t rate_counter) {
    if (rate_counter->avaiable_new_connections() == 0) {
        // Should spawn fiber to update tokens on next second
        ssx::spawn_with_gate(
          _connection_gate,
          [this,
           rate_counter,
           sleep_time = rate_counter->one_token_time]() mutable {
              return ss::sleep(ss::lowres_clock::duration(sleep_time))
                .then([this, rate_counter] {
                    if (rate_counter.get() != nullptr) {
                        allow_one_new_connection(rate_counter);
                    }
                });
          });
    }
}

void connection_rate::allow_one_new_connection(connection_rate_t rate_counter) {
    auto now = ss::lowres_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      now - rate_counter->last_update_time);

    // If somebody update tokens in updating interval we should skip
    // updating phase
    if (
      duration.count() < rate_counter->one_token_time.count()
      || rate_counter->avaiable_new_connections() > 0) {
        return;
    }

    rate_counter->update_rate(1, now);
}

void connection_rate::update_connection_rate(
  connection_rate_t rate_counter, std::optional<int64_t> new_value) {
    if (new_value) {
        auto should_spawn_fiber = rate_counter->update_max_rate(
          new_value.value());
        if (should_spawn_fiber == should_spawn_fiber_on_update::yes) {
            spawn_updating_fiber_if_needed(rate_counter);
        }
    } else {
        rate_counter->break_semaphore();
        rate_counter = nullptr;
    }
}

void connection_rate::fill_overrides(
  const std::vector<ss::sstring>& new_overrides) {
    for (const auto& setting_for_ip : new_overrides) {
        auto parsed_override = config::parse_connection_rate_override(
          setting_for_ip);

        vassert(
          parsed_override.has_value(),
          "Validation for redpanda config should signal about invalid "
          "overrides");

        auto [_, res] = _overrides.emplace(
          parsed_override.value().first,
          ss::make_lw_shared<connection_rate_counter>(
            parsed_override.value().second));

        vassert(
          res,
          "Validation for redpanda config should signal about invalid "
          "overrides");
    }
}

} // namespace net
