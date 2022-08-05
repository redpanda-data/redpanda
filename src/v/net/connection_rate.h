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

#include "config/validators.h"
#include "net/connection_rate_counter.h"
#include "net/inet_address_wrapper.h"
#include "net/server_probe.h"
#include "seastar/core/coroutine.hh"
#include "seastarx.h"
#include "ssx/future-util.h"
#include "vassert.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/net.hh>
#include <seastar/util/bool_class.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>

#include <chrono>

using namespace std::chrono_literals;

namespace net {

struct connection_rate_info {
    std::optional<int64_t> max_connection_rate;
    std::vector<ss::sstring> overrides;
};

// This class implement logic to count connections for current seconds. It uses
// token bucket algorithm inside. If we can not accept new connections they will
// wait new tokens. We use handlers to on-line updates for redpanda config.
template<typename Clock = ss::lowres_clock>
class connection_rate {
public:
    connection_rate(
      const connection_rate_info& rate_info,
      ss::gate& connection_gate,
      server_probe& probe,
      std::chrono::milliseconds max_wait_time = 100ms) noexcept
      : _max_wait_time(typename Clock::duration(max_wait_time))
      , _connection_gate(connection_gate)
      , _probe(probe) {
        if (rate_info.max_connection_rate) {
            _general_rate = ss::make_lw_shared<connection_rate_counter<Clock>>(
              rate_info.max_connection_rate.value());
        }

        fill_overrides(rate_info.overrides);
    }

    // Handlers for on-line update config calue for connection_rate
    void update_general_rate(std::optional<int64_t> new_value) {
        if (_general_rate) {
            update_connection_rate(_general_rate, new_value);
        } else {
            if (!new_value) {
                return;
            }

            _general_rate = ss::make_lw_shared<connection_rate_counter<Clock>>(
              new_value.value());
        }
    }

    void update_overrides_rate(const std::vector<ss::sstring>& new_value) {
        if (_overrides.size() != 0) {
            if (new_value.size() != 0) {
                absl::node_hash_map<inet_address_wrapper, int64_t>
                  new_overrides;
                for (const auto& new_connection_rate_str : new_value) {
                    auto parsed_override
                      = config::parse_connection_rate_override(
                        new_connection_rate_str);
                    vassert(
                      parsed_override.has_value(),
                      "Validation for redpanda config should signal about "
                      "invalid "
                      "overrides");

                    ss::net::inet_address addr;
                    try {
                        addr = ss::net::inet_address(parsed_override->first);
                    } catch (...) {
                        vassert(
                          false,
                          "Validation for redpanda config should signal "
                          "about "
                          "invalid ip: {}",
                          parsed_override->first);
                    }

                    auto [_, res] = new_overrides.emplace(
                      addr, parsed_override->second);
                    vassert(
                      res,
                      "Validation for redpanda config should signal about "
                      "invalid "
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

    // New connection will wait when redpanda have free tokens for new
    // connection
    ss::future<> maybe_wait(const ss::net::inet_address& addr) {
        inet_address_wrapper addr_wrapper(addr);
        auto sem = find_sem(addr_wrapper);
        if (sem.get() == nullptr) {
            co_return;
        }

        allow_new_connections(sem);
        co_await sem->wait(_max_wait_time, _probe);
        spawn_updating_fiber_if_needed(sem);
    }

    void stop() {
        _as.request_abort();
        if (_general_rate) {
            _general_rate->break_semaphore();
        }
        for (auto& [_, sem] : _overrides) {
            sem->break_semaphore();
        }
    }

private:
    void fill_overrides(const std::vector<ss::sstring>& new_overrides) {
        for (const auto& setting_for_ip : new_overrides) {
            auto parsed_override = config::parse_connection_rate_override(
              setting_for_ip);

            vassert(
              parsed_override.has_value(),
              "Validation for redpanda config should signal about invalid "
              "overrides");

            auto [_, res] = _overrides.emplace(
              parsed_override.value().first,
              ss::make_lw_shared<connection_rate_counter<Clock>>(
                parsed_override.value().second));

            vassert(
              res,
              "Validation for redpanda config should signal about invalid "
              "overrides");
        }
    }

    using connection_rate_t = ss::lw_shared_ptr<connection_rate_counter<Clock>>;

    connection_rate_t find_sem(const inet_address_wrapper& addr) {
        auto overrides_it = _overrides.find(addr);
        if (overrides_it != _overrides.end()) {
            return overrides_it->second;
        }

        return _general_rate;
    }

    // Increae rate_counter to max connections for current second
    void allow_new_connections(connection_rate_t rate_counter) {
        auto now = Clock::now();

        int64_t max_tokens_for_update = std::max(
          0l,
          rate_counter->max_tokens - rate_counter->avaiable_new_connections());

        if (max_tokens_for_update == 0) {
            rate_counter->update_rate(0, now);
            return;
        }

        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
          now - rate_counter->get_last_update_time());

        int64_t tokens_for_update = duration.count()
                                    / rate_counter->one_token_time.count();

        if (tokens_for_update == 0) {
            return;
        }

        int64_t new_tokens_count = std::min(
          tokens_for_update, max_tokens_for_update);

        rate_counter->update_rate(new_tokens_count, now);
    }

    void spawn_updating_fiber_if_needed(connection_rate_t rate_counter) {
        if (rate_counter->avaiable_new_connections() == 0) {
            // Should spawn fiber to update tokens on next second
            ssx::spawn_with_gate(
              _connection_gate,
              [this,
               rate_counter,
               sleep_time = rate_counter->one_token_time]() mutable {
                  return ss::sleep_abortable<Clock>(
                           typename Clock::duration(sleep_time), _as)
                    .then([this, rate_counter] {
                        if (rate_counter.get() != nullptr) {
                            allow_one_new_connection(rate_counter);
                        }
                    })
                    .handle_exception_type([](const ss::sleep_aborted&) {
                        // do nothing;
                    });
              });
        }
    }

    void allow_one_new_connection(connection_rate_t rate_counter) {
        auto now = Clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
          now - rate_counter->get_last_update_time());

        // If somebody update tokens in updating interval we should skip
        // updating phase
        if (
          duration.count() < rate_counter->one_token_time.count()
          || rate_counter->avaiable_new_connections() > 0) {
            return;
        }

        rate_counter->update_rate(1, now);
    }

    void update_connection_rate(
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

    const typename Clock::duration _max_wait_time;

    connection_rate_t _general_rate;
    // absl::node_hash_map<ss::sstring, connection_rate_t> _overrides;
    absl::node_hash_map<inet_address_wrapper, connection_rate_t> _overrides;

    ss::gate& _connection_gate;
    ss::abort_source _as;

    server_probe& _probe;
};

} // namespace net
