// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "conn_quota.h"

#include "config/configuration.h"
#include "config/validators.h"
#include "hashing/xx.h"
#include "rpc/logger.h"
#include "seastar/core/coroutine.hh"
#include "ssx/future-util.h"
#include "vlog.h"

namespace net {

conn_quota::units::~units() noexcept {
    _verify_shard.assert_shard_source_location();
    if (_quotas) {
        (*_quotas).get().put(_addr);
    }
}

conn_quota::conn_quota(conn_quota::config_fn cfg_f) noexcept
  : _cfg(cfg_f()) {
    if (ss::this_shard_id() == total_shard) {
        if (_cfg.max_connections()) {
            // Initialize state for enforcement of global connection count
            total_home = ss::make_lw_shared<home_allowance>(
              _cfg.max_connections().value(), _cfg.max_connections().value());
        } else {
            total_home = ss::make_lw_shared<home_allowance>(0, 0);
        }
    }

    _cfg.max_connections.watch([this]() {
        if (ss::this_shard_id() == total_shard) {
            if (_cfg.max_connections()) {
                vlog(
                  rpc::rpclog.info,
                  "Connection count limit updated to {}",
                  _cfg.max_connections().value());
                update_limit({}, *total_home, _cfg.max_connections().value());
            } else {
                total_home = ss::make_lw_shared<home_allowance>(0, 0);
                vlog(rpc::rpclog.info, "Connection count limit disabled");
            }
        } else {
            if (!_cfg.max_connections()) {
                // Reset, in anticipation of limit being re-enabled later
                // (do not want to keep borrowed units around)
                total_remote = remote_allowance{};
            }
        }
    });

    _cfg.max_connections_per_ip.watch([this]() {
        if (
          !_cfg.max_connections_per_ip()
          && _cfg.max_connections_overrides().empty()) {
            if (ss::this_shard_id() == 0) {
                vlog(
                  rpc::rpclog.info, "Connection count per-IP limit disabled");
            }
            ip_home.clear();
            ip_remote.clear();
        } else {
            auto new_limit = _cfg.max_connections_per_ip().value();
            if (ss::this_shard_id() == 0) {
                vlog(
                  rpc::rpclog.info,
                  "Connection count per-IP limit updated to {}",
                  new_limit);
            }

            for (auto& i : ip_home) {
                if (overrides.contains(i.first)) {
                    // Overridden IPs are exempt from the general per-IP limit
                    continue;
                }
                update_limit(i.first, *(i.second), new_limit);
            }
        }
    });

    if (!_cfg.max_connections_overrides().empty()) {
        apply_overrides();
    }

    _cfg.max_connections_overrides.watch([this]() {
        if (
          !_cfg.max_connections_per_ip()
          && _cfg.max_connections_overrides().empty()) {
            overrides.clear();
            ip_home.clear();
            ip_remote.clear();
            return;
        } else {
            apply_overrides();
        }
    });
}

ss::future<> conn_quota::stop() { return _gate.close(); }

/**
 * Release a token, on close of a connection from client IP `addr`
 *
 * Always completes without yielding.  Usually shard local, but
 * may spawn a background invoke_on future if the limits are close
 * to being reached.
 */
void conn_quota::put(ss::net::inet_address addr) {
    vlog(rpc::rpclog.trace, "put({})", addr);

    // If enforcement was disabled since the token
    // was issued, drop it on the floor.
    if (_cfg.max_connections()) {
        do_put({});
    }

    if (_cfg.max_connections_per_ip()) {
        do_put(addr);
    }

    vlog(rpc::rpclog.trace, "leaving put({})", addr);
}

/**
 * Attempt to acquire a token for an incoming connection from a particular
 * client IP.  If none is available, an inert units object is returned.
 *
 * Very fast if limits are disabled.  If limits are enabled this is usually
 * a shard-local operation but can be cross-shard if we are close to reaching
 * limits.
 */
ss::future<conn_quota::units> conn_quota::get(ss::net::inet_address addr) {
    vlog(rpc::rpclog.trace, "get({})", addr);

    if (_cfg.max_connections()) {
        if (!co_await do_get({})) {
            co_return units();
        };
    } else {
        vlog(rpc::rpclog.trace, "Global conn limit disabled");
    }

    if (_cfg.max_connections_per_ip() || overrides.contains(addr)) {
        if (!co_await do_get(addr)) {
            // Release the unit we already took for total connection count
            if (_cfg.max_connections()) {
                do_put({});
            }
            co_return units();
        };
    }

    co_return units(*this, addr);
}

/**
 * @addr either a real address, or {} for total allowance
 */
ss::future<bool> conn_quota::do_get(ss::net::inet_address addr) {
    // Apply global connection count limit
    auto home_shard = addr_to_shard(addr);
    if (home_shard == ss::this_shard_id()) {
        // Fast path: we are the home shard for this address, can
        // probably get a token locally (unless exhausted)
        return home_get_units(addr);
    } else {
        auto& allowance = get_remote_allowance(addr);
        if (allowance.borrowed > 0) {
            // Fast path: we have a borrowed token on this shard
            vlog(rpc::rpclog.trace, "got local borrowed token");
            allowance.borrowed -= 1;
            return ss::make_ready_future<bool>(true);
        } else {
            // Slow path: call to the home core to request a token
            return container().invoke_on(home_shard, [addr](conn_quota& cq) {
                return cq.home_get_units(addr);
            });
        }
    }
}

ss::shard_id conn_quota::addr_to_shard(ss::net::inet_address addr) const {
    if (addr == ss::net::inet_address()) {
        return total_shard;
    } else {
        uint32_t hash = xxhash_32((char*)(addr.data()), addr.size());
        return hash % ss::smp::count;
    }
}

void conn_quota::update_limit(
  ss::net::inet_address addr,
  conn_quota::home_allowance& allowance,
  uint32_t new_limit) {
    vlog(
      rpc::rpclog.trace,
      "update_limit({}): {} -> {}",
      addr,
      allowance.max,
      new_limit);
    auto in_use = allowance.max - allowance.available;
    bool was_dirty_decrease = in_use != 0 && allowance.max > new_limit;

    allowance.max = new_limit;
    if (in_use >= allowance.max) {
        vlog(
          rpc::rpclog.trace,
          "Connection count limit {} decreased below current ({}) for {}",
          allowance.max,
          in_use,
          addr);
        allowance.available = 0;
    } else {
        allowance.available = allowance.max - in_use;
    }

    // If the allowance might have had borrowed units on other
    // nodes, then we must reclaim them in case of a decrease
    // to the limit.
    if (was_dirty_decrease) {
        ssx::spawn_with_gate(_gate, [this, addr]() {
            auto allowance = get_home_allowance(addr);
            return reclaim_to(allowance, addr, true);
        });
    }
}

ss::lw_shared_ptr<conn_quota::home_allowance>
conn_quota::get_home_allowance(ss::net::inet_address addr) {
    assert_on_home(addr);

    if (addr == ss::net::inet_address{}) {
        return total_home;
    } else {
        auto found = ip_home.find(addr);
        if (found != ip_home.end()) {
            return found->second;
        } else {
            vlog(
              rpc::rpclog.trace,
              "Creating default home_allowance for {} (limit {})",
              addr,
              _cfg.max_connections_per_ip().value());
            auto [iter, created] = ip_home.insert(std::make_pair(
              addr,
              ss::make_lw_shared<home_allowance>(
                _cfg.max_connections_per_ip().value(),
                _cfg.max_connections_per_ip().value())));
            return iter->second;
        }
    }
}

conn_quota::remote_allowance&
conn_quota::get_remote_allowance(ss::net::inet_address addr) {
    if (addr == ss::net::inet_address{}) {
        return total_remote;
    } else {
        auto found = ip_remote.find(addr);
        if (found != ip_remote.end()) {
            return found->second;
        } else {
            auto [iter, created] = ip_remote.insert(
              std::make_pair(addr, remote_allowance{}));
            return iter->second;
        }
    }
}

/**
 * Called on the home shard for `addr`.  Dispatch reclaim requests
 * to all other shards, hopefully they had some borrowed tokens
 * that we can claw back.
 */
ss::future<> conn_quota::reclaim_to(
  ss::lw_shared_ptr<conn_quota::home_allowance> allowance,
  ss::net::inet_address addr,
  bool one_time) {
    auto locked = co_await allowance->reclaim_lock.get_units();
    if (allowance->reclaim) {
        // We are already in reclaim mode: remote allowances will
        // not be holding any units belong to us, so don't waste
        // time looking.
        co_return;
    }

    allowance->reclaim = true;
    uint32_t total_released = co_await container().map_reduce0(
      [addr, one_time, home = ss::this_shard_id()](conn_quota& cq) -> uint32_t {
          if (home == ss::this_shard_id()) {
              return 0;
          } else {
              return cq.reclaim_from(addr, one_time);
          }
      },
      0,
      std::plus<uint32_t>());

    allowance->available = std::min(
      allowance->max, allowance->available + total_released);
}

/**
 * Called on the non-home shards for `addr` during a reclaim of
 * borrowed tokens.  Returns the number of borrowed tokens, and
 * sets the reclaim flag to true so that subsequently freed tokens
 * will be dispatched back to the home shard.
 *
 * @param one_time if true, do not leave the shard in reclaim mode,
 *                 just grab any borrowed units they have currently.
 */
uint32_t conn_quota::reclaim_from(ss::net::inet_address addr, bool one_time) {
    vlog(rpc::rpclog.trace, "reclaim_from({})", addr);
    if (addr == ss::net::inet_address()) {
        total_remote.reclaim = !one_time;
        return std::exchange(total_remote.borrowed, 0);
    } else {
        ip_remote[addr].reclaim = !one_time;
        return std::exchange(ip_remote[addr].borrowed, 0);
    }
}

/**
 * Called on non-home shards for `addr` after a previous call
 * to `reclaim_from`, to unset the reclaim flag and permit this
 * non-home shard to store borrowed tokens again.
 *
 * This is invoked from the home shard when it deems that there
 * is no longer pressure for tokens.
 */
void conn_quota::cancel_reclaim_from(ss::net::inet_address addr) {
    vlog(rpc::rpclog.trace, "cancel_reclaim_from({})", addr);
    if (addr == ss::net::inet_address()) {
        total_remote.reclaim = false;
    } else {
        auto found = ip_remote.find(addr);
        if (found != ip_remote.end()) {
            found->second.reclaim = false;
        }
    }
}

/**
 * I am the home shard for this address.  Acquire tokens, reclaiming
 * if necessary.
 *
 * Return true if we successfully acquired tokens.
 */
ss::future<bool> conn_quota::home_get_units(ss::net::inet_address addr) {
    assert_on_home(addr);

    auto allowance = get_home_allowance(addr);
    vlog(rpc::rpclog.trace, "home_get_units({}) allowance={}", addr, allowance);

    // Optimization: early return if allowance has a zero limit, i.e.
    // the administrator is using an override to block a particular client
    if (allowance->max == 0) {
        vlog(
          rpc::rpclog.debug,
          "home_get_units: client {} is blocked (0 connection limit)",
          addr);
        co_return false;
    }

    bool result = try_get_units(*allowance);
    if (result || allowance->reclaim) {
        // If we got a token, or we didn't and are already in
        // reclaim mode, this is the final answer
        vlog(rpc::rpclog.trace, "home_get_units: fast path got={}", result);
        co_return result;
    }

    // We didn't get any units, but there might be some on other cores
    co_await reclaim_to(allowance, addr, false);

    // This can still fail if there were no reclaimable units, but
    // we did our best.
    result = try_get_units(*allowance);
    vlog(
      rpc::rpclog.trace, "home_get_units: slow (reclaim) path got={}", result);
    co_return result;
}

bool conn_quota::try_get_units(home_allowance& allowance) {
    if (allowance.available) {
        allowance.available -= 1;
        return true;
    } else {
        return false;
    }
}

/**
 * Given the home state of an allowance, decide whether it should
 * leave reclaim (i.e. permit other shards to borrow tokens again)
 */
bool conn_quota::should_leave_reclaim(home_allowance& allowance) {
    return allowance.reclaim
           // Must be enough tokens for it to be worth borrowing any
           && allowance.max > ss::smp::count
           // Must have at least half its tokens free
           && allowance.available > allowance.max / 2
           // Must not be in the middle of starting a reclaim
           && allowance.reclaim_lock.ready();
}

/**
 * Broadcast (in the background) to all other shards that they
 * may clear the reclaim flag.
 */
void conn_quota::cancel_reclaim_to(
  ss::net::inet_address addr, ss::lw_shared_ptr<home_allowance> allowance) {
    assert_on_home(addr);

    vlog(rpc::rpclog.trace, "cancel_reclaim_to({})", addr);

    ssx::spawn_with_gate(_gate, [this, allowance = std::move(allowance), addr] {
        // Re-check conditions are still suitable.
        if (should_leave_reclaim(*allowance)) {
            // Guaranteed to have units because of precheck in
            // should_leave_reclaim
            auto units = allowance->reclaim_lock.try_get_units().value();
            allowance->reclaim = false;
            return container()
              .invoke_on_others([addr](conn_quota& cq) -> ss::future<> {
                  cq.cancel_reclaim_from(addr);
                  return ss::now();
              })
              .finally([u = std::move(units)] {})
              // Keep allowance alive until after units are dropped
              .finally([allowance] {});
        } else {
            return ss::now();
        }
    });
}

void conn_quota::do_put(ss::net::inet_address addr) {
    vlog(rpc::rpclog.trace, "do_put({})", addr);

    auto home_shard = addr_to_shard(addr);
    if (home_shard == ss::this_shard_id()) {
        auto allowance = get_home_allowance(addr);
        vlog(
          rpc::rpclog.trace,
          "do_put: release directly to home allowance={}",
          allowance);
        allowance->put();
        if (should_leave_reclaim(*allowance)) {
            cancel_reclaim_to(addr, allowance);
        }
    } else {
        auto& allowance = get_remote_allowance(addr);
        if (!allowance.reclaim) {
            vlog(rpc::rpclog.trace, "do_put: release to local borrowed");
            allowance.put();
        } else {
            vlog(rpc::rpclog.trace, "do_put: reclaim, dispatch to home");

            ssx::spawn_with_gate(_gate, [this, addr, home_shard]() {
                return container().invoke_on(
                  home_shard, [addr](conn_quota& cq) { cq.do_put(addr); });
            });
        }
    }
}

/**
 * When the overrides config binding is updated, call this to populate
 * all the overridden addresses into ip_home.
 */
void conn_quota::apply_overrides() {
    overrides.clear();
    overrides.reserve(_cfg.max_connections_overrides().size());
    for (const auto& o : _cfg.max_connections_overrides()) {
        auto parsed = config::parse_connection_rate_override(o);
        if (!parsed.has_value()) {
            if (ss::this_shard_id() == ss::shard_id{0}) {
                // Avoid log spam from all shards, only log on shard 0
                vlog(
                  rpc::rpclog.warn,
                  "Invalid entry in kafka_connections_max_overrides: '{}'",
                  o);
            }
            continue;
        }

        auto [addr, limit] = parsed.value();
        overrides.emplace(addr, limit);
    }

    for (const auto& i : overrides) {
        const auto& addr = i.first;

        // We only care about updating ip_home, where the authoritative
        // limit is stored.
        if (addr_to_shard(addr) != ss::this_shard_id()) {
            continue;
        }

        auto found = ip_home.find(i.first);
        if (found != ip_home.end()) {
            update_limit(i.first, *(found->second), i.second);
        } else {
            vlog(
              rpc::rpclog.trace,
              "Populating override home_allowance for {}",
              i.first);
            ip_home.insert(std::make_pair(
              i.first, ss::make_lw_shared<home_allowance>(i.second, i.second)));
        }
    }

    // In case there were overrides removed, update existing ip_home
    // state
    std::vector<inet_address_key> to_delete;
    for (auto& i : ip_home) {
        if (!overrides.contains(i.first)) {
            if (!_cfg.max_connections_per_ip().has_value()) {
                // If it's not overridden and there is per_ip config,
                // then we can drop the state.
                to_delete.emplace_back(i.first);
            } else if (i.second->max != _cfg.max_connections_per_ip().value()) {
                // If it used to be overridden, reset its limit to
                // the general per-IP limit
                update_limit(
                  i.first, *(i.second), _cfg.max_connections_per_ip().value());
            }
        }
    }
    for (const auto& k : to_delete) {
        ip_home.erase(k);
    }
}

bool conn_quota::test_only_is_in_reclaim(ss::net::inet_address addr) const {
    auto home_shard = addr_to_shard(addr);
    if (home_shard == ss::this_shard_id()) {
        if (addr == ss::net::inet_address{}) {
            return total_home->reclaim;
        } else if (ip_home.contains(addr)) {
            return ip_home.find(addr)->second->reclaim;
        } else {
            return false;
        }
    } else {
        if (addr == ss::net::inet_address{} || ip_remote.contains(addr)) {
            auto& allowance = get_remote_allowance(addr);
            return allowance.reclaim;
        } else {
            return false;
        }
    }
}

} // namespace net
