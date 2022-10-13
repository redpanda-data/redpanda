// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "config/property.h"
#include "seastar/core/gate.hh"
#include "seastar/core/sharded.hh"
#include "seastar/net/inet_address.hh"
#include "seastarx.h"
#include "utils/mutex.h"
#include "vassert.h"

#include <absl/container/flat_hash_map.h>
#include <absl/hash/hash.h>

#include <stdint.h>

namespace net {

class conn_quota;

struct conn_quota_config {
    config::binding<std::optional<uint32_t>> max_connections;
    config::binding<std::optional<uint32_t>> max_connections_per_ip;
    config::binding<std::vector<ss::sstring>> max_connections_overrides;
};

/**
 * A sharded service responsible for storing connection counts by
 * IP and node-wide, to enable network servers to apply limits on
 * concurrent connections.
 *
 * There are global quotas and per-IP quotas.  Many functions take
 * an inet_address argument, and if it is a default-initialized
 * object, that means we are acting on the global quota.
 */
class conn_quota : public ss::peering_sharded_service<conn_quota> {
public:
    /**
     * RAII helper for reliably releasing connection count quota.
     *
     * For convenience of call sites where quotas may be optional,
     * there is a default constructor that provides a guard that
     * does nothing.
     */
    class [[nodiscard]] units {
    public:
        units() = default;

        units(conn_quota& quotas, ss::net::inet_address const& addr)
          : _quotas(std::ref(quotas))
          , _addr(addr) {}

        units(units const&) = delete;
        units& operator=(units const&) = delete;
        units(units&& rhs) noexcept
          : _addr(rhs._addr) {
            _quotas = std::exchange(rhs._quotas, std::nullopt);
        }
        units& operator=(units&& rhs) noexcept {
            _quotas = std::exchange(rhs._quotas, std::nullopt);
            _addr = rhs._addr;
            return *this;
        }

        ~units() noexcept;

        /**
         * A default-constructed `units` is not live, i.e.
         * does not release anything on destruction.  Once
         * it is constructed with a reference to a `conn_quota`
         * it becomes live.
         */
        bool live() const { return _quotas.has_value(); }

    private:
        std::optional<std::reference_wrapper<conn_quota>> _quotas;
        ss::net::inet_address _addr;
    };

    using config_fn = std::function<conn_quota_config()>;
    conn_quota(config_fn) noexcept;

    ss::future<units> get(ss::net::inet_address);
    void put(ss::net::inet_address);

    ss::future<> stop();

    /**
     * Hook for unit tests to validate the reclaim logic.
     */
    bool test_only_is_in_reclaim(ss::net::inet_address addr) const;

private:
    /**
     * State on a home core for an address: this records the authoritative
     * maximum tokens allowed, plus how many are available on this core.  The
     * available count does not include tokens which are currently borrowed
     * by another core (i.e. in a remote_allowance::borrowed)
     */
    struct home_allowance {
        home_allowance(uint32_t m, uint32_t a)
          : max(m)
          , available(a) {}
        uint32_t max{0};
        uint32_t available{0};

        void put() { available = std::min(max, available + 1); }

        // When reclaim is true and available=0, it is not necessary
        // to do cross-core communication: we can be sure there are
        // not any borrowed tokens elsewhere.
        bool reclaim{false};

        // Lock to prevent multiple fibers trying to concurrently
        // do reclaims (would happen if multiple incoming connections
        // on the same shard when available==0)
        mutex reclaim_lock;
    };

    friend std::ostream& operator<<(std::ostream& o, const home_allowance& ha) {
        fmt::print(
          o, "{{ {}/{} reclaim={}}}", ha.available, ha.max, ha.reclaim);
        return o;
    }

    /**
     * State on a non-home core for an address: records how many tokens
     * we have borrowed from the home core, and whether we are in reclaim
     * mode (release tokens to home core) or not (release tokens to our
     * own borrowed pool).
     */
    struct remote_allowance {
        uint32_t borrowed{0};

        void put() { borrowed += 1; }

        // When reclaim is true, released tokens are dispatched to
        // the home core rather than back into `borrowed`.
        bool reclaim{false};
    };

    /**
     * Wrapper for inet_address implementing operators needed for use in a map
     */
    class inet_address_key : public ss::net::inet_address {
    public:
        inet_address_key(ss::net::inet_address addr)
          : ss::net::inet_address(addr) {}

        template<typename H>
        friend H AbslHashValue(H h, const inet_address_key& k) {
            return H::combine(
              std::move(h), std::hash<ss::net::inet_address>{}(k));
        }
    };

    // Which shard holds home_allowance for this address?
    ss::shard_id addr_to_shard(ss::net::inet_address) const;

    void
    assert_on_home([[maybe_unused]] ss::net::inet_address const& addr) const {
#ifndef NDEBUG
        vassert(
          conn_quota::addr_to_shard(addr) == ss::this_shard_id(),
          "Wrong shard");
#endif
    }

    // Which shard holds the allowances for the total connection count?
    static constexpr ss::shard_id total_shard = 0;

    // impl details of put/get
    ss::future<bool> do_get(ss::net::inet_address);
    void do_put(ss::net::inet_address);
    ss::future<bool> home_get_units(ss::net::inet_address);
    bool try_get_units(home_allowance& allowance);

    // Reclaim logic
    ss::future<> reclaim_to(
      ss::lw_shared_ptr<home_allowance> allowance,
      ss::net::inet_address,
      bool one_time);
    uint32_t reclaim_from(ss::net::inet_address, bool one_time);
    void cancel_reclaim_to(
      ss::net::inet_address, ss::lw_shared_ptr<home_allowance>);
    void cancel_reclaim_from(ss::net::inet_address);
    bool should_leave_reclaim(home_allowance& allowance);

    ss::lw_shared_ptr<home_allowance> get_home_allowance(ss::net::inet_address);
    remote_allowance& get_remote_allowance(ss::net::inet_address);
    const remote_allowance&
    get_remote_allowance(ss::net::inet_address addr) const {
        return const_cast<conn_quota&>(*this).get_remote_allowance(addr);
    };

    void apply_overrides();

    /**
     * A note on types:
     * - the home_allowance instances in the `ip_home` map need to
     *   be refcounted to enable safely holding the encapsulated mutex
     *   across scheduling points, where the entry could get removed
     *   from ip_home in the background.
     * - total_home does not have this problem, but it's wrapped in
     *   a smart pointer just to conform to the common get_home_allowance
     *   interface.
     */

    // Allowance state for total connection count
    ss::lw_shared_ptr<home_allowance> total_home; // Valid on shard 0
    remote_allowance total_remote;                // Valid on shard !=0

    // Allowance state for each client IP.
    absl::flat_hash_map<inet_address_key, ss::lw_shared_ptr<home_allowance>>
      ip_home;
    absl::flat_hash_map<inet_address_key, remote_allowance> ip_remote;

    // Parsed content of _cfg.max_connections_overrides.
    absl::flat_hash_map<inet_address_key, uint32_t> overrides;

    // Apply a configuration change
    void
    update_limit(ss::net::inet_address, conn_quota::home_allowance&, uint32_t);

    conn_quota_config _cfg;

    ss::gate _gate;
};

} // namespace net