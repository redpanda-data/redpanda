/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "config/property.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "raft/logger.h"
#include "seastar/core/gate.hh"
#include "seastar/core/lowres_clock.hh"
#include "seastar/core/timer.hh"
#include "seastarx.h"
#include "ssx/semaphore.h"

#include <fmt/core.h>

#include <vector>

namespace raft {

class follower_recovery_state;

/**
 * Summarized state of a recovery_coordinator.  Use merge()
 * across statuses from all shards to get a total node state.
 *
 * Suitable for use in admin API, statistics, etc.
 */
struct recovery_status {
    uint64_t partitions_pending{0};
    uint64_t partitions_active{0};
    uint64_t offsets_pending{0};
    uint64_t offsets_hwm{0};

    void merge(recovery_status const& rhs) {
        partitions_pending += rhs.partitions_pending;
        partitions_active += rhs.partitions_active;
        offsets_pending += rhs.offsets_pending;
        offsets_hwm += rhs.offsets_hwm;
    }
};

/**
 * Although each raft group's recovery can proceed independently, it is
 * not desirable to run all of them in parallel, because it leads to
 * an excess of concurrent I/Os to a node coming back online after an outage.
 *
 * This class is responsible for limiting the number of recoveries
 * done to a single node at once, and prioritizing them to recover
 * important metadata before bulk data.
 *
 * It is split into a "base" class that contains most of the logic, and
 * then specialized into a subclass that contains timing code, so that the
 * latter part can easily be switched out for unit testing with manual_clock.
 */
class recovery_coordinator_base {
public:
    recovery_coordinator_base(config::binding<size_t> rps);
    ~recovery_coordinator_base();

    void register_follower(follower_recovery_state*);
    void unregister_follower(follower_recovery_state*);

    const recovery_status& get_status();

    /**
     * Signal to the coordinator that something has changed, and it
     * should wake up and make scheduling decisions (if stats_only
     * is false), and update status.
     */
    virtual void maybe_advance(const model::ntp&, bool stats_only) = 0;

protected:
    ss::gate _gate;

    ss::future<> tick();
    void sort();

    void start_one(follower_recovery_state* frs);

    // TODO: define more efficient data structure for sorting + lookup
    // on register/deregister
    // All partitions on this shard which are currently waiting to
    // recover or in the process of recovering.
    std::vector<follower_recovery_state*> _states;

    // This structure is updated in tick() to enable other classes
    // (mainly the admin server) to look up the total recovery status.
    recovery_status _status;

    // Limit how many recoveries may proceed concurrently on this shard.
    ssx::named_semaphore<> _concurrent_recoveries;

    // Followers have registered or deregistered, so we may need to
    // make some new scheduling decisions.  If we tick and this is false,
    // that means we only need to update status.
    bool _followers_dirty{false};
};

/**
 * The 'ticker' is the concrete implementation of recovery_coordinator,
 * templated on a particular clock type to use.
 */
template<typename Clock>
class recovery_coordinator_ticker : public recovery_coordinator_base {
public:
    recovery_coordinator_ticker(
      config::binding<size_t> rps,
      config::binding<std::chrono::milliseconds> period,
      config::binding<std::chrono::milliseconds> grace_period)
      : recovery_coordinator_base(std::move(rps))
      , _timer([this] { return tick(); })
      , _period(std::move(period))
      , _grace_period(std::move(grace_period)) {}

    ss::future<> start() { co_return; }

    ss::future<> stop() {
        _timer.cancel();
        return _gate.close();
    }

    void maybe_advance(const model::ntp& ntp, bool stats_only) override {
        using namespace std::chrono_literals;

        if (!stats_only) {
            _followers_dirty = true;
        }

        // Q: Why don't we check that some units are available before scheduling
        //    a tick, to avoid doing it if it won't be able to start anything?
        // A: Because we may be killed while a follower_recovery_state is
        //    unregistering itself, while it still holds semaphore units which
        //    it will release afterward.

        // Q: Why do we tick even if _states is empty?
        // A: Because we might have just unregistered the last follower state,
        //    and need to do a round of updates to the status structure to
        //    make out final status visible via the admin API.
        if (!_timer.armed()) {
            // This time period is meant to be long enough to accumulate a round
            // of heartbeats and then prioritize them in a batch.

            // Grace period
            // TODO: pre-empt grace period if the system has fewer partitions
            // than the limit.
            if (_grace) {
                _timer.arm(Clock::now() + _grace_period());

                // Do not exit grace mode when we see the controller NTP
                // come up, because this happens so much earlier than the
                // rest of the system.
                if (ntp != model::controller_ntp) {
                    _grace = false;
                }
            } else {
                _timer.arm(Clock::now() + _period());
            }
        }
    }

private:
    // This timer is armed after a follower registers or unregisters, to
    // prompt a call to tick().  The timer's delay provides nagling of
    // the sorting of _states and updates to _status.
    seastar::timer<Clock> _timer;

    // How long to accumulate registrations for before making the next
    // decision about which partitions to start recovering.
    config::binding<std::chrono::milliseconds> _period;

    // How long to wait after the first partition registers for recovery,
    // before ticking for the first time.  Allows accumulating enough
    // follower_recovery_states to make a reasonable prioritization decision,
    // rather than ending up with a "first come first served" behavior.
    config::binding<std::chrono::milliseconds> _grace_period;

    // If true, wait longer before scheduling recoveries to give all the
    // partitions a chance to report in on startup.
    bool _grace{true};
};

using recovery_coordinator = recovery_coordinator_ticker<ss::lowres_clock>;

/**
 * If a `consensus` object is ready for recovery or currently recovering,
 * it instantiates one of these.
 *
 * In most cases, the purpose is to register that the follower requires
 * recovery, to enable recovery_coordinator to start considering this
 * as a candidate for kicking off recovery.  However, this can also be
 * constructed in an already-recovering state for situations where recovery
 * has been initiated by the leader & not via recovery_coordinator.
 */
class follower_recovery_state {
public:
    follower_recovery_state(
      recovery_coordinator_base* rc,
      model::ntp n,
      model::offset current,
      model::offset hwm,
      bool already_recovering)
      : recovering(already_recovering)
      , ntp(n)
      , last_offset(current)
      , hwm(hwm)
      , _coordinator(rc) {
        if (_coordinator) {
            _coordinator->register_follower(this);
        }
    }

    follower_recovery_state(follower_recovery_state&& rhs) noexcept
      : _coordinator(rhs._coordinator)
      , _units(std::move(rhs._units)) {
        if (_coordinator) {
            _coordinator->unregister_follower(&rhs);
            _coordinator->register_follower(this);
        }
    }

    follower_recovery_state(const follower_recovery_state& rhs) = delete;
    follower_recovery_state& operator=(const follower_recovery_state&) = delete;

    ~follower_recovery_state() {
        if (_coordinator) {
            _coordinator->unregister_follower(this);
        }
    }

    void update(model::offset latest) {
        if (latest > last_offset) {
            last_offset = latest;
        }
        hwm = std::max(hwm, last_offset + model::offset{1});
        _coordinator->maybe_advance(ntp, true);
    }

    /**
     * When recovering is true, we advertise may_recover in append_entries_reply
     */
    bool recovering{false};

    // Copy of the NTP for the partition we refer to.
    model::ntp ntp;

    // The last offset we recovered
    model::offset last_offset{0};

    // The highest offset we've seen, we use this for indicating progress
    // of our recovery
    model::offset hwm{0};

    void detach() {
        _units = ssx::semaphore_units();
        _coordinator = nullptr;
    }

    void start_recovery(ssx::semaphore_units&& units) {
        _units = std::move(units);
        recovering = true;
    }

    model::offset offset_count() {
        return std::max(hwm - last_offset, model::offset{0});
    }

private:
    recovery_coordinator_base* _coordinator;

    ssx::semaphore_units _units;
};

} // namespace raft

template<>
struct fmt::formatter<raft::follower_recovery_state> {
    using type = raft::follower_recovery_state;

    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template<typename FormatContext>
    typename FormatContext::iterator
    format(const type& r, FormatContext& ctx) const;
};
