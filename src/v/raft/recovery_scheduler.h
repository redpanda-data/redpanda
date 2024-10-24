// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "base/seastarx.h"
#include "config/property.h"
#include "container/intrusive_list_helpers.h"
#include "metrics/metrics.h"
#include "model/fundamental.h"
#include "raft/fwd.h"
#include "seastar/core/gate.hh"
#include "seastar/core/lowres_clock.hh"
#include "seastar/core/timer.hh"

#include <fmt/core.h>

namespace raft {

class recovery_scheduler_base;

/**
 * Summarized state of a recovery_scheduler.  Use merge()
 * across statuses from all shards to get a total node state.
 *
 * Suitable for use in admin API, statistics, etc.
 */
struct recovery_status {
    uint64_t partitions_to_recover{0};
    uint64_t partitions_active{0};
    uint64_t offsets_pending{0};

    void merge(const recovery_status& rhs) {
        partitions_to_recover += rhs.partitions_to_recover;
        partitions_active += rhs.partitions_active;
        offsets_pending += rhs.offsets_pending;
    }
};

/**
 * If a `consensus` object is a follower that needs recovery or is currently
 * recovering, it instantiates one of these.
 *
 * It will register with recovery_scheduler so that it will start considering
 * this partition as a candidate for allowing recovery.  However, this can also
 * be constructed in an already-recovering state for situations where recovery
 * has been initiated by the leader & not via the scheduler.
 */
class follower_recovery_state {
public:
    follower_recovery_state(
      recovery_scheduler_base& scheduler,
      consensus& parent,
      model::offset our_last,
      model::offset leader_last,
      bool force_active);

    follower_recovery_state(follower_recovery_state&& rhs) = delete;
    follower_recovery_state(const follower_recovery_state& rhs) = delete;
    follower_recovery_state& operator=(const follower_recovery_state&) = delete;
    follower_recovery_state& operator=(follower_recovery_state&&) = delete;
    ~follower_recovery_state() noexcept;

    void force_active();
    void update_progress(model::offset our_last, model::offset leader_last);
    void yield();

    const model::ntp& ntp() const;
    bool is_active() const { return _is_active; }
    int64_t pending_offset_count() const;

    friend std::ostream&
    operator<<(std::ostream&, const follower_recovery_state&);

private:
    friend class recovery_scheduler_base;

    consensus* _parent = nullptr;

    bool _is_active = false;
    model::offset _our_last_offset;
    model::offset _leader_last_offset;

    recovery_scheduler_base* _scheduler = nullptr;
    safe_intrusive_list_hook _list_hook;
};

/**
 * Although each raft group's recovery can proceed independently, it is
 * not desirable to run all of them in parallel, because it leads to
 * an excess of concurrent I/Os to a node coming back online after an outage.
 *
 * This class is responsible for limiting the number of recoveries
 * done to a single shard (and therefore to a single node) at once, and
 * prioritizing them to recover important metadata before bulk data.
 *
 * It is split into a "base" class that contains most of the logic, and
 * then specialized into a subclass that contains timing code, so that the
 * latter part can easily be switched out for unit testing with manual_clock.
 */
class recovery_scheduler_base {
public:
    explicit recovery_scheduler_base(config::binding<size_t> max_active);
    virtual ~recovery_scheduler_base();

    recovery_status get_status();

protected:
    void tick();
    ss::gate _gate;

private:
    void activate_some();

    friend class follower_recovery_state;

    // state transitions
    void add(follower_recovery_state&);
    void activate(follower_recovery_state&);
    void yield(follower_recovery_state&);
    void remove(follower_recovery_state&);

    /**
     * Signal to the scheduler that something has changed, and it
     * should wake up, make scheduling decisions and update status.
     */
    virtual void request_tick() = 0;

    void setup_metrics();

private:
    config::binding<size_t> _max_active;

    using state_list = counted_intrusive_list<
      follower_recovery_state,
      &follower_recovery_state::_list_hook>;

    state_list _active;
    state_list _pending;

    int64_t _offsets_pending = 0;

    metrics::internal_metric_groups _metrics;
    metrics::public_metric_groups _public_metrics;
};

/**
 * The 'ticker' is the concrete implementation of recovery_scheduler,
 * templated on a particular clock type to use.
 */
template<typename Clock>
class recovery_scheduler_ticker : public recovery_scheduler_base {
public:
    recovery_scheduler_ticker(
      config::binding<size_t> max_active,
      config::binding<std::chrono::milliseconds> period)
      : recovery_scheduler_base(std::move(max_active))
      , _timer([this] { return tick(); })
      , _period(std::move(period)) {}

    ss::future<> start() { co_return; }

    ss::future<> stop() {
        _timer.cancel();
        return _gate.close();
    }

    void request_tick() override {
        if (!_timer.armed()) {
            // This time period is meant to be long enough to accumulate a round
            // of heartbeats from all peers and then prioritize them in a batch.
            _timer.arm(_period());
        }
    }

private:
    // This timer is armed after a follower registers or unregisters, to
    // prompt a call to tick().  The timer's delay
    seastar::timer<Clock> _timer;

    // How long to accumulate registrations for before making the next
    // decision about which partitions to start recovering. This time period is
    // meant to be long enough to accumulate a round of heartbeats from all
    // peers.
    config::binding<std::chrono::milliseconds> _period;
};

using recovery_scheduler = recovery_scheduler_ticker<ss::lowres_clock>;

} // namespace raft
