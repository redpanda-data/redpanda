/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/format_to.h"
#include "base/seastarx.h"
#include "base/vassert.h"
#include "cloud_io/scheduler_types.h"
#include "container/intrusive_list_helpers.h"
#include "ssx/semaphore.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>

#include <chrono>
#include <cstdint>
#include <exception>
#include <optional>

namespace cloud_io {

/// How long a group with no in-flight ops and no queued waiters keeps
/// its reservation lane before the policy reclaims it and returns those
/// slots to the common pool.
inline constexpr std::chrono::seconds default_dwell_duration{5};

/// One queued admit() call. The promise's future resolves with a value
/// on dispatch, or aborted on cancellation.
struct reservation_waiter {
    ss::promise<> p;
    intrusive_list_hook link;
    /// Policy-global monotonic insertion sequence. Used for FIFO
    /// ordering across groups in dispatch.
    uint64_t seq{0};
};

/// Per-group scheduling state.
///
/// The policy has two kinds of slot storage:
///
///   - A per-group reservation lane (this struct's reserved_sem). Slots in
///     this lane can only be claimed by the owning group; releases stay in
///     this lane unless the policy reclaims them.
///
///   - A single common pool of slots (reservation_policy::_shared) any group
///     can claim from. Releases go back here unless refill diverts them
///     into a reservation lane.
///
/// Capacity moves between a group's reservation lane and the common pool
/// by two policy actions, each in one direction:
///
///   - refill (common pool → reservation lane): on a common-pool release
///     with no queued waiter, if some group is below its target_reserved
///     the policy diverts the slot into that group's reservation lane.
///     One slot per release event; the lane grows toward target_reserved
///     while the group has demand.
///
///   - reclaim (reservation lane → common pool): an admit sweeps for
///     groups that have been idle past default_dwell_duration and pulls
///     their idle reserved slots (the count on reserved_sem; not the
///     in-flight ones) back into the common pool. The group's reservation
///     drops to zero.
///
/// Reservations ebb and flow with demand: an active under-target group's
/// lane refills toward target_reserved; an idle past-dwell group's lane
/// is reclaimed to zero. set_target_reserved at startup pre-allocates the
/// lane so the first admits don't pay refill latency; after that the
/// cycle is purely demand-driven.
struct reservation_group_state {
    reservation_group_state(group_id id, ss::sstring sem_name)
      : id(id)
      , reserved_sem(0, std::move(sem_name)) {}

    reservation_group_state(const reservation_group_state&) = delete;
    reservation_group_state& operator=(const reservation_group_state&) = delete;
    reservation_group_state(reservation_group_state&&) = delete;
    reservation_group_state& operator=(reservation_group_state&&) = delete;
    ~reservation_group_state() = default;

    /// The group_id this state belongs to.
    group_id id;

    /// The group's reservation lane.
    ssx::semaphore reserved_sem;

    /// Configured cap on the reservation lane.
    size_t target_reserved = 0;

    /// Ops holding a slot from this group, across both the reservation
    /// lane and the common pool.
    size_t in_flight = 0;

    /// Of in_flight, how many came from the reservation lane. Counted
    /// into current_reserved(); a group with more of its lane in use
    /// ranks lower for refill (its lane is already closer to target).
    ///
    /// Invariants at admit/release boundaries:
    ///   reserved_in_flight <= in_flight
    ///   reserved_in_flight <= target_reserved
    size_t reserved_in_flight = 0;

    intrusive_list<reservation_waiter, &reservation_waiter::link> waiters;

    /// Lifetime counters surfaced by metrics & diagnostics.
    uint64_t admit_total = 0;
    uint64_t admit_immediate_total = 0;

    /// Timestamp the group went idle (in_flight=0, no queued waiters).
    /// nullopt means the group has not been active — pre-allocated lane
    /// is reclaim-eligible (drains on the first sweep) and refill-
    /// ineligible (common-pool releases don't route to it). Set to now
    /// on the live idle transition; reset to nullopt when the policy
    /// drains this group's lane.
    std::optional<ss::lowres_clock::time_point> inactive_since;

    /// Group has demand or in-flight ops right now.
    bool is_active() const noexcept {
        return in_flight > 0 || !waiters.empty();
    }

    /// Refill picks targets from the set of effective_active groups.
    /// Capacity is reclaimed from groups that are not effective_active.
    bool is_effective_active(ss::lowres_clock::time_point now) const noexcept {
        if (is_active()) {
            return true;
        }
        if (!inactive_since.has_value()) {
            return false;
        }
        return now - *inactive_since < default_dwell_duration;
    }

    /// True if the group is eligible for reclamation. A never-active
    /// group (inactive_since=nullopt) is eligible from the first
    /// reclaim sweep — its pre-allocated lane drains to the common pool
    /// rather than being held forever for a group that may never show
    /// up. Once a group becomes active, its lane is protected for the
    /// duration of the dwell window after each idle transition.
    bool is_dwell_expired(ss::lowres_clock::time_point now) const noexcept {
        if (is_active()) {
            return false;
        }
        if (!inactive_since.has_value()) {
            return true;
        }
        return now - *inactive_since >= default_dwell_duration;
    }

    /// True if this group should be considered for a refill: it has a
    /// target_reserved floor configured, current_reserved is below that,
    /// and the group is effective-active.
    bool is_refill_eligible(ss::lowres_clock::time_point now) const noexcept {
        return target_reserved > 0 && current_reserved() < target_reserved
               && is_effective_active(now);
    }

    /// True if the group's in-flight count is below its reservation
    /// target. Dispatch uses this to prefer groups that haven't yet
    /// filled their reservation entitlement.
    bool has_reservation_headroom() const noexcept {
        return in_flight < target_reserved;
    }

    /// The lane's runtime size: slots currently held by this group
    /// (available + in-flight). Derived, not stored.
    size_t current_reserved() const noexcept {
        return reserved_sem.current() + reserved_in_flight;
    }

    /// Currently-queued waiters. O(n); intrusive_list::size() is not
    /// constant-time, callers should not assume otherwise.
    size_t waiter_count() const noexcept { return waiters.size(); }

    /// Score for picking a refill target among eligible groups: smaller
    /// is more under-target. Precondition: target_reserved > 0.
    size_t refill_priority_ratio() const noexcept {
        vassert(
          target_reserved > 0,
          "refill_priority_ratio: target_reserved is zero for {}",
          *this);
        return current_reserved() * 1000 / target_reserved;
    }

    /// Record a fast-path admit (no queue). from_reserved=true when the
    /// slot came from the reservation lane, false when from the common
    /// pool.
    void on_immediate_admit(bool from_reserved) noexcept {
        ++in_flight;
        if (from_reserved) {
            ++reserved_in_flight;
        }
        ++admit_total;
        ++admit_immediate_total;
    }

    /// Record a dispatched admit (one that was queued). in_flight was
    /// already incremented at dispatch time inside release_front_waiter.
    void on_dispatched_admit() noexcept { ++admit_total; }

    /// Record a slot release on this group. Decrements in_flight and,
    /// if that transitioned the group to inactive, stamps
    /// inactive_since=now.
    void on_release(ss::lowres_clock::time_point now) noexcept {
        vassert(in_flight > 0, "on_release: in_flight underflow on {}", *this);
        --in_flight;
        if (!is_active()) {
            inactive_since = now;
        }
    }

    /// Try to claim a slot from this group's reservation lane.
    [[nodiscard]] bool try_take_reserved_slot() noexcept {
        return target_reserved > 0 && reserved_sem.try_wait(1);
    }

    /// Add a slot to this group's reservation lane. Used by refill
    /// when the policy routes a common-pool slot into a group below
    /// its target.
    void grant_reserved_slot() noexcept { reserved_sem.signal(1); }

    /// Push a waiter onto the queue. The waiter's storage is owned by
    /// the caller (admit's coroutine frame).
    void enqueue_waiter(reservation_waiter& w, uint64_t seq) noexcept {
        w.seq = seq;
        waiters.push_back(w);
    }

    /// Cancel a queued waiter: unlink it, resolve its promise with
    /// the given exception (defaulting to abort_requested_exception),
    /// and stamp inactive_since=now if the group went idle. Returns
    /// false (no-op) if the waiter was already dispatched.
    bool cancel_waiter(
      reservation_waiter& w,
      ss::lowres_clock::time_point now,
      std::exception_ptr ex = {}) noexcept {
        if (!w.link.is_linked()) {
            return false;
        }
        w.link.unlink();
        w.p.set_exception(
          ex ? std::move(ex)
             : std::make_exception_ptr(ss::abort_requested_exception{}));
        if (!is_active()) {
            inactive_since = now;
        }
        return true;
    }

    /// Pop the front waiter, hand it a slot, and resolve its future.
    /// from_reserved=true when the slot came from the reservation lane.
    /// Caller must ensure the queue is non-empty.
    void release_front_waiter(bool from_reserved = false) noexcept {
        vassert(
          !waiters.empty(), "release_front_waiter: queue empty for {}", *this);
        auto& w = waiters.front();
        waiters.pop_front();
        ++in_flight;
        if (from_reserved) {
            ++reserved_in_flight;
        }
        w.p.set_value();
    }

    /// If the group has a reserved slot in flight, return it to
    /// the lane (to a same-group waiter if one is queued, otherwise
    /// to the lane's semaphore) and return true. Returns false if
    /// the in-flight release should be handled as a common-pool
    /// release instead.
    bool maybe_return_reserved_slot() noexcept {
        if (reserved_in_flight == 0) {
            return false;
        }
        --reserved_in_flight;
        if (!waiters.empty()) {
            release_front_waiter(/*from_reserved=*/true);
        } else {
            reserved_sem.signal(1);
        }
        return true;
    }

    /// Drain the idle reserved slots out of the reservation lane (i.e.
    /// the count on reserved_sem, not the in-flight ones). Returns the
    /// number of slots drained, which the policy signals back to the
    /// common pool. Clears inactive_since so the dwell timer restarts on
    /// the next idle interval.
    size_t drain_idle_reserved() noexcept {
        const auto to_drain = reserved_sem.current();
        if (to_drain > 0) {
            reserved_sem.consume(to_drain);
        }
        inactive_since.reset();
        return to_drain;
    }

    fmt::iterator format_to(fmt::iterator out) const;
};

} // namespace cloud_io
