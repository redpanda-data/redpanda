/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_io/reservation_policy.h"

#include "base/vassert.h"
#include "base/vlog.h"
#include "cloud_io/logger.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>

#include <algorithm>
#include <limits>
#include <utility>

namespace cloud_io {

namespace {

/// Build the per-group state. Each group's reservation lane gets a
/// semaphore named after its group_id.
template<size_t... Is>
per_group<reservation_group_state>
make_group_states(std::index_sequence<Is...>) {
    return {{reservation_group_state{
      static_cast<group_id>(Is),
      fmt::format(
        "cloud_io/reservation_policy/reserved/{}",
        to_string_view(static_cast<group_id>(Is)))}...}};
}

} // namespace

fmt::iterator reservation_group_state::format_to(fmt::iterator out) const {
    return fmt::format_to(
      out,
      "{}{{in_flight={}[reserved_in_flight={}], target_reserved={}, "
      "current_reserved={}, waiter_count={}}}",
      to_string_view(id),
      in_flight,
      reserved_in_flight,
      target_reserved,
      current_reserved(),
      waiter_count());
}

reservation_policy::reservation_policy(
  size_t capacity, reservation_policy_config cfg)
  : scheduler_policy(capacity)
  , _current_total_capacity(capacity)
  , _shared(0, "cloud_io/reservation_policy/shared")
  , _groups(make_group_states(std::make_index_sequence<num_group_ids>{}))
  , _now_fn([] { return ss::lowres_clock::now(); }) {
    const size_t target_sum = std::ranges::fold_left(
      cfg.target_reserved, size_t{0}, std::plus{});
    vassert(
      target_sum <= capacity,
      "reservation_policy: target_reserved sum ({}) exceeds capacity ({})",
      target_sum,
      capacity);

    _shared.signal(capacity);
    for (const auto g : all_group_ids) {
        set_target_reserved(g, cfg.target_reserved[g]);
    }

    vlog(
      log.info,
      "reservation_policy initialized: capacity={} dwell={}s "
      "target_reserved={}",
      _current_total_capacity,
      default_dwell_duration.count(),
      cfg.target_reserved.data);
}

reservation_policy::~reservation_policy() noexcept {
    for (const auto& gs : _groups) {
        vassert(
          gs.waiters.empty(),
          "cloud_io::reservation_policy destroyed with active waiters");
    }
}

ss::future<> reservation_policy::stop() {
    // Synchronous: abort all queued waiters.
    for (auto& gs : _groups) {
        while (!gs.waiters.empty()) {
            gs.cancel_waiter(gs.waiters.front(), _now_fn());
        }
    }
    return ss::now();
}

size_t reservation_policy::in_flight(group_id g) const noexcept {
    return _groups[g].in_flight;
}

size_t reservation_policy::waiters(group_id g) const noexcept {
    return _groups[g].waiter_count();
}

size_t reservation_policy::available_slots() const noexcept {
    size_t total = _shared.current();
    for (const auto& gs : _groups) {
        total += gs.reserved_sem.current();
    }
    return total;
}

size_t reservation_policy::total_capacity() const noexcept {
    return _current_total_capacity;
}

ss::future<> reservation_policy::admit(group_id g, ss::abort_source& as) {
    // Fast path.
    if (try_admit(g)) {
        co_return;
    }

    auto& gs = _groups[g];

    // Slow path: queue this caller on the group's waiter list. The
    // caller must keep the waiter node alive until the future
    // resolves (success or abort).
    reservation_waiter w;
    gs.enqueue_waiter(w, _waiter_seq_counter++);
    auto fut = w.p.get_future();

    auto sub = as.subscribe(
      [&w, &gs, this](const std::optional<std::exception_ptr>& ex) noexcept {
          gs.cancel_waiter(w, _now_fn(), ex.value_or(nullptr));
      });

    if (!sub) {
        // abort_source was already aborted, so just cancel the waiter.
        // cancel operation will fail its promise with abort_requested
        gs.cancel_waiter(w, _now_fn());
    }

    co_await std::move(fut);
    gs.on_dispatched_admit();
    co_return;
}

bool reservation_policy::try_admit(group_id g) noexcept {
    auto& gs = _groups[g];

    // Reclaim idle reservations first. Otherwise a waiting group could
    // queue behind capacity stranded on an inactive group.
    reclaim_idle_reservations(_now_fn());

    if (gs.try_take_reserved_slot()) {
        gs.on_immediate_admit(/*from_reserved=*/true);
        return true;
    }

    if (_shared.try_wait(1)) {
        gs.on_immediate_admit(/*from_reserved=*/false);
        return true;
    }

    return false;
}

void reservation_policy::release(group_id g) noexcept {
    auto& gs = _groups[g];
    gs.on_release(_now_fn());

    // Try to return a slot directly to this group, either to another waiter or
    // to the reservation semaphore.
    if (gs.maybe_return_reserved_slot()) {
        return;
    }

    // Common-pool release: try to dispatch a queued waiter, then
    // refill a reservation lane, then fall back to the common pool.
    if (dispatch_next()) {
        return;
    }
    if (const auto target = pick_refill_candidate(); target.has_value()) {
        _groups[*target].grant_reserved_slot();
    } else {
        _shared.signal(1);
    }
}

bool reservation_policy::dispatch_next() noexcept {
    // One pass picks two candidates: the oldest seq among under-target
    // groups (preferred), and the oldest seq globally (fallback).
    std::optional<group_id> under_target_pick;
    std::optional<group_id> any_pick;
    uint64_t under_target_oldest = std::numeric_limits<uint64_t>::max();
    uint64_t any_oldest = std::numeric_limits<uint64_t>::max();

    for (const auto& gs : _groups) {
        if (gs.waiters.empty()) {
            continue;
        }
        const uint64_t front_seq = gs.waiters.front().seq;
        if (front_seq < any_oldest) {
            any_oldest = front_seq;
            any_pick = gs.id;
        }
        if (gs.has_reservation_headroom() && front_seq < under_target_oldest) {
            under_target_oldest = front_seq;
            under_target_pick = gs.id;
        }
    }

    const auto pick = under_target_pick.has_value() ? under_target_pick
                                                    : any_pick;
    if (!pick.has_value()) {
        return false;
    }

    auto& gs = _groups[*pick];
    gs.release_front_waiter();

    if (_dispatch_counter++ % 1000 == 0) {
        vlog(
          log.debug,
          "reservation_policy: dispatch #{} picked={} | {}",
          _dispatch_counter,
          to_string_view(gs.id),
          fmt::join(_groups, " "));
    }

    return true;
}

void reservation_policy::set_total_slots(size_t desired) {
    if (desired == _current_total_capacity) {
        return;
    }
    if (desired > _current_total_capacity) {
        _shared.signal(desired - _current_total_capacity);
    } else {
        _shared.consume(_current_total_capacity - desired);
    }
    vlog(
      log.info,
      "cloud_io reservation_policy total slots: {} -> {}",
      _current_total_capacity,
      desired);
    _current_total_capacity = desired;
}

void reservation_policy::set_target_reserved(group_id g, size_t value) {
    auto& gs = _groups[g];
    // Reconcile the reservation lane to reflect the new target. Compute
    // the delta against current_reserved() (the derived current size) so
    // that any reclamation or refill since the last call are accounted
    // for; the lane may have ebbed and flowed between calls.
    const size_t cur = gs.current_reserved();
    if (value > cur) {
        const size_t delta = value - cur;
        vassert(
          _shared.current() >= delta,
          "set_target_reserved({}, {}): would underflow _shared "
          "(current={}, delta={})",
          to_string_view(g),
          value,
          _shared.current(),
          delta);
        _shared.consume(delta);
        gs.reserved_sem.signal(delta);
    } else if (value < cur) {
        const size_t delta = cur - value;
        vassert(
          gs.reserved_sem.current() >= delta,
          "set_target_reserved({}, {}): would underflow reserved_sem "
          "(current={}, in_flight={}, delta={})",
          to_string_view(g),
          value,
          gs.reserved_sem.current(),
          gs.reserved_in_flight,
          delta);
        gs.reserved_sem.consume(delta);
        _shared.signal(delta);
    }
    gs.target_reserved = value;
}

size_t reservation_policy::target_reserved(group_id g) const noexcept {
    return _groups[g].target_reserved;
}

size_t reservation_policy::current_reserved(group_id g) const noexcept {
    return _groups[g].current_reserved();
}

uint64_t reservation_policy::admit_total(group_id g) const noexcept {
    return _groups[g].admit_total;
}

uint64_t reservation_policy::admit_immediate_total(group_id g) const noexcept {
    return _groups[g].admit_immediate_total;
}

size_t reservation_policy::total_waiters() const noexcept {
    return std::ranges::fold_left(
      _groups, size_t{0}, [](size_t acc, const auto& gs) {
          return acc + gs.waiter_count();
      });
}

void reservation_policy::set_now_fn_for_test(now_fn_t fn) {
    _now_fn = std::move(fn);
}

void reservation_policy::reclaim_idle_reservations(
  ss::lowres_clock::time_point now) {
    for (auto& gs : _groups) {
        if (gs.is_dwell_expired(now)) {
            _shared.signal(gs.drain_idle_reserved());
        }
    }
}

std::optional<group_id> reservation_policy::pick_refill_candidate() noexcept {
    const auto now = _now_fn();
    std::optional<group_id> winner;
    // Smaller ratio = more under-target.
    size_t lowest_ratio = std::numeric_limits<size_t>::max();
    for (const auto& gs : _groups) {
        if (!gs.is_refill_eligible(now)) {
            continue;
        }
        if (
          const auto ratio = gs.refill_priority_ratio(); ratio < lowest_ratio) {
            lowest_ratio = ratio;
            winner = gs.id;
        }
    }
    return winner;
}

} // namespace cloud_io
