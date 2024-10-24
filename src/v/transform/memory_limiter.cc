/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "memory_limiter.h"

#include "base/vassert.h"
#include "utils/human.h"

#include <limits>

namespace transform {

namespace {
// See: http://locklessinc.com/articles/sat_arithmetic/
constexpr size_t saturating_add(size_t x, size_t y) {
    size_t res = x + y;
    res |= -static_cast<size_t>(res < x);
    return res;
}

// Mini inline unit tests :D
static_assert(saturating_add(1, 1) == 2);
static_assert(saturating_add(0, 0) == 0);
static_assert(saturating_add(0, 1) == 1);
static_assert(saturating_add(1, 0) == 1);
static_assert(
  saturating_add(
    std::numeric_limits<size_t>::max() / 2,
    std::numeric_limits<size_t>::max() / 2)
  == std::numeric_limits<size_t>::max() - 1);
static_assert(
  saturating_add(
    std::numeric_limits<size_t>::max() / 2,
    1 + std::numeric_limits<size_t>::max() / 2)
  == std::numeric_limits<size_t>::max());
static_assert(
  saturating_add(
    std::numeric_limits<size_t>::max() / 2,
    2 + std::numeric_limits<size_t>::max() / 2)
  == std::numeric_limits<size_t>::max());
static_assert(
  saturating_add(
    std::numeric_limits<size_t>::max() / 2,
    3 + std::numeric_limits<size_t>::max() / 2)
  == std::numeric_limits<size_t>::max());
static_assert(
  saturating_add(std::numeric_limits<size_t>::max(), 1)
  == std::numeric_limits<size_t>::max());
static_assert(
  saturating_add(std::numeric_limits<size_t>::max(), 0)
  == std::numeric_limits<size_t>::max());
static_assert(
  saturating_add(std::numeric_limits<size_t>::max() - 1, 1)
  == std::numeric_limits<size_t>::max());
static_assert(
  saturating_add(std::numeric_limits<size_t>::max() - 1, 2)
  == std::numeric_limits<size_t>::max());
static_assert(
  saturating_add(std::numeric_limits<size_t>::max() - 1, 0)
  == std::numeric_limits<size_t>::max() - 1);
static_assert(
  saturating_add(
    std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max())
  == std::numeric_limits<size_t>::max());
} // namespace

memory_limiter::memory_limiter(size_t limit)
  : _available(limit)
  , _max(limit) {}

memory_limiter::~memory_limiter() {
    vassert(
      _waiters.empty(),
      "must wait for all waiters to finish before destructing memory_limiter");
}

memory_limiter::units::units(memory_limiter* limiter, size_t amount)
  : _limiter(limiter)
  , _amount(amount) {}
memory_limiter::units::units(units&& other) noexcept
  : _limiter(std::exchange(other._limiter, nullptr))
  , _amount(other._amount) {}

void memory_limiter::units::release_all() {
    if (_limiter != nullptr) {
        _limiter->release(_amount);
        _limiter = nullptr;
    }
}
memory_limiter::units&
memory_limiter::units::operator=(units&& other) noexcept {
    if (&other == this) {
        return *this;
    }
    release_all();
    std::swap(other._limiter, _limiter);
    _amount = other._amount;
    return *this;
}
memory_limiter::units::~units() noexcept { release_all(); }

ss::future<memory_limiter::units>
memory_limiter::wait(size_t amount, ss::abort_source* as) {
    co_await acquire(amount, as);
    co_return units(this, amount);
}
ss::future<> memory_limiter::acquire(size_t amount, ss::abort_source* as) {
    vassert(
      amount <= _max,
      "must not acquire more memory than available. requesting={}, max={}",
      human::bytes(amount),
      human::bytes(_max));
    // Add a fast path if there are no waiters and there is available
    // memory.
    //
    // We check for waiters so that this limiter is fair.
    if (_waiters.empty() && has_available(amount)) {
        _available -= amount;
        return ss::now();
    }
    auto it = _waiters.emplace(_waiters.end(), amount);
    auto sub = as->subscribe([it]() noexcept {
        if (!it->state.has_value()) {
            it->state = waiter::memory_acquired::no;
            it->prom.set_value();
        }
    });
    // erase from the linked list only in this acquire, this simplifies the
    // lifetime requirements as this is the only function that mutates
    // _waiters. Doing this means there are less async concurrent list
    // modification issues to worry about.
    if (!sub) {
        _waiters.erase(it);
        as->check(); // Will always throw
    }
    return it->prom.get_future().then([this, it, as, sub = std::move(sub)] {
        auto state = it->state.value();
        _waiters.erase(it);
        // Throw if aborted, note that we can't do this unconditionally in the
        // case that we "acquire" memory but then the abort is requested before
        // this continuation runs.
        if (state == waiter::memory_acquired::no) {
            as->check();
        }
    });
}
void memory_limiter::release(size_t amount) {
    // Guard against bugs by capping our limit and using saturating math.
    _available = std::min(saturating_add(amount, _available), _max);
    notify();
}
size_t memory_limiter::max_memory() const { return _max; }
size_t memory_limiter::used_memory() const { return _max - _available; }
size_t memory_limiter::available_memory() const { return _available; }
bool memory_limiter::has_available(size_t amount) {
    return amount <= _available;
}
void memory_limiter::notify() {
    for (auto& waiter : _waiters) {
        if (waiter.state.has_value()) {
            continue;
        }
        // We have to check if the waiter has already been notified, as we
        // don't mutate the _waiters list here so that there is a single
        // owner in terms of who can mutate the list.
        //
        // Generally the number of waiters is expected to be low, so we
        // don't anticipate reactor stalls or having to skip over already
        // set promises to be an issue.
        if (!has_available(waiter.amount)) {
            // If we don't have memory available, our queue of waiters here
            // is fair, we need to stop to preserve fairness.
            break;
        }
        waiter.state = waiter::memory_acquired::yes;
        waiter.prom.set_value();
        // Decrement the memory here because we don't know when the
        // waiter will be scheduled to wake up and remove itself from
        // the list.
        _available -= waiter.amount;
        // Keep going until we run out of waiters or we don't have enough
        // memory to free waiters.
    }
}
} // namespace transform
