/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/server/fetch_memory_units.h"

#include "ssx/future-util.h"

#include <seastar/core/reactor.hh>
#include <seastar/util/later.hh>

namespace kafka {

fetch_memory_units_manager::fetch_memory_units_manager(
  ssx::semaphore& kafka_units,
  ssx::semaphore& fetch_units,
  local_instance_fn&& local_fn)
  : _kafka_units(kafka_units)
  , _fetch_units(fetch_units)
  , _release_units_timer([this] { release_all_units_to_semaphore(); })
  , _local_instance_fn(std::move(local_fn)) {
    _release_units_timer.arm_periodic(max_release_period);
}

ss::future<> fetch_memory_units_manager::stop() {
    _release_units_timer.cancel();
    release_all_units_to_semaphore();
    co_await _gate.close();
}

void fetch_memory_units_manager::units::adopt(
  fetch_memory_units_manager::units&& o) {
    // Adopts assert internally that the units are from the same semaphore.
    // So there is no need to assert that they are from the same shard here.
    kafka_units.adopt(std::move(o.kafka_units));
    fetch_units.adopt(std::move(o.fetch_units));
}

void fetch_memory_units_manager::release_all_units_to_semaphore() {
    for (auto& u : _units_to_release) {
        release_units_to_semaphore(std::move(u.second));
    }
}

fetch_memory_units_manager::units&
fetch_memory_units_manager::units::operator=(units&& o) noexcept {
    if (this != &o) {
        this->~units();
        new (this) units(std::move(o));
    }
    return *this;
}

fetch_memory_units_manager::units::~units() noexcept {
    vassert(
      !has_units() || shard == ss::this_shard_id(),
      "foreign units need to be released via the fetch_memory_units_manager");
}

ss::future<fetch_memory_units>
fetch_memory_units_manager::allocate_memory_units(
  size_t max_units,
  const size_t min_units,
  const bool require_min_units,
  ss::abort_source& as) {
    vassert(!_gate.is_closed(), "fetch_memory_units_manager is stopped");

    const size_t available_units = std::min(
      _kafka_units.current(), _fetch_units.current());
    // Note that it's not currently enforced that max_units >= min_units. Hence
    // we set max_units to the larger of the two here to ensure that is the
    // case.
    max_units = std::max(max_units, min_units);

    size_t units_to_alloc = 0;
    if (require_min_units) {
        // if \ref require_min_units is true then we must read at least \ref
        // min_units. So allocate at least that many even if it causes the
        // semaphores to become negative.
        units_to_alloc = std::max(
          min_units, std::min(max_units, available_units));
        return get_units(units_to_alloc, as);
    } else if (available_units >= min_units) {
        // only reserve memory if we have space for at least \ref min_units,
        // otherwise allocate none.
        units_to_alloc = std::min(available_units, max_units);
        return ss::make_ready_future<fetch_memory_units>(
          try_get_units(units_to_alloc));
    }

    return ss::make_ready_future<fetch_memory_units>(zero_units());
}

fetch_memory_units fetch_memory_units_manager::zero_units() {
    return fetch_memory_units{
      {ss::consume_units(_kafka_units, 0), ss::consume_units(_fetch_units, 0)},
      _local_instance_fn};
}

void fetch_memory_units_manager::release_units_to_manager(units&& u) {
    vassert(!_gate.is_closed(), "fetch_memory_units_manager is stopped");

    auto [map_it, succ] = _units_to_release.try_emplace(u.shard, std::move(u));
    if (!succ) {
        map_it->second.adopt(std::move(u));
    }

    if (map_it->second.num_units() >= max_release_size) {
        release_units_to_semaphore(std::move(map_it->second));
    }
}

void fetch_memory_units_manager::release_units_to_semaphore(units&& u) {
    ssx::spawn_with_gate(_gate, [&] mutable {
        return ss::smp::submit_to(
          u.shard,
          [uk = std::move(u.kafka_units),
           uf = std::move(u.fetch_units)]() mutable noexcept {
              uk.return_all();
              uf.return_all();
          });
    });
}

fetch_memory_units_manager::units
fetch_memory_units_manager::consume_units(const size_t target) {
    return {
      ss::consume_units(_kafka_units, target),
      ss::consume_units(_fetch_units, target)};
}

ss::future<fetch_memory_units> fetch_memory_units_manager::get_units(
  const size_t target, ss::abort_source& as) {
    try {
        // Try to get the fetch units first. They should normally be the lesser
        // of the two and holding them won't block other request types on
        // this shard.
        auto fetch_units = co_await ss::get_units(_fetch_units, target, as);
        auto kafka_units = co_await ss::get_units(_kafka_units, target, as);
        co_return fetch_memory_units{
          {std::move(kafka_units), std::move(fetch_units)}, _local_instance_fn};
    } catch (...) {
        co_return zero_units();
    }
}

fetch_memory_units
fetch_memory_units_manager::try_get_units(const size_t target) {
    auto fetch_units_opt = ss::try_get_units(_fetch_units, target);
    if (!fetch_units_opt) {
        return zero_units();
    }
    auto kafka_units_opt = ss::try_get_units(_kafka_units, target);
    if (!kafka_units_opt) {
        return zero_units();
    }

    return {
      {std::move(*kafka_units_opt), std::move(*fetch_units_opt)},
      _local_instance_fn};
}

fetch_memory_units::fetch_memory_units(
  fetch_memory_units_manager::units&& units,
  fetch_memory_units_manager::local_instance_fn& local_instance_fn)
  : _units(std::move(units))
  , _local_instance_fn(local_instance_fn) {}

fetch_memory_units::~fetch_memory_units() noexcept {
    if (_units.shard == ss::this_shard_id() || !has_units()) {
        return;
    }

    local_manager().release_units_to_manager(std::move(_units));
}

fetch_memory_units&
fetch_memory_units::operator=(fetch_memory_units&& o) noexcept {
    if (this != &o) {
        this->~fetch_memory_units();
        new (this) fetch_memory_units(std::move(o));
    }
    return *this;
}

void fetch_memory_units::adjust_units(const size_t target) {
    vassert(
      ss::this_shard_id() == _units.shard,
      "units need to be adjusted on their source shard");
    const size_t current_units = _units.num_units();

    if (target < current_units) {
        _units.kafka_units.return_units(current_units - target);
        _units.fetch_units.return_units(current_units - target);
    }
    if (target > current_units) {
        _units.adopt(local_manager().consume_units(target - current_units));
    }
}

fetch_memory_units_manager& fetch_memory_units::local_manager() {
    return _local_instance_fn();
}

} // namespace kafka
