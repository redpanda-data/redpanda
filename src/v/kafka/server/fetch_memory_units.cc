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
  , _release_units_timer([this] { release_units(); })
  , _local_instance_fn(std::move(local_fn)) {
    _release_units_timer.arm(max_release_period);
}

ss::future<> fetch_memory_units_manager::stop() {
    _release_units_timer.cancel();
    _units_to_release.clear();
    return ss::now();
}

void fetch_memory_units_manager::units::adopt(
  fetch_memory_units_manager::units&& o) {
    // Adopts assert internally that the units are from the same semaphore.
    // So there is no need to assert that they are from the same shard here.
    kafka_units.adopt(std::move(o.kafka_units));
    fetch_units.adopt(std::move(o.fetch_units));
}

void fetch_memory_units_manager::release_units() {
    _release_units_timer.cancel();
    _release_units_timer.arm(max_release_period);

    _units_to_release.clear();
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
    if (shard == ss::this_shard_id() || !has_units()) {
        return;
    }

    ssx::background = ss::smp::submit_to(
      shard,
      [uk = std::move(kafka_units),
       uf = std::move(fetch_units)]() mutable noexcept {
          uk.return_all();
          uf.return_all();
      });
}

fetch_memory_units fetch_memory_units_manager::allocate_memory_units(
  size_t max_units, const size_t min_units, const bool require_min_units) {
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
    } else if (available_units >= min_units) {
        // only reserve memory if we have space for at least \ref min_units,
        // otherwise allocate none.
        units_to_alloc = std::min(available_units, max_units);
    }

    if (units_to_alloc == 0) {
        return {};
    }

    return {allocate_units(units_to_alloc), _local_instance_fn};
}

void fetch_memory_units_manager::release_units(units&& u) {
    auto map_it = _units_to_release.find(u.shard);
    if (map_it == _units_to_release.end()) {
        // Move the first set of semaphore_units to get a copy of
        // the semaphore pointer and the shard results originates
        // from.
        map_it
          = _units_to_release.insert_or_assign(u.shard, std::move(u)).first;
    } else {
        map_it->second.adopt(std::move(u));
    }

    if (map_it->second.num_units() >= max_release_size) {
        _units_to_release.erase(map_it);
    }
}

fetch_memory_units_manager::units
fetch_memory_units_manager::allocate_units(const size_t target) {
    units u{};
    u.fetch_units = ss::consume_units(_fetch_units, target);
    u.kafka_units = ss::consume_units(_kafka_units, target);
    return u;
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

    local_manager().release_units(std::move(_units));
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
        auto& m = local_manager();
        _units.adopt(m.allocate_units(target - current_units));
    }
}

fetch_memory_units_manager& fetch_memory_units::local_manager() {
    vassert(_local_instance_fn, "manager is empty");
    return _local_instance_fn();
}

} // namespace kafka
