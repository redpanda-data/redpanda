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

#include "kafka/protocol/logger.h"
#include "ssx/future-util.h"

#include <seastar/core/reactor.hh>
#include <seastar/util/later.hh>

namespace kafka {

fetch_memory_units_manager::fetch_memory_units_manager(
  ssx::semaphore& kafka_units,
  ssx::semaphore& fetch_units,
  local_instance_fn&& local_fn,
  config::binding<std::optional<int32_t>>&& max_message_size)
  : _kafka_units(kafka_units)
  , _fetch_units(fetch_units)
  , _max_fetch_units(fetch_units.current())
  , _max_message_size(std::move(max_message_size))
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

fetch_memory_units fetch_memory_units_manager::allocate_memory_units(
  const model::ktp& ktp,
  size_t max_bytes,
  size_t max_batch_size,
  const size_t avg_batch_size,
  const bool require_max_batch_size) {
    vassert(!_gate.is_closed(), "fetch_memory_units_manager is stopped");

    static constexpr auto rate = 5min;

    if (max_bytes > _max_fetch_units) {
        thread_local static ss::logger::rate_limit rate_limit(rate);
        klog.log(
          ss::log_level::debug,
          rate_limit,
          "{}: max_bytes({}) exceeds available fetch memory ({}). Consider "
          "reducing `max.partition.fetch.bytes` on the consumers. Setting "
          "max_bytes to available fetch memory.",
          ktp,
          max_bytes,
          _max_fetch_units);
        max_bytes = _max_fetch_units;
    }

    const auto& configured_max = _max_message_size();
    const size_t max_message_size = configured_max.has_value()
                                      ? static_cast<size_t>(*configured_max)
                                      : _max_fetch_units;

    if (max_batch_size > max_message_size) {
        thread_local static ss::logger::rate_limit rate_limit(rate);
        klog.log(
          ss::log_level::error,
          rate_limit,
          "{}: max_batch_size({}) exceeds max message size ({}). "
          "Consider reducing `message.max.bytes` for the topic. Setting "
          "max_batch_size to max message size.",
          ktp,
          max_batch_size,
          max_message_size);
        max_batch_size = max_message_size;
    }

    const size_t available_units = std::min(
      _kafka_units.current(), _fetch_units.current());

    // There is no relation enforced for \ref max_bytes and \ref max_batch_size.
    // Normally max_bytes >= max_batch_size, however, that is not always the
    // case.
    const auto max_units = std::max(max_bytes, max_batch_size);

    size_t units_to_alloc = 0;
    if (require_max_batch_size) {
        // If \ref require_max_batch_size is true then we must read at least
        // \ref max_batch_size. Even if that means the memory semaphores result
        // in negative units.
        units_to_alloc = std::max(
          max_batch_size, std::min(max_units, available_units));
    } else if (available_units >= avg_batch_size) {
        // Only reserve memory if we have space for at least \ref
        // avg_batch_size, otherwise allocate none.
        units_to_alloc = std::min(available_units, max_units);
    }

    return {allocate_units(units_to_alloc), _local_instance_fn};
}

fetch_memory_units fetch_memory_units_manager::zero_units() {
    return {allocate_units(0), _local_instance_fn};
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
fetch_memory_units_manager::allocate_units(const size_t target) {
    return {
      ss::consume_units(_kafka_units, target),
      ss::consume_units(_fetch_units, target)};
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
        _units.adopt(local_manager().allocate_units(target - current_units));
    }
}

fetch_memory_units_manager& fetch_memory_units::local_manager() {
    return _local_instance_fn();
}

} // namespace kafka
