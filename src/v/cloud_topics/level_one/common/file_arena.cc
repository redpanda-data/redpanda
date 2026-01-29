/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/file_arena.h"

#include "cloud_topics/level_one/common/file_arena_probe.h"
#include "ssx/future-util.h"
#include "ssx/semaphore.h"
#include "utils/human.h"

#include <chrono>

using namespace std::chrono_literals;

namespace cloud_topics::l1 {

staging_file_with_reservation::staging_file_with_reservation(
  std::unique_ptr<staging> file, ssx::semaphore_units disk_reservation)
  : _file(std::move(file))
  , _disk_reservation(std::move(disk_reservation)) {}

ss::future<size_t> staging_file_with_reservation::size() {
    return _file->size();
}

ss::future<ss::output_stream<char>>
staging_file_with_reservation::output_stream() {
    return _file->output_stream();
}

void staging_file_with_reservation::finalize(size_t file_size) {
    size_t units = _disk_reservation.count();
    if (units > file_size) {
        // We asked for more units than we used. Return the excess to the
        // arena's local pool.
        size_t excess = units - file_size;
        _disk_reservation.return_units(excess);
    }
}

ss::future<> staging_file_with_reservation::remove() {
    co_await _file->remove();
    _disk_reservation.return_all();
}

file_arena_manager::file_arena_manager(
  config::binding<size_t> scratch_space_size_bytes,
  config::binding<double> scratch_space_soft_limit_size_percent,
  config::binding<size_t> block_size,
  std::string_view arena_ctx,
  ss::logger& logger)
  : _scratch_space_size_bytes(scratch_space_size_bytes)
  , _scratch_space_soft_limit_size_percent(
      scratch_space_soft_limit_size_percent)
  , _block_size(block_size)
  , _disk_bytes_reservable(0, fmt::format("file_arena::{}::global", arena_ctx))
  , _logger(logger) {
    // Set up watch functions on bindings
    _scratch_space_size_bytes.watch([this] { update_disk_limits(); });
    _scratch_space_soft_limit_size_percent.watch(
      [this] { update_disk_limits(); });

    // Initialize
    update_disk_limits();
}

ss::future<> file_arena_manager::start(ss::sharded<file_arena>* arena) {
    _arena = arena;
    ssx::spawn_with_gate(_gate, [this] { return disk_space_monitor(); });
    co_return;
}

ss::future<> file_arena_manager::stop() {
    _as.request_abort();
    _disk_space_monitor_cv.broken();
    _disk_bytes_reservable.broken();
    co_await _gate.close();
}

bool file_arena_manager::disk_space_soft_limit_exceeded() const {
    // we use the available_units semaphore interface which is adjusted to
    // reflect any deficits (cores owe us units), and might be negative.
    const auto used = static_cast<ssize_t>(_disk_bytes_reservable_total)
                      - _disk_bytes_reservable.available_units();
    return used > static_cast<ssize_t>(_disk_bytes_reservable_soft_limit);
}

ss::future<> file_arena_manager::disk_space_monitor() {
    while (!_gate.is_closed()) {
        co_await _disk_space_monitor_cv.wait(
          [this] { return disk_space_soft_limit_exceeded(); });

        try {
            co_await reclaim_disk_space();
        } catch (...) {
            vlog(
              _logger.info,
              "Recoverable error reclaiming scratch space: {}",
              std::current_exception());
        }

        /*
         * even when we the system is driven to high disk space utilization
         * frequently, we don't want to go too hard with the reclaim loop.
         */
        co_await ss::sleep_abortable(1s, _as);
    }
}

ss::future<> file_arena_manager::reclaim_disk_space() {
    auto excess_units = co_await _arena->map_reduce0(
      [](file_arena& a) { return a.release_unused_disk_units(); },
      size_t{0},
      [](size_t acc, size_t units) { return acc + units; });

    if (excess_units > 0) {
        _disk_bytes_reservable.signal(excess_units);
    }

    vlog(
      _logger.debug,
      "Collected {} unused shard-local disk reservation units for the global "
      "pool, new reservable total: {}",
      human::bytes(excess_units),
      human::bytes(_disk_bytes_reservable.current()));
}

void file_arena_manager::update_disk_limits() {
    const auto new_total_size = _scratch_space_size_bytes();
    const auto new_soft_percent = _scratch_space_soft_limit_size_percent();

    // current settings
    const auto prev_total_size = _disk_bytes_reservable_total;
    const auto prev_soft_limit = _disk_bytes_reservable_soft_limit;
    const auto prev_available = _disk_bytes_reservable.available_units();

    _disk_bytes_reservable_soft_limit = static_cast<size_t>(
      static_cast<double>(new_total_size) * (new_soft_percent / 100.0));

    if (new_total_size > prev_total_size) {
        auto delta = new_total_size - prev_total_size;
        _disk_bytes_reservable_total = new_total_size;
        _disk_bytes_reservable.signal(delta);
    } else if (new_total_size < prev_total_size) {
        auto delta = prev_total_size - new_total_size;
        _disk_bytes_reservable_total = new_total_size;
        _disk_bytes_reservable.consume(delta);
    }

    // Alert the disk space monitor in case there is any work to be done due to
    // the new configurations.
    _disk_space_monitor_cv.signal();

    auto format_reservable = [](auto v) {
        if (v >= 0) {
            return fmt::format("{}", human::bytes(v));
        }
        return fmt::format("{} ({})", human::bytes(0), v);
    };

    vlog(
      _logger.info,
      "Setting file arena scratch space total reservation: {}, soft limit: {}, "
      "available: {} (previous total: {}, soft limit: {}, available: {})",
      human::bytes(new_total_size),
      human::bytes(_disk_bytes_reservable_soft_limit),
      format_reservable(_disk_bytes_reservable.available_units()),
      human::bytes(prev_total_size),
      human::bytes(prev_soft_limit),
      format_reservable(prev_available));
}

file_arena::file_arena(
  io* io,
  config::binding<size_t> scratch_space_size_bytes,
  config::binding<double> scratch_space_soft_limit_size_percent,
  config::binding<size_t> block_size,
  std::string_view arena_ctx,
  ss::logger& logger)
  : _io(io)
  , _local_disk_bytes_reservable(
      0, fmt::format("file_arena::{}::local", arena_ctx))
  , _logger(logger)
  , _probe(std::make_unique<file_arena_probe>(this, arena_ctx)) {
    if (ss::this_shard_id() == manager_shard) {
        _core0_arena_manager = std::make_unique<file_arena_manager>(
          scratch_space_size_bytes,
          scratch_space_soft_limit_size_percent,
          block_size,
          arena_ctx,
          logger);
    }
}

ss::future<> file_arena::start() {
    if (ss::this_shard_id() == manager_shard) {
        co_await _core0_arena_manager->start(&container());
    }
    _probe->setup_metrics();
}

ss::future<> file_arena::stop() {
    _local_disk_bytes_reservable.broken();
    if (_core0_arena_manager) {
        co_await _core0_arena_manager->stop();
    }
    _probe.reset(nullptr);
}

ss::future<std::expected<staging_file_with_reservation, io::errc>>
file_arena::create_tmp_file(size_t required_file_size, ss::abort_source& as) {
    auto units = co_await reserve_units(required_file_size, as);
    auto file_res = co_await _io->create_tmp_file();
    if (!file_res.has_value()) {
        co_return std::unexpected(file_res.error());
    }

    co_return staging_file_with_reservation(
      std::move(file_res).value(), std::move(units));
}

ss::future<ssx::semaphore_units>
file_arena::reserve_units(size_t required_file_size, ss::abort_source& as) {
    static const auto init_backoff = 100ms;
    static const auto max_backoff = 1000ms;

    auto backoff = init_backoff;
    while (true) {
        auto local_opt_units = ss::try_get_units(
          _local_disk_bytes_reservable, required_file_size);
        if (local_opt_units.has_value()) {
            if (_logger.is_enabled(ss::log_level::trace)) {
                vlog(
                  _logger.trace,
                  "Allocated {} disk reservation from shard-local pool. Total "
                  "available {}",
                  human::bytes(local_opt_units.value().count()),
                  human::bytes(_local_disk_bytes_reservable.current()));
            }
            co_return std::move(local_opt_units).value();
        }

        auto units = co_await request_units_from_core0_manager();
        if (units > 0) {
            _local_disk_bytes_reservable.signal(units);
            if (_logger.is_enabled(ss::log_level::debug)) {
                vlog(
                  _logger.debug,
                  "Received {} disk reservation from global pool for this "
                  "shard. New total available {}",
                  human::bytes(units),
                  human::bytes(_local_disk_bytes_reservable.current()));
            }
        }

        backoff = std::min(backoff, max_backoff);

        co_await ss::sleep_abortable(backoff, as);

        backoff *= 2;
    }
}

size_t file_arena::release_unused_disk_units() {
    const auto units = _local_disk_bytes_reservable.current();
    if (units > 0) {
        _local_disk_bytes_reservable.consume(units);
    }
    return units;
}

size_t file_arena_manager::get_units(ss::shard_id shard) {
    const auto units = std::min(
      _disk_bytes_reservable.current(), _block_size());

    if (units > 0) {
        _disk_bytes_reservable.consume(units);
    }

    if (disk_space_soft_limit_exceeded()) {
        _disk_space_monitor_cv.signal();
    }

    vlog(
      _logger.debug,
      "Allocated {} disk reservation for shard {} from global pool. New total "
      "available {}",
      human::bytes(units),
      shard,
      human::bytes(_disk_bytes_reservable.current()));

    return units;
}

ss::future<size_t> file_arena::request_units_from_core0_manager() {
    auto from = ss::this_shard_id();
    return container().invoke_on(manager_shard, [from](file_arena& a) {
        return a._core0_arena_manager->get_units(from);
    });
}

} // namespace cloud_topics::l1
