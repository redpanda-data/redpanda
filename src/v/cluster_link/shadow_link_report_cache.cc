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

#include "cluster_link/shadow_link_report_cache.h"

#include "cluster_link/logger.h"
#include "cluster_link/service.h"
#include "ssx/future-util.h"

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

namespace cluster_link {

ss::future<model::status_report_ret_t>
default_shadow_link_report_fetcher::fetch_link_report(
  const model::name_t link_name, ss::abort_source&) {
    return _shadow_service->local().shadow_link_report(link_name);
}

shadow_link_report_cache::shadow_link_report_cache(
  config::binding<std::chrono::milliseconds> cache_ttl,
  std::unique_ptr<shadow_link_report_fetcher> fetcher)
  : _cache_ttl(std::move(cache_ttl))
  , _link_status_fetcher(std::move(fetcher)) {
    if (ss::this_shard_id() != fetcher_shard) {
        return;
    }
    _invalidator.set_callback([this]() {
        ssx::spawn_with_gate(
          _gate, [this] { return invalidate_stale_reports(); });
    });
}

ss::future<> shadow_link_report_cache::start() {
    // Only start the refresh timer on shard 0
    if (ss::this_shard_id() == fetcher_shard) {
        vlog(
          cllog.debug,
          "Starting shadow topic report cache with TTL: {}",
          _cache_ttl());
        _invalidator.arm(_cache_ttl());
    }
    co_return;
}

ss::future<> shadow_link_report_cache::stop() {
    vlog(cllog.debug, "Stopping shadow link report cache");
    _invalidator.cancel();
    _cached_cv.broken();
    _as.request_abort();
    co_await _gate.close();
    vlog(cllog.debug, "Stopped shadow link report cache");
}

ss::future<> shadow_link_report_cache::invalidate_stale_reports() noexcept {
    vassert(
      ss::this_shard_id() == fetcher_shard,
      "invalidate_stale_reports must be called on fetcher shard");
    const auto now = cache_clock::now();
    const auto ttl = _cache_ttl();

    chunked_vector<model::name_t> to_invalidate;
    to_invalidate.reserve(_cache.size());
    // collect a list of stale reports to invalidate
    auto next_scheduled_time = now + ttl;
    for (auto& [link_id, cached_report] : _cache) {
        auto age = now - cached_report.last_refresh_time;
        if (age >= ttl) {
            to_invalidate.push_back(link_id);
        } else {
            next_scheduled_time = std::min(
              next_scheduled_time, now + (_cache_ttl() - age));
        }
    }
    if (!to_invalidate.empty()) {
        vlog(
          cllog.debug,
          "Invalidating stale shadow link reports: {}",
          to_invalidate);
        co_await container().invoke_on_all(
          [&to_invalidate](auto& local) mutable {
              for (const auto& link_id : to_invalidate) {
                  local.invalidate_link_report(link_id);
              }
          });
    }
    if (_gate.is_closed()) {
        co_return;
    }
    // reschedule invalidation timer
    _invalidator.arm(next_scheduled_time);
}

bool shadow_link_report_cache::has_valid_cached_report(
  const model::name_t& link_name) const {
    auto it = _cache.find(link_name);
    if (it == _cache.end()) {
        return false;
    }
    const auto& cached_report = it->second;
    const auto now = cache_clock::now();
    const auto age = now - cached_report.last_refresh_time;
    return age <= _cache_ttl() && cached_report.report != nullptr;
}

void shadow_link_report_cache::maybe_dispatch_report_fetcher(
  const model::name_t& name, std::chrono::milliseconds backoff) noexcept {
    if (ss::this_shard_id() != fetcher_shard) {
        ssx::spawn_with_gate(_gate, [this, name, backoff] {
            return this->container().invoke_on(
              fetcher_shard, [name, backoff](auto& local) {
                  local.maybe_dispatch_report_fetcher(name, backoff);
              });
        });
        return;
    }
    if (_gate.is_closed() || _in_flight_fetches.contains(name)) {
        return;
    }
    _in_flight_fetches.insert(name);
    ssx::spawn_with_gate(_gate, [this, name, backoff]() {
        return do_dispatch_report_fetcher(name, backoff);
    });
}

ss::future<> shadow_link_report_cache::do_dispatch_report_fetcher(
  model::name_t link_id, std::chrono::milliseconds backoff) {
    // Expected to be called on a fetcher shard and under a gate.
    vassert(_in_flight_fetches.contains(link_id), "Fetch not marked in flight");
    vassert(
      ss::this_shard_id() == fetcher_shard,
      "do_dispatch_report_fetcher must be called on fetcher shard");
    auto failed = true;
    vlog(
      cllog.trace,
      "Dispatching shadow link report fetcher for link {} with backoff {}",
      link_id,
      backoff);
    try {
        // prev fetch request failed, backoff before retrying
        co_await ss::sleep_abortable(backoff, _as);
        auto report = co_await _link_status_fetcher->fetch_link_report(
          link_id, _as);
        if (report.has_value()) {
            // we have a new report, dispatch it to all shards.
            auto report_data = std::move(report.value());
            co_await container().invoke_on_all(
              [link_id, &report_data](auto& local) mutable {
                  return report_data.copy().then(
                    [&local, link_id](auto report) mutable {
                        local.update_local_report(link_id, std::move(report));
                    });
              });
            failed = false;
        } else if (!_gate.is_closed()) {
            vlog(
              cllog.warn,
              "Could not fetch shadow link report for link {}: err: {}",
              link_id,
              report.error());
            // treat as failure to trigger retry logic
        }
    } catch (...) {
        auto eptr = std::current_exception();
        auto log_level = ssx::is_shutdown_exception(eptr) ? ss::log_level::debug
                                                          : ss::log_level::warn;
        vlogl(
          cllog,
          log_level,
          "Failed to fetch shadow link report for link {}: {}",
          link_id,
          eptr);
    }
    _in_flight_fetches.erase(link_id);
    // schedule another fetch if needed
    // sometimes there is a competing force refresh that may have updated the
    // cache while we were fetching, in which case we will schedule a new fetch
    // without backoff, right away.
    auto schedule_another = (failed || !has_valid_cached_report(link_id))
                            && !_gate.is_closed();
    if (schedule_another) {
        auto next_backoff = no_backoff;
        if (failed) {
            next_backoff = std::min(
              std::max(backoff * 2, min_backoff), max_backoff);
        }
        maybe_dispatch_report_fetcher(link_id, next_backoff);
    }
    _cached_cv.broadcast();
}

void shadow_link_report_cache::update_local_report(
  const model::name_t& link_name,
  std::optional<model::shadow_link_status_report> report) noexcept {
    if (_gate.is_closed()) {
        return;
    }
    vlog(
      cllog.trace,
      "Updating local shadow link report for link {}: {}",
      link_name,
      report);
    _cache.erase(link_name);
    if (!report.has_value()) {
        return;
    }
    _cache.emplace(link_name, cached_link_report{std::move(report.value())});
    _cached_cv.broadcast();
}

void shadow_link_report_cache::invalidate_link_report(
  const model::name_t& link_name) {
    update_local_report(link_name, std::nullopt);
}

ss::future<shadow_link_report_cache::report_return_t>
shadow_link_report_cache::get_report(
  const model::name_t& link_name,
  std::chrono::milliseconds timeout,
  bool force_refresh) {
    auto holder = _gate.hold();
    if (force_refresh) {
        invalidate_link_report(link_name);
    }
    if (has_valid_cached_report(link_name)) {
        auto report_it = _cache.find(link_name);
        return ssx::now<report_return_t>(report_it->second.report);
    }
    maybe_dispatch_report_fetcher(link_name, no_backoff);
    return _cached_cv
      .wait(
        timeout,
        [this, link_name]() { return has_valid_cached_report(link_name); })
      .then([this, link_name] {
          auto report_it = _cache.find(link_name);
          if (report_it != _cache.end()) {
              return ssx::now<report_return_t>(report_it->second.report);
          }
          return ssx::now<report_return_t>(
            std::unexpected(errc::report_generation_unknown_error));
      })
      .handle_exception_type(
        [link_name](const ss::condition_variable_timed_out&) {
            vlog(
              cllog.warn,
              "Timeout waiting for shadow link report fetch for link {}",
              link_name);
            return ssx::now<report_return_t>(
              std::unexpected(errc::report_generation_timed_out));
        })
      .handle_exception_type([link_name](const ss::broken_condition_variable&) {
          vlog(
            cllog.debug,
            "Shadow link report fetch aborted for link {} due to cache "
            "shutdown",
            link_name);
          return ssx::now<report_return_t>(
            std::unexpected(errc::service_shutting_down));
      })
      .handle_exception_type([link_name](const ss::gate_closed_exception&) {
          vlog(
            cllog.debug,
            "Shadow link report fetch aborted for link {} due to cache "
            "shutdown",
            link_name);
          return ssx::now<report_return_t>(
            std::unexpected(errc::service_shutting_down));
      })
      .finally([holder = std::move(holder)] {});
}

} // namespace cluster_link
