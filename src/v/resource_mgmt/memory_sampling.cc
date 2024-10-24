/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "resource_mgmt/memory_sampling.h"

#include "base/vlog.h"
#include "resource_mgmt/available_memory.h"
#include "ssx/future-util.h"
#include "ssx/sformat.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/memory_diagnostics.hh>

#include <chrono>
#include <limits>
#include <vector>

constexpr std::string_view diagnostics_header() { return "Top-N alloc sites:"; }

constexpr std::string_view confluence_reference() {
    return "If you work at Redpanda please refer to "
           "https://vectorizedio.atlassian.net/l/cp/iuEMd2NN\n";
}

fmt::appender fmt::formatter<seastar::memory::allocation_site>::format(
  const seastar::memory::allocation_site& site,
  fmt::format_context& ctx) const {
    return fmt::format_to(
      ctx.out(),
      "size: {} count: {} at: {}",
      site.size,
      site.count,
      site.backtrace);
}

/// Put `top_n` allocation sites into the front of `allocation_sites`
static void top_n_allocation_sites(
  std::vector<ss::memory::allocation_site>& allocation_sites, size_t top_n) {
    std::partial_sort(
      allocation_sites.begin(),
      allocation_sites.begin() + top_n,
      allocation_sites.end(),
      [](const auto& lhs, const auto& rhs) { return lhs.size > rhs.size; });
}

ss::noncopyable_function<void(ss::memory::memory_diagnostics_writer)>
memory_sampling::get_oom_diagnostics_callback() {
    // preallocate those so that we don't allocate on OOM
    std::vector<seastar::memory::allocation_site> allocation_sites(1000);
    std::vector<char> format_buf(1000);

    return [allocation_sites = std::move(allocation_sites),
            format_buf = std::move(format_buf)](
             seastar::memory::memory_diagnostics_writer writer) mutable {
        auto num_sites = ss::memory::sampled_memory_profile(
          allocation_sites.data(), allocation_sites.size());

        const size_t top_n = std::min(size_t(10), num_sites);
        top_n_allocation_sites(allocation_sites, top_n);

        writer("Alloc sites legend:\n"
               "    size: the estimated total size of all allocations at this "
               "stack (i.e., adjusted up from observed samples)\n"
               "    count: the number of live samples at this "
               "stack (i.e., NOT adjusted up from observed samples)\n"
               "    at: the backtrace for this allocation site\n");
        writer(diagnostics_header());
        writer("\n");

        for (size_t i = 0; i < top_n; ++i) {
            auto bytes_written = fmt::format_to_n(
                                   format_buf.begin(),
                                   format_buf.size(),
                                   "{}\n",
                                   allocation_sites[i])
                                   .size;

            writer(std::string_view(format_buf.data(), bytes_written));
        }

        writer(confluence_reference());
    };
}

/// We want to print the top-n allocation sites on OOM
/// Set this up and make sure we don't allocate extra at that point
void setup_additional_oom_diagnostics() {
    seastar::memory::set_additional_diagnostics_producer(
      memory_sampling::get_oom_diagnostics_callback());
}

void memory_sampling::start_low_available_memory_logging() {
    // We want some periodic logging "on the way" to OOM. At the same time we
    // don't want to spam the logs. Hence, we periodically look at the available
    // memory low watermark (free seastar memory plus what's reclaimable from
    // the batch cache). If we see that we have crossed the 10% and 20% marks we
    // log the allocation sites. We stop afterwards.

    // The periodic check is implemented using a timer. This is a very simple
    // approach which potentially might miss steps if we are consuming memory
    // very quickly. An alternative approach that was tested was to hook this
    // into the batch cache. However, doing this in reclaim is tricky as you
    // want to avoid allocating there.  Equally, it's not entirely clear whether
    // that is the right place as reclaim just "moves" memory from "reclaimable"
    // to "free".

    size_t first_log_limit = _first_log_limit_fraction
                             * seastar::memory::stats().total_memory();
    size_t second_log_limit = _second_log_limit_fraction
                              * seastar::memory::stats().total_memory();
    size_t next_log_limit = first_log_limit;

    _logging_timer.set_callback(
      [this, first_log_limit, second_log_limit, next_log_limit]() mutable {
          auto current_low_water_mark
            = resources::available_memory::local().available_low_water_mark();

          if (current_low_water_mark > next_log_limit) {
              return;
          }

          auto allocation_sites = ss::memory::sampled_memory_profile();
          const size_t top_n = std::min(size_t(5), allocation_sites.size());
          top_n_allocation_sites(allocation_sites, top_n);

          vlog(
            _logger.info,
            "{} bytes of available memory left - {} {}",
            next_log_limit,
            diagnostics_header(),
            fmt::join(
              allocation_sites.begin(), allocation_sites.begin() + top_n, "|"));

          if (next_log_limit == first_log_limit) {
              next_log_limit = second_log_limit;
          } else {
              _logging_timer.cancel();
          }
      });

    _logging_timer.arm_periodic(_log_check_frequency);
}

memory_sampling::memory_sampling(
  ss::logger& logger, config::binding<bool> enabled)
  : memory_sampling(
      logger, std::move(enabled), std::chrono::seconds(60), 0.2, 0.1) {}

memory_sampling::memory_sampling(
  ss::logger& logger,
  config::binding<bool> enabled,
  std::chrono::seconds log_check_frequency,
  double first_log_limit_fraction,
  double second_log_limit_fraction)
  : _logger(logger)
  , _enabled(std::move(enabled))
  , _first_log_limit_fraction(first_log_limit_fraction)
  , _second_log_limit_fraction(second_log_limit_fraction)
  , _log_check_frequency(log_check_frequency) {
    _enabled.watch([this]() { on_enabled_change(); });
}

void memory_sampling::on_enabled_change() {
    // We chose a sampling rate of ~3MB. From testing this has a very low
    // overhead of something like ~1%. We could still get away with something
    // smaller like 1MB and have acceptable overhead (~3%) but 3MB should be a
    // safer default for the initial rollout.
    const size_t sampling_rate = 3000037;

    // Note no logging here as seastar already logs about this
    if (_enabled()) {
        if (ss::memory::get_heap_profiling_sample_rate() == sampling_rate) {
            return;
        }

        ss::memory::set_heap_profiling_sampling_rate(sampling_rate);
    } else {
        if (ss::memory::get_heap_profiling_sample_rate() == 0) {
            return;
        }

        ss::memory::set_heap_profiling_sampling_rate(0);
    }
}

void memory_sampling::start() {
    setup_additional_oom_diagnostics();

    // start now if enabled
    on_enabled_change();

    start_low_available_memory_logging();
}

ss::future<> memory_sampling::stop() {
    _logging_timer.cancel();
    co_return;
}

memory_sampling::serialized_memory_profile
memory_sampling::get_sampled_memory_profile() {
    auto stacks = ss::memory::sampled_memory_profile();

    std::vector<memory_sampling::serialized_memory_profile::allocation_site>
      allocation_sites;
    allocation_sites.reserve(stacks.size());

    for (auto& stack : stacks) {
        allocation_sites.emplace_back(
          stack.size,
          stack.count,
          ssx::sformat("{}", std::move(stack.backtrace)));
    }

    return memory_sampling::serialized_memory_profile{
      ss::this_shard_id(), std::move(allocation_sites)};
}

ss::future<std::vector<memory_sampling::serialized_memory_profile>>
memory_sampling::get_sampled_memory_profiles(std::optional<size_t> shard_id) {
    using result_t = memory_sampling::serialized_memory_profile;
    std::vector<result_t> resp;

    if (shard_id.has_value()) {
        resp.push_back(co_await container().invoke_on(*shard_id, [](auto&) {
            return memory_sampling::get_sampled_memory_profile();
        }));
    } else {
        resp = co_await container().map_reduce0(
          [](auto&) { return memory_sampling::get_sampled_memory_profile(); },
          std::vector<result_t>{},
          [](std::vector<result_t> all, result_t result) {
              all.push_back(std::move(result));
              return all;
          });
    }

    co_return resp;
}
