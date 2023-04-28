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

#pragma once

#include "seastarx.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/log.hh>
#include <seastar/util/memory_diagnostics.hh>
#include <seastar/util/noncopyable_function.hh>

#include <fmt/core.h>
#include <fmt/format.h>

#include <optional>

template<>
struct fmt::formatter<seastar::memory::allocation_site>
  : fmt::formatter<std::string_view> {
    template<typename FormatContext>
    auto
    format(const seastar::memory::allocation_site& site, FormatContext& ctx) {
        return fmt::format_to(
          ctx.out(), "{} {} {}", site.size, site.count, site.backtrace);
    }
};

/// Very simple service enabling memory profiling on all shards.
class memory_sampling : public ss::peering_sharded_service<memory_sampling> {
public:
    struct serialized_memory_profile {
        struct allocation_site {
            // cumulative size at the allocation site (upscaled not sampled)
            size_t size;
            // count at the allocation site
            size_t count;
            // backtrace of this allocation site
            ss::sstring backtrace;

            allocation_site(size_t size, size_t count, ss::sstring backtrace)
              : size(size)
              , count(count)
              , backtrace(std::move(backtrace)) {}
        };

        /// shard id of this profile
        ss::shard_id shard_id;
        /// Backtraces of this shard
        std::vector<allocation_site> allocation_sites;

        explicit serialized_memory_profile(
          long shard_id, std::vector<allocation_site> traces)
          : shard_id(shard_id)
          , allocation_sites(std::move(traces)) {}
    };

    /// Starts the service (enables memory sampling, sets up additional OOM
    /// information and enables logging in highwatermark situations)
    void start();
    ss::future<> stop();

    /// Get the serialized memory profile for a shard or all shards if shard_id
    /// is nullopt
    ss::future<std::vector<serialized_memory_profile>>
    get_sampled_memory_profiles(std::optional<size_t> shard_id);

    /// Notify the memory sampling service that a memory reclaim from the
    /// seastar allocator has happened. Used by the batch_cache
    void notify_of_reclaim();

    /// Constructs the service. Logger will be used to log top stacks under high
    /// memory pressure
    explicit memory_sampling(ss::logger& logger);

    /// Constructor as above but allows overriding high memory thresholds. Used
    /// for testing.
    explicit memory_sampling(
      ss::logger& logger,
      double first_log_limit_fraction,
      double second_log_limit_fraction);

    /// Returns the callback we register to run on OOM to add the memory
    /// sampling output
    static ss::noncopyable_function<void(ss::memory::memory_diagnostics_writer)>
    get_oom_diagnostics_callback();

private:
    /// Starts the background future running the allocation site logging on low
    /// available memory
    ss::future<> start_low_available_memory_logging();

    /// Returns the serialized memory_profile for the current shard
    static memory_sampling::serialized_memory_profile
    get_sampled_memory_profile();

    ss::logger& _logger;

    // When a memory reclaim from the seastar allocator happens the batch_cache
    // notifies us via below condvar. If we see a new low watermark that's and
    // we are below 20% then we log once and a second time the first time we saw
    // a 10% lower watermark. Values are overridable for tests
    double _first_log_limit_fraction;
    double _second_log_limit_fraction;
    ss::condition_variable _low_watermark_cond;
    ss::gate _low_watermark_gate;
};
