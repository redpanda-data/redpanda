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

#pragma once

#include "absl/container/flat_hash_map.h"
#include "cluster_link/model/types.h"
#include "config/property.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timer.hh>

#include <chrono>

using namespace std::chrono_literals;

namespace cluster_link {

class service;

using link_status_ptr
  = ss::lw_shared_ptr<const model::shadow_link_status_report>;

using cache_clock = ss::lowres_clock;

/**
 * Interface for fetching shadow link reports.
 * This abstraction allows easy testing and pluggable report generation logic.
 */
class shadow_link_report_fetcher {
public:
    virtual ~shadow_link_report_fetcher() = default;

    /**
     * Fetch a shadow_link_status_report for a given link.
     * This should aggregate reports from all shards on the local node.
     *
     * @param link_id The link to fetch the report for
     * @return A future that resolves to the shadow_link_status_report_response
     */
    virtual ss::future<model::status_report_ret_t>
    fetch_link_report(model::name_t link_id, ss::abort_source&) = 0;
};

class default_shadow_link_report_fetcher : public shadow_link_report_fetcher {
public:
    explicit default_shadow_link_report_fetcher(
      ss::sharded<cluster_link::service>* shadow_service)
      : _shadow_service(shadow_service) {}
    ss::future<model::status_report_ret_t>
    fetch_link_report(model::name_t link_name, ss::abort_source& as) override;

private:
    ss::sharded<cluster_link::service>* _shadow_service;
};

/**
 * Per-link cached report data.
 * Uses copy-on-write semantics with lw_shared_ptr for efficient sharing.
 */
struct cached_link_report {
    // The cached report (immutable once set)
    link_status_ptr report;

    // When this report was last refreshed
    ss::lowres_clock::time_point last_refresh_time;

    explicit cached_link_report(model::shadow_link_status_report r)
      : report(
          ss::make_lw_shared<const model::shadow_link_status_report>(
            std::move(r)))
      , last_refresh_time(cache_clock::now()) {}
};

/**
 * Read-through cache for shadow topic reports.
 *
 * Key design features:
 * 1. Invalidates cache every `cache_ttl` seconds (cluster config)
 * 2. Invalidation runs on shard 0, results copied to all shards
 * 3. Copy-on-write with ss::lw_shared_ptr<const T> for efficient sharing
 * 4. Organized by link (one report per link)
 * 5. Supports get_report(link_id, topic) with optional force_refresh
 * 6. Pluggable report fetcher interface for testing
 */
class shadow_link_report_cache
  : public ss::peering_sharded_service<shadow_link_report_cache> {
public:
    shadow_link_report_cache(
      config::binding<std::chrono::milliseconds> cache_ttl,
      std::unique_ptr<shadow_link_report_fetcher> fetcher);

    /**
     * Start the cache. If running on shard 0, starts the periodic
     * invalidation timer.
     */
    ss::future<> start();

    /**
     * Stop the cache and clean up resources.
     */
    ss::future<> stop();

    /**
     * Get a report for a specific link and topic.
     *
     * @param link_id The link to get the report for
     * @param topic The topic to extract from the report
     * @param force_refresh If true, bypass cache and force a fresh fetch
     * @return A future that resolves to the topic's report, or nullptr if not
     * found
     */
    using report_return_t = std::expected<link_status_ptr, errc>;
    ss::future<report_return_t> get_report(
      const model::name_t& link_name,
      std::chrono::milliseconds timeout,
      bool force_refresh = false);

private:
    static constexpr auto fetcher_shard = ss::shard_id{0};
    static constexpr auto no_backoff = 0ms;
    static constexpr auto initial_backoff = 100ms;
    static constexpr auto min_backoff = 100ms;
    static constexpr auto max_backoff = 10000ms; // 10s

    bool has_valid_cached_report(const model::name_t& link_name) const;
    void invalidate_link_report(const model::name_t& link_nam);
    void maybe_dispatch_report_fetcher(
      const model::name_t& link_name,
      std::chrono::milliseconds backoff) noexcept;
    ss::future<> do_dispatch_report_fetcher(
      model::name_t link_name, std::chrono::milliseconds backoff);
    ss::future<> invalidate_stale_reports() noexcept;
    void update_local_report(
      const model::name_t& link_name,
      std::optional<model::shadow_link_status_report>) noexcept;

    config::binding<std::chrono::milliseconds> _cache_ttl;
    std::unique_ptr<shadow_link_report_fetcher> _link_status_fetcher;
    // Periodic refresh timer (only active on shard 0)
    ss::timer<cache_clock> _invalidator;
    // Per-link cached reports
    // Key: link_id, Value: cached report with metadata
    absl::flat_hash_map<model::name_t, cached_link_report> _cache;
    absl::flat_hash_set<model::name_t> _in_flight_fetches;
    ss::condition_variable _cached_cv;
    ss::gate _gate;
    ss::abort_source _as;
};

} // namespace cluster_link
