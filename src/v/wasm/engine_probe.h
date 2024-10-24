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

#include "base/seastarx.h"
#include "metrics/metrics.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/weak_ptr.hh>

#include <absl/container/btree_map.h>

namespace wasm {

class engine_probe_cache;

namespace internal {
class engine_probe_impl
  : public ss::enable_lw_shared_from_this<engine_probe_impl> {
public:
    engine_probe_impl(engine_probe_cache* cache, ss::sstring name);
    engine_probe_impl(const engine_probe_impl&) = delete;
    engine_probe_impl(engine_probe_impl&&) = delete;
    engine_probe_impl& operator=(const engine_probe_impl&) = delete;
    engine_probe_impl& operator=(engine_probe_impl&&) = delete;
    ~engine_probe_impl();
    void report_memory_usage_delta(int64_t);
    void report_max_memory_delta(int64_t);
    void increment_cpu_time(ss::steady_clock_type::duration d);

private:
    ss::sstring _name;
    engine_probe_cache* _cache;
    int64_t _memory_usage = 0;
    int64_t _max_memory = 0;
    ss::steady_clock_type::duration _cpu_time = std::chrono::seconds(0);
    metrics::public_metric_groups _public_metrics;
};
} // namespace internal

/**
 * A probe for all types of wasm engines.
 *
 * Used to track things like memory and CPU usage across multiple engines. There
 * are cases (ie deployment) where there maybe multiple versions of the same
 * probe on the same core at once, so this class is a small utility around
 * ensuring we only report the delta for gauges.
 */
class engine_probe {
public:
    explicit engine_probe(ss::lw_shared_ptr<internal::engine_probe_impl> impl);
    engine_probe(const engine_probe&) = delete;
    engine_probe& operator=(const engine_probe&) = delete;
    engine_probe(engine_probe&&) = default;
    engine_probe& operator=(engine_probe&&) = default;
    ~engine_probe();

    void increment_cpu_time(ss::steady_clock_type::duration);
    void report_memory_usage(uint32_t);
    void report_max_memory(uint32_t);

private:
    ss::lw_shared_ptr<internal::engine_probe_impl> _impl;
    uint32_t _last_reported_memory_usage = 0;
    uint32_t _last_reported_max_memory = 0;
};

/**
 * A cache for managing probes for all engines on a core.
 *
 * Since during deployments it's possible to have multiple versions of an engine
 * with the same name, we need to ensure there are not conflicting probes by
 * caching and reusing them.
 *
 * This cache must outlive all probes it creates and should have application
 * scoped lifetimes.
 */
class engine_probe_cache {
public:
    /**
     * Create an engine probe under a given name.
     */
    engine_probe make_probe(const ss::sstring&);

private:
    friend internal::engine_probe_impl;

    void remove_probe(const ss::sstring&);

    absl::btree_map<ss::sstring, internal::engine_probe_impl*> _probe_cache;
};

} // namespace wasm
