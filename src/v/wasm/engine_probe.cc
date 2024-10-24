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

#include "engine_probe.h"

#include "base/vassert.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/weak_ptr.hh>

namespace wasm {

namespace internal {

engine_probe_impl::engine_probe_impl(
  engine_probe_cache* cache, ss::sstring name)
  : _name(std::move(name))
  , _cache(cache) {
    namespace sm = ss::metrics;

    auto name_label = sm::label("function_name");
    const std::vector<sm::label_instance> labels = {
      name_label(_name),
    };
    _public_metrics.add_group(
      prometheus_sanitize::metrics_name("wasm_engine"),
      {
        sm::make_counter(
          "cpu_seconds_total",
          sm::description(
            "Total CPU time (in seconds) spent inside a WebAssembly "
            "function"),
          labels,
          [this] { return std::chrono::duration<double>(_cpu_time).count(); })
          .aggregate({ss::metrics::shard_label}),
        sm::make_gauge(
          "memory_usage",
          sm::description("Amount of memory usage for a WebAssembly function"),
          labels,
          [this] { return _memory_usage; })
          .aggregate({ss::metrics::shard_label}),
        sm::make_gauge(
          "max_memory",
          [this] { return _max_memory; },
          sm::description("Max amount of memory for a WebAssembly function"),
          labels)
          .aggregate({ss::metrics::shard_label}),
      });
}

engine_probe_impl::~engine_probe_impl() {
    // Remove from cache when deleted.
    _cache->remove_probe(_name);
}

void engine_probe_impl::report_memory_usage_delta(int64_t delta) {
    _memory_usage += delta;
}

void engine_probe_impl::report_max_memory_delta(int64_t delta) {
    _max_memory += delta;
}

void engine_probe_impl::increment_cpu_time(ss::steady_clock_type::duration d) {
    _cpu_time += d;
}

} // namespace internal

engine_probe::engine_probe(ss::lw_shared_ptr<internal::engine_probe_impl> impl)
  : _impl(std::move(impl)) {}

engine_probe::~engine_probe() {
    if (!_impl) {
        return;
    }
    _impl->report_max_memory_delta(-int64_t(_last_reported_max_memory));
    _impl->report_memory_usage_delta(-int64_t(_last_reported_memory_usage));
}

void engine_probe::report_memory_usage(uint32_t usage) {
    int64_t delta = int64_t(usage) - int64_t(_last_reported_memory_usage);
    _impl->report_memory_usage_delta(delta);
    _last_reported_memory_usage = usage;
}

void engine_probe::report_max_memory(uint32_t max) {
    int64_t delta = int64_t(max) - int64_t(_last_reported_max_memory);
    _impl->report_max_memory_delta(delta);
    _last_reported_max_memory = max;
}

void engine_probe::increment_cpu_time(ss::steady_clock_type::duration d) {
    _impl->increment_cpu_time(d);
}

engine_probe engine_probe_cache::make_probe(const ss::sstring& name) {
    internal::engine_probe_impl*& probe = _probe_cache[name];
    if (probe) {
        return engine_probe(probe->shared_from_this());
    }
    auto probe_impl = ss::make_lw_shared<internal::engine_probe_impl>(
      this, name);
    probe = probe_impl.get();
    return engine_probe(probe_impl);
}

void engine_probe_cache::remove_probe(const ss::sstring& name) {
    vassert(
      _probe_cache.erase(name) > 0, "wasm engine probe cache inconsistency");
}
} // namespace wasm
