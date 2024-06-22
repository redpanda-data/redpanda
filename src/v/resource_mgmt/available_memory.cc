/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "resource_mgmt/available_memory.h"

#include "base/seastarx.h"
#include "config/configuration.h"
#include "metrics/prometheus_sanitize.h"

#include <seastar/core/memory.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_registration.hh>

#include <fmt/core.h>

#include <iterator>
#include <limits>
#include <memory>
#include <numeric>
#include <stdexcept>

namespace resources {

available_memory::available_memory()
  : _lwm(std::numeric_limits<decltype(_lwm)>::max()) {}

available_memory::deregister_holder
available_memory::inner_register(const ss::sstring& name, afn&& fn) {
    auto ret = std::unique_ptr<reporter>(new reporter{name, std::move(fn)});
    _reporters.push_back(*ret);
    return ret;
}

size_t available_memory::available() const {
    return ss::memory::free_memory() + reclaimable();
}

size_t available_memory::reclaimable() const {
    size_t reclaimable = 0;
    for (const auto& r : _reporters) {
        reclaimable += r.avail_fn();
    }
    return reclaimable;
}

size_t available_memory::available_low_water_mark() {
    update_low_water_mark();
    return _lwm;
}

void available_memory::update_low_water_mark() {
    _lwm = std::min(_lwm, available());
}

void available_memory::register_metrics() {
    if (_metrics || config::shard_local_cfg().disable_public_metrics()) {
        // already initialized or disabled
        return;
    }

    auto& m = _metrics.emplace();
    m.add_group(
      prometheus_sanitize::metrics_name("memory"),
      {ss::metrics::make_gauge(
         "available_memory",
         [this] { return available(); },
         ss::metrics::description(
           "Total shard memory potentially available in bytes "
           "(free_memory plus reclaimable)")),
       ss::metrics::make_gauge(
         "available_memory_low_water_mark",
         [this] { return available_low_water_mark(); },
         ss::metrics::description(
           "The low-water mark for available_memory from process start"))});
}

thread_local available_memory available_memory::_local_instance;

} // namespace resources
