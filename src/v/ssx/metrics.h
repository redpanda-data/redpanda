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

#pragma once

#include "ssx/sformat.h"
#include "utils/hdr_hist.h"

#include <seastar/core/future.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_registration.hh>

namespace ssx::metrics {

// The seastar metrics handle to be used for the '/public_metrics' prometheus
// endpoint.
const auto public_metrics_handle = ss::metrics::default_handle() + 1;

// A thin wrapper around ss::metrics::metric_groups that isn't moveable.
//
// The standard usage pattern for metric_groups is as a member of a class, then
// various additional members on that class are the metrics. As part of
// registration, a lambda captures [this], which means that the holder class of
// the metric_groups should not be moved without updating the metrics.
//
// Instead of explicitly supporting moves (they're easy to forget when the
// compiler generates them for you) we mark this class as not moveable.
class metric_groups {
public:
    metric_groups() = default;
    explicit metric_groups(int handle)
      : _underlying(handle) {}
    metric_groups(const metric_groups&) = delete;
    metric_groups& operator=(const metric_groups&) = delete;
    metric_groups(metric_groups&&) = delete;
    metric_groups& operator=(metric_groups&&) = delete;
    ~metric_groups() = default;

    static metric_groups make_internal() { return {}; }
    static metric_groups make_public() {
        return metric_groups(public_metrics_handle);
    }

    // Add metrics belonging to the same group.
    //
    // Use the metrics creation functions to add metrics.
    //
    // For example:
    //  _metrics.add_group("my_group", {
    //   make_counter(
    //     "my_counter_name1",
    //     counter,
    //     description("my counter description")
    //   ),
    //   make_counter(
    //     "my_counter_name2",
    //     counter,
    //     description("my other counter description")
    //   ),
    //   make_gauge(
    //     "my_gauge_name1",
    //     gauge,
    //     description("my gauge description")
    //   ),
    //  });
    metric_groups& add_group(
      const ss::metrics::group_name_type& name,
      const std::initializer_list<ss::metrics::metric_definition>& l) {
        _underlying.add_group(name, l);
        return *this;
    }

    // Add metrics belonging to the same group.
    //
    // This is the same method as above but using an explicit vector.
    metric_groups& add_group(
      const ss::metrics::group_name_type& name,
      const std::vector<ss::metrics::metric_definition>& l) {
        _underlying.add_group(name, l);
        return *this;
    }

    // Clear all metric registrations.
    void clear() { _underlying.clear(); }

private:
    ss::metrics::metric_groups _underlying;
};

// Convert an HDR histogram to a seastar histogram for reporting.
// Entries with values ranging form 250 microseconds to 1 minute are
// grouped in 18 buckets of exponentially increasing size.
inline ss::metrics::histogram report_default_histogram(const hdr_hist& hist) {
    static constexpr size_t num_buckets = 18;
    static constexpr size_t first_value = 250;
    static constexpr double log_base = 2.0;
    static constexpr int64_t scale = 1000000;

    return hist.seastar_histogram_logform(
      num_buckets, first_value, log_base, scale);
}

constexpr auto label_namespace = "redpanda";

inline ss::metrics::label make_namespaced_label(const seastar::sstring& name) {
    return ss::metrics::label(ssx::sformat("{}_{}", label_namespace, name));
}

struct public_metrics_group {
    metric_groups groups = metric_groups::make_public();
    ss::future<> stop() {
        groups.clear();
        return ss::make_ready_future<>();
    }
};

/**
 * @brief A class bundling together public and internal metrics.
 *
 * Intended to replace an ss::metrics::metric_groups instance when
 * you want a metric exposed on both the /metrics (aka "internal")
 * and /public_metrics (aka "public") metrics endpoint.
 */
class all_metrics_groups {
    metric_groups _public = metric_groups::make_public();
    metric_groups _internal = metric_groups::make_internal();

public:
    /**
     * @brief Adds the given metric group to public and internal metrics.
     *
     * The behavior is same as ss::metrics::metric_groups::add_group but for
     * both metric endpoints.
     */
    all_metrics_groups& add_group(
      const seastar::metrics::group_name_type& name,
      const std::initializer_list<seastar::metrics::metric_definition>& l) {
        _internal.add_group(name, l);
        _public.add_group(name, l);
        return *this;
    };
};

} // namespace ssx::metrics
