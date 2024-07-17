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

#include "metrics/metrics_registry.h"
#include "ssx/sformat.h"
#include "utils/hdr_hist.h"

#include <seastar/core/future.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/sstring.hh>

#include <unordered_set>

namespace metrics {

// The seastar metrics handle to be used for the '/public_metrics' prometheus
// endpoint.
const auto public_metrics_handle = ss::metrics::default_handle() + 1;

// A thin wrapper around ss::metrics::metric_groups that isn't moveable.
//
// The standard usage pattern for metric_groups_base (and children) is as a
// member of a class, then various additional members on that class are the
// metrics. As part of registration, a lambda captures [this], which means that
// the holder class of the metric_groups_base should not be moved without
// updating the metrics.
//
// Instead of explicitly supporting moves (they're easy to forget when the
// compiler generates them for you) we mark this class as not moveable.
class metric_groups_base {
public:
    explicit metric_groups_base(int handle)
      : _underlying(handle) {}
    metric_groups_base(const metric_groups_base&) = delete;
    metric_groups_base& operator=(const metric_groups_base&) = delete;
    metric_groups_base(metric_groups_base&&) = delete;
    metric_groups_base& operator=(metric_groups_base&&) = delete;
    ~metric_groups_base() = default;

    // Clear all metric registrations.
    void clear() { _underlying.clear(); }

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
    metric_groups_base& add_group(
      const ss::metrics::group_name_type& name,
      const std::initializer_list<ss::metrics::metric_definition>& l) {
        _underlying.add_group(name, l);
        return *this;
    }

    // Add metrics belonging to the same group.
    //
    // This is the same method as above but using an explicit vector.
    metric_groups_base& add_group(
      const ss::metrics::group_name_type& name,
      const std::vector<ss::metrics::metric_definition>& l) {
        _underlying.add_group(name, l);
        return *this;
    }

protected:
    ss::metrics::metric_groups _underlying;
};

/**
 * @brief Used to (de-)register "internal" metrics on the /metrics admin
 * endpoint.
 *
 * Internal metrics are our default metrics group for all metrics. See also
 * public_metric_groups.
 *
 * To reduce cardinality internal metrics are supposed to obey the
 * `aggregate_metrics` config flag in scenarios where the full cardinality would
 * otherwise create thousands of metrics series. The `add_group` overload from
 * this class simplifies the aggregate_metrics usage.
 */
class internal_metric_groups : public metric_groups_base {
public:
    internal_metric_groups()
      : metric_groups_base(ss::metrics::default_handle()) {}

    using metric_groups_base::add_group;

    /**
     * @brief Extension to metric_groups_base::add_group. When registering
     * metrics with label aggregation prefer to use this method. It will make
     * the metrics automatically obey the current state of the
     * `aggregate_metrics` config flag
     *
     * @param name The name of the metric group.
     * @param metrics The metrics to register.
     * @param non_aggregated_labels The aggregation labels to use when
     * `aggregate_metrics=false`.
     * @param aggregated_labels The aggregation labels to use when
     * `aggregate_metrics=true`.
     *
     * The specified labels will be used for all the specified metrics in
     * the group. Specifying individual labels for each metric is not
     * allowed (use multiple `add_group` calls for that).
     *
     * Example:
     *
     * metrics.add_group(
     *  prometheus_sanitize::metrics_name("foo"),
     *  {
     *    sm::make_counter(...),
     *    sm::make_counter(...),
     *    sm::make_histogram(...),
     *  },
     *  {},
     *  {seastar::metrics::shard_label});
     *
     */
    internal_metric_groups& add_group(
      const ss::metrics::group_name_type& name,
      std::vector<ss::metrics::impl::metric_definition_impl> metrics,
      const std::vector<ss::metrics::label>& non_aggregated_labels,
      const std::vector<ss::metrics::label>& aggregated_labels);
};

/**
 * @brief Used to (de-)register "public" metrics on the /public_metrics admin
 * API endpoint
 *
 * In contrast to internal metrics public metrics are supposed to have a many
 * orders of magnitude smaller cardinality. Hence, you should always
 * unconditionally aggregate as many labels as possible (e.g.: shard).
 *
 * Public metrics are also thought to be for customer consumption (in cloudv2
 * customers do have access to public metrics). Consider whether a metric needs
 * to be on public metrics at all. If it is of no use for the customer then
 * probably that metric should only go on internal metrics (for example an
 * internal buffer consumption metric).
 */
class public_metric_groups : public metric_groups_base {
public:
    public_metric_groups()
      : metric_groups_base(public_metrics_handle) {}
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

struct public_metrics_group_service {
    public_metric_groups groups;
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
    public_metric_groups _public;
    internal_metric_groups _internal;

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

} // namespace metrics
