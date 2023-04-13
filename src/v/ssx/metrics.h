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
const auto public_metrics_handle = seastar::metrics::default_handle() + 1;

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
    ss::metrics::metric_groups groups{public_metrics_handle};
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
    ss::metrics::metric_groups _public{public_metrics_handle}, _internal;

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
