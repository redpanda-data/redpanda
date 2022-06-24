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

#include "utils/hdr_hist.h"

#include <seastar/core/metrics.hh>

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

} // namespace ssx::metrics
