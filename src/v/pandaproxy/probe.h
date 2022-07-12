/*
 * Copyright 2020 Redpanda Data, Inc.
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

#include <seastar/core/metrics_registration.hh>
#include <seastar/http/json_path.hh>

namespace pandaproxy {

class probe {
public:
    probe(
      ss::httpd::path_description& path_desc, const ss::sstring& group_name);
    hdr_hist& hist() { return _request_hist; }

private:
    hdr_hist _request_hist;
    ss::metrics::metric_groups _metrics;
    ss::metrics::metric_groups _public_metrics;
};

class error_probe {
public:
    explicit error_probe(const ss::sstring& group_name);

    void increment_5xx(int64_t count = 1) { _5xx_count += count; }

    void increment_4xx(int64_t count = 1) { _4xx_count += count; }

    void increment_3xx(int64_t count = 1) { _3xx_count += count; }

private:
    int64_t _5xx_count;
    int64_t _4xx_count;
    int64_t _3xx_count;
    ss::metrics::metric_groups _public_metrics;
};

} // namespace pandaproxy
