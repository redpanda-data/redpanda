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
#include <seastar/http/reply.hh>

namespace pandaproxy {

/// If the request is good, measure latency, otherwise record the error.
class http_status_metric {
public:
    class measurement {
    public:
        measurement(
          http_status_metric* p, std::unique_ptr<hdr_hist::measurement> m)
          : _p(p)
          , _m(std::move(m)) {}

        void set_status(ss::http::reply::status_type s) {
            using status_type = ss::http::reply::status_type;
            if (s < status_type{300}) {
                return;
            }
            if (s < status_type{400}) {
                ++_p->_3xx_count;
            } else if (s < status_type{500}) {
                ++_p->_4xx_count;
            } else {
                ++_p->_5xx_count;
            }
            _m->set_trace(false);
        }

    private:
        http_status_metric* _p;
        std::unique_ptr<hdr_hist::measurement> _m;
    };
    hdr_hist& hist() { return _hist; }
    auto auto_measure() { return measurement{this, _hist.auto_measure()}; }

    hdr_hist _hist;
    int64_t _5xx_count{0};
    int64_t _4xx_count{0};
    int64_t _3xx_count{0};
};

class probe {
public:
    probe(
      ss::httpd::path_description& path_desc, const ss::sstring& group_name);
    auto auto_measure() { return _request_metrics.auto_measure(); }

private:
    void setup_metrics();
    void setup_public_metrics();

private:
    http_status_metric _request_metrics;
    const ss::httpd::path_description& _path;
    const ss::sstring& _group_name;
    ss::metrics::metric_groups _metrics;
    ss::metrics::metric_groups _public_metrics;
};

} // namespace pandaproxy
