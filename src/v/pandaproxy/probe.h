#pragma once

#include "utils/hdr_hist.h"

#include <seastar/core/metrics_registration.hh>
#include <seastar/http/json_path.hh>

namespace pandaproxy {

class probe {
public:
    probe(ss::httpd::path_description& path_desc);
    hdr_hist& hist() { return _request_hist; }

private:
    hdr_hist _request_hist;
    ss::metrics::metric_groups _metrics;
};

} // namespace pandaproxy
