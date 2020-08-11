#include "pandaproxy/probe.h"

#include "prometheus/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>

namespace pandaproxy {

probe::probe(ss::httpd::path_description& path_desc)
  : _request_hist()
  , _metrics() {
    namespace sm = ss::metrics;
    std::vector<sm::label_instance> labels{
      sm::label("operation")(path_desc.operations.nickname)};
    _metrics.add_group(
      "pandaproxy",
      {sm::make_histogram(
        "request_latency",
        sm::description("Request latency"),
        std::move(labels),
        [this] { return _request_hist.seastar_histogram_logform(); })});
}

} // namespace pandaproxy