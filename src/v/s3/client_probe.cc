/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "prometheus/prometheus_sanitize.h"
#include "s3/error.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/smp.hh>

#include <s3/client_probe.h>

namespace s3 {

client_probe::client_probe(
  net::metrics_disabled disable, ss::sstring region, ss::sstring endpoint)
  : http::client_probe()
  , _total_rpc_errors(0)
  , _total_slowdowns(0)
  , _total_nosuchkeys(0) {
    namespace sm = ss::metrics;
    if (disable) {
        return;
    }

    auto endpoint_label = sm::label("endpoint");
    auto region_label = sm::label("region");

    const std::vector<sm::label_instance> labels = {
      endpoint_label(std::move(endpoint)), region_label(std::move(region))};

    _metrics.add_group(
      prometheus_sanitize::metrics_name("s3:client"),
      {
        sm::make_counter(
          "total_uploads",
          [this] { return get_total_put_requests(); },
          sm::description("Number of completed PUT requests"),
          labels),
        sm::make_counter(
          "total_downloads",
          [this] { return get_total_get_requests(); },
          sm::description("Number of completed GET requests"),
          labels),
        sm::make_counter(
          "all_requests",
          [this] { return get_total_requests(); },
          sm::description(
            "Number of completed HTTP requests (includes PUT and GET)"),
          labels),
        sm::make_gauge(
          "active_uploads",
          [this] { return get_active_put_requests(); },
          sm::description("Number of active PUT requests at the moment"),
          labels),
        sm::make_gauge(
          "active_downloads",
          [this] { return get_active_get_requests(); },
          sm::description("Number of active GET requests at the moment"),
          labels),
        sm::make_gauge(
          "active_requests",
          [this] { return get_active_requests(); },
          sm::description("Number of active HTTP requests at the moment "
                          "(includes PUT and GET)"),
          labels),
        sm::make_counter(
          "total_inbound_bytes",
          [this] { return get_inbound_bytes(); },
          sm::description(
            "Total number of bytes received from cloud storage bucket"),
          labels),
        sm::make_counter(
          "total_outbound_bytes",
          [this] { return get_outbound_bytes(); },
          sm::description("Total number of bytes sent to cloud storage bucket"),
          labels),
        sm::make_counter(
          "num_rpc_errors",
          [this] { return _total_rpc_errors; },
          sm::description("Total number of S3 REST API errors received from "
                          "cloud storage provider"),
          labels),
        sm::make_counter(
          "num_transport_errors",
          [this] { return get_transport_errors(); },
          sm::description("Total number of transport errors (TCP and TLS)"),
          labels),
        sm::make_counter(
          "num_slowdowns",
          [this] { return _total_slowdowns; },
          sm::description("Total number of SlowDown errors received from cloud "
                          "storage provider"),
          labels),
        sm::make_counter(
          "num_nosuchkey",
          [this] { return _total_nosuchkeys; },
          sm::description(
            "Total number of NoSuchKey errors received from cloud "
            "storage provider"),
          labels),
      });
}

void client_probe::register_failure(s3_error_code err) {
    if (err == s3_error_code::slow_down) {
        _total_slowdowns += 1;
    } else if (err == s3_error_code::no_such_key) {
        _total_nosuchkeys += 1;
    }
    _total_rpc_errors += 1;
}

} // namespace s3