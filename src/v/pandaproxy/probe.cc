// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/probe.h"

#include "config/configuration.h"
#include "prometheus/prometheus_sanitize.h"
#include "ssx/metrics.h"
#include "ssx/sformat.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_registration.hh>

namespace pandaproxy {

probe::probe(
  ss::httpd::path_description& path_desc, const ss::sstring& group_name)
  : _request_metrics()
  , _path(path_desc)
  , _group_name(group_name)
  , _metrics()
  , _public_metrics(ssx::metrics::public_metrics_handle) {
    setup_metrics();
    setup_public_metrics();
}

void probe::setup_metrics() {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    auto operation_label = sm::label("operation");
    std::vector<sm::label_instance> labels{
      operation_label(_path.operations.nickname)};

    auto aggregate_labels = std::vector<sm::label>{
      sm::shard_label, operation_label};

    auto internal_aggregate_labels
      = config::shard_local_cfg().aggregate_metrics()
          ? aggregate_labels
          : std::vector<sm::label>{};

    _metrics.add_group(
      "pandaproxy",
      {sm::make_histogram(
         "request_latency",
         sm::description("Request latency"),
         labels,
         [this] { return _request_metrics.hist().seastar_histogram_logform(); })
         .aggregate(internal_aggregate_labels)});
}

void probe::setup_public_metrics() {
    namespace sm = ss::metrics;

    if (config::shard_local_cfg().disable_public_metrics()) {
        return;
    }

    auto operation_label = ssx::metrics::make_namespaced_label("operation");
    auto status_label = ssx::metrics::make_namespaced_label("status");

    std::vector<sm::label_instance> labels{
      operation_label(_path.operations.nickname)};

    auto aggregate_labels = std::vector<sm::label>{
      sm::shard_label, operation_label};

    _public_metrics.add_group(
      _group_name,
      {sm::make_histogram(
         "request_latency_seconds",
         sm::description(
           ssx::sformat("Internal latency of request for {}", _group_name)),
         labels,
         [this] {
             return ssx::metrics::report_default_histogram(
               _request_metrics.hist());
         })
         .aggregate(aggregate_labels),

       sm::make_counter(
         "request_errors_total",
         [this] { return _request_metrics._5xx_count; },
         sm::description(
           ssx::sformat("Total number of {} server errors", _group_name)),
         {operation_label(_path.operations.nickname), status_label("5xx")})
         .aggregate(aggregate_labels),

       sm::make_counter(
         "request_errors_total",
         [this] { return _request_metrics._4xx_count; },
         sm::description(
           ssx::sformat("Total number of {} client errors", _group_name)),
         {operation_label(_path.operations.nickname), status_label("4xx")})
         .aggregate(aggregate_labels),

       sm::make_counter(
         "request_errors_total",
         [this] { return _request_metrics._3xx_count; },
         sm::description(
           ssx::sformat("Total number of {} redirection errors", _group_name)),
         {operation_label(_path.operations.nickname), status_label("3xx")})
         .aggregate(aggregate_labels)});
}

} // namespace pandaproxy
