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
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"
#include "ssx/sformat.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/util/bool_class.hh>

namespace pandaproxy {

probe::probe(
  ss::httpd::path_description& path_desc, const ss::sstring& group_name)
  : _request_metrics()
  , _path(path_desc)
  , _group_name(group_name)
  , _metrics() {
    setup_metrics();
}

void probe::setup_metrics() {
    namespace sm = ss::metrics;

    using is_internal = ss::bool_class<struct is_internal_tag>;

    struct Labels {
        sm::label_instance label;
        std::vector<sm::label> agg;
        sm::label status;
    };
    const auto make_labels = [this](const is_internal internal) -> Labels {
        const auto make_label =
          [](const ss::sstring& key, const is_internal internal) {
              return internal ? sm::label(key)
                              : metrics::make_namespaced_label(key);
          };
        const auto operation_label = make_label("operation", internal);
        const auto agg = internal ? std::vector<sm::label>{sm::shard_label}
                                  : std::vector<sm::label>{
                                      sm::shard_label, operation_label};
        const auto status = make_label("status", internal);
        return {
          .label = operation_label(_path.operations.nickname),
          .agg = agg,
          .status = status};
    };

    const auto internal_labels = make_labels(is_internal::yes);
    const auto public_labels = make_labels(is_internal::no);

    const auto make_internal_request_latency = [this](const Labels& l) {
        return sm::make_histogram(
          "request_latency",
          sm::description("Request latency"),
          {l.label},
          [this] {
              return _request_metrics.hist().internal_histogram_logform();
          });
    };

    const auto make_public_request_latency = [this](const Labels& l) {
        return sm::make_histogram(
          "request_latency_seconds",
          sm::description(
            ssx::sformat("Internal latency of request for {}", _group_name)),
          {l.label},
          [this] {
              return _request_metrics.hist().public_histogram_logform();
          });
    };

    const auto make_request_errors_total_5xx = [this](const Labels& l) {
        return sm::make_counter(
          "request_errors_total",
          [this] { return _request_metrics._5xx_count; },
          sm::description(
            ssx::sformat("Total number of {} server errors", _group_name)),
          {l.label, l.status("5xx")});
    };

    const auto make_request_errors_total_4xx = [this](const Labels& l) {
        return sm::make_counter(
          "request_errors_total",
          [this] { return _request_metrics._4xx_count; },
          sm::description(
            ssx::sformat("Total number of {} client errors", _group_name)),
          {l.label, l.status("4xx")});
    };

    const auto make_request_errors_total_3xx = [this](const Labels& l) {
        return sm::make_counter(
          "request_errors_total",
          [this] { return _request_metrics._3xx_count; },
          sm::description(
            ssx::sformat("Total number of {} redirection errors", _group_name)),
          {l.label, l.status("3xx")});
    };

    if (!config::shard_local_cfg().disable_metrics()) {
        _metrics.add_group(
          "pandaproxy",
          {make_internal_request_latency(internal_labels),
           make_request_errors_total_5xx(internal_labels),
           make_request_errors_total_4xx(internal_labels),
           make_request_errors_total_3xx(internal_labels)},
          {},
          internal_labels.agg);
    }
    if (!config::shard_local_cfg().disable_public_metrics()) {
        _public_metrics.add_group(
          _group_name,
          {make_public_request_latency(public_labels)
             .aggregate(public_labels.agg),
           make_request_errors_total_5xx(public_labels)
             .aggregate(public_labels.agg),
           make_request_errors_total_4xx(public_labels)
             .aggregate(public_labels.agg),
           make_request_errors_total_3xx(public_labels)
             .aggregate(public_labels.agg)});
    }
}

server_probe::server_probe(
  server::context_t& ctx, const ss::sstring& group_name)
  : _ctx(ctx)
  , _group_name(group_name)
  , _metrics()
  , _public_metrics() {
    setup_metrics();
}

void server_probe::setup_metrics() {
    namespace sm = ss::metrics;

    auto setup_common = [this]<typename MetricDef>() {
        const auto usage = [](const size_t current, const size_t max) {
            constexpr double divide_by_zero = -1.;
            constexpr double invalid_values = -2.;
            if (max == 0) {
                return divide_by_zero;
            }
            if (current > max) {
                return invalid_values;
            }
            const auto max_d = static_cast<double>(max);
            const auto current_d = static_cast<double>(current);
            return (max_d - current_d) / max_d;
        };

        std::vector<MetricDef> defs;
        defs.reserve(3);
        defs.emplace_back(
          sm::make_gauge(
            "inflight_requests_usage_ratio",
            [this, usage] {
                return usage(_ctx.inflight_sem.current(), _ctx.max_inflight);
            },
            sm::description(ssx::sformat(
              "Usage ratio of in-flight requests in the {}", _group_name)))
            .aggregate({}));
        defs.emplace_back(
          sm::make_gauge(
            "inflight_requests_memory_usage_ratio",
            [this, usage] {
                return usage(_ctx.mem_sem.current(), _ctx.max_memory);
            },
            sm::description(ssx::sformat(
              "Memory usage ratio of in-flight requests in the {}",
              _group_name)))
            .aggregate({}));
        defs.emplace_back(
          sm::make_gauge(
            "queued_requests_memory_blocked",
            [this] { return _ctx.mem_sem.waiters(); },
            sm::description(ssx::sformat(
              "Number of requests queued in {}, due to memory limitations",
              _group_name)))
            .aggregate({}));
        return defs;
    };

    if (!config::shard_local_cfg().disable_metrics()) {
        _metrics.add_group(
          _group_name,
          setup_common
            .template operator()<ss::metrics::impl::metric_definition_impl>(),
          {},
          {});
    }

    if (!config::shard_local_cfg().disable_public_metrics()) {
        _public_metrics.add_group(
          _group_name,
          setup_common.template operator()<ss::metrics::metric_definition>());
    }
}

} // namespace pandaproxy
