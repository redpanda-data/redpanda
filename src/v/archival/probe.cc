/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/probe.h"

#include "prometheus/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/smp.hh>

namespace archival {

ntp_level_probe::ntp_level_probe(
  per_ntp_metrics_disabled disabled, const model::ntp& ntp)
  : _uploaded()
  , _missing()
  , _pending() {
    if (disabled) {
        return;
    }
    namespace sm = ss::metrics;

    auto ns_label = sm::label("namespace");
    auto topic_label = sm::label("topic");
    auto partition_label = sm::label("partition");
    const std::vector<sm::label_instance> labels = {
      ns_label(ntp.ns()),
      topic_label(ntp.tp.topic()),
      partition_label(ntp.tp.partition()),
    };

    _metrics.add_group(
      prometheus_sanitize::metrics_name("ntp_archiver"),
      {
        sm::make_counter(
          "missing",
          [this] { return _missing; },
          sm::description("Missing offsets due to gaps"),
          labels),
        sm::make_counter(
          "uploaded",
          [this] { return _uploaded; },
          sm::description("Uploaded offsets"),
          labels),
        sm::make_gauge(
          "pending",
          [this] { return _pending; },
          sm::description("Pending offsets"),
          labels),
      });
}

service_probe::service_probe(service_metrics_disabled disabled)
  : _cnt_gaps()
  , _cnt_start_archiving_ntp()
  , _cnt_stop_archiving_ntp()
  , _cnt_reconciliations() {
    if (disabled) {
        return;
    }
    namespace sm = ss::metrics;

    _metrics.add_group(
      prometheus_sanitize::metrics_name("archival_service"),
      {
        sm::make_counter(
          "num_gaps",
          [this] { return _cnt_gaps; },
          sm::description("Number of detected offset gaps")),
        sm::make_counter(
          "start_archiving_ntp",
          [this] { return _cnt_start_archiving_ntp; },
          sm::description("Start archiving ntp event counter")),
        sm::make_counter(
          "stop_archiving_ntp",
          [this] { return _cnt_stop_archiving_ntp; },
          sm::description("Stop archiving ntp event counter")),
        sm::make_gauge(
          "num_archived_ntp",
          [this] { return _cnt_start_archiving_ntp - _cnt_stop_archiving_ntp; },
          sm::description("Total number of ntp that archiver manages")),
        sm::make_counter(
          "num_reconciliations",
          [this] { return _cnt_reconciliations; },
          sm::description("Number of reconciliation loop iterations")),
      });
}

} // namespace archival
