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

#include "config/configuration.h"
#include "prometheus/prometheus_sanitize.h"
#include "model/fundamental.h"

#include <seastar/core/metrics.hh>

namespace cluster {
class partition_probe_v2 {
public:
    void setup_metrics(const model::ntp& ntp) {
        namespace sm = ss::metrics;

        if (config::shard_local_cfg().disable_metrics()) {
            return;
        }

        auto req_label = sm::label("request");
        auto ns_label = sm::label("namespace");
        auto topic_label = sm::label("topic");

        const std::vector<sm::label_instance> cluster_prod_labels = {req_label("produce")};
        const std::vector<sm::label_instance> cluster_cons_labels = {req_label("consume")};
        const std::vector<sm::label_instance> topic_prod_labels = {req_label("produce"), ns_label(ntp.ns()), topic_label(ntp.tp.topic())};
        const std::vector<sm::label_instance> topic_cons_labels = {req_label("consume"), ns_label(ntp.ns()), topic_label(ntp.tp.topic())};

        _metrics.add_group(prometheus_sanitize::metrics_name("kafka"),
          {
          sm::make_total_bytes(
          "request_bytes_total",
          [this] { return _cluster_produce_bytes; },
          sm::description("Number of bytes in client payloads by request type"),
          cluster_prod_labels),

        sm::make_total_bytes(
          "request_bytes_total",
          [this] { return _cluster_consume_bytes; },
          sm::description("Number of bytes in client payloads by request type"),
          cluster_cons_labels),

          sm::make_total_bytes(
          "request_bytes_total",
          [this] { return _topic_produce_bytes; },
          sm::description("Number of bytes in client payloads by request type, namespace, and topic"),
          topic_prod_labels),

        sm::make_total_bytes(
          "request_bytes_total",
          [this] { return _topic_consume_bytes; },
          sm::description("Number of bytes in client payloads by request type, namespace, and topic"),
          topic_cons_labels),
        });
    }

    void add_produce_bytes_cluster_lvl(uint64_t bytes) { _cluster_produce_bytes += bytes; }
    void add_fetch_bytes_cluster_lvl(uint64_t bytes) { _cluster_consume_bytes += bytes; }
    void add_produce_bytes_topic_lvl(uint64_t bytes) { _topic_produce_bytes += bytes; }
    void add_fetch_bytes_topic_lvl(uint64_t bytes) { _topic_consume_bytes += bytes; }

private:
    uint64_t _cluster_produce_bytes = 0;
    uint64_t _cluster_consume_bytes = 0;
    uint64_t _topic_produce_bytes = 0;
    uint64_t _topic_consume_bytes = 0;
    ss::metrics::metric_groups _metrics;
};

} // namespace kafka
